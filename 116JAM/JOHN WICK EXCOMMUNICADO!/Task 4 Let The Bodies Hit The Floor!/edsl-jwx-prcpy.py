import json
import urllib.parse
import boto3
import os

rds_client = boto3.client('rds-data')
database_name = 'continental_db'
db_cluster_arn = os.environ['db_cluster_arn']
db_secrets_store_arn = os.environ['db_secrets_store_arn']

sns = boto3.client('sns')
sns_target = os.environ['sns_target']
sns_subject = ' Xcommunicado'
sns_body = ' has been marked Xcommunicado - can be killed with no consequences!'

# function to execute sql statements against RDS with sql parameters
def execute_statement(sql, sql_parameters=[]):
    response = rds_client.execute_statement(
        secretArn=db_secrets_store_arn,
        database=database_name,
        resourceArn=db_cluster_arn,
        sql=sql,
        parameters=sql_parameters
    )
    return response

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        print("File Content Type: " + obj['ContentType'])
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(file_key, bucket_name))
        raise e

    try:
        # get lines inside the file
        lines = obj['Body'].read().split(b'\n')
        total_rows = 0
        total_updates = 0
        for r in lines:
            total_rows = total_rows + 1
            sql="update continental_agent set agent_status = :agent_status where upper(agent_first_name||' '||agent_last_name) = :agent_name"
            agent_status = 'EXCOMMUNICADO'
            # to handle various mixed case agent names
            agent_name = r.decode('utf-8').upper()
            sql_parameters = [
                {'name':'agent_status', 'value':{'stringValue': f'{agent_status}'}},
                {'name':'agent_name', 'value':{'stringValue': f'{agent_name}'}}
            ]
            response = execute_statement(sql, sql_parameters)
            update_count = response["numberOfRecordsUpdated"]
            total_updates = total_updates + update_count
            
            # if agent updated send notification to subscribers
            if update_count > 0:
                print(agent_name + sns_body)
                response = sns.publish(
                    TargetArn = sns_target,
                    Message = json.dumps({'default': json.dumps("{}"),
                                          'sms': agent_name + sns_subject,
                                          'email': agent_name + sns_body}),
                    Subject = agent_name + sns_subject,
                    MessageStructure = 'json'
                )
            else:
                print('{} not found in Continental DB.'.format(agent_name))
        
        print('Total rows read: {}, total number of agents updated: {}'.format(total_rows,total_updates))
            
    except Exception as e:
        print(e)
        print('On line {} error executing statement {}'.format(agent_name,sql))
        raise e

    return {'statusCode': 200}