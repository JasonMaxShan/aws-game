import json
import boto3          
from botocore.exceptions import ClientError
import logging

logging.basicConfig(level=logging.DEBUG)
logger=logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

iam_resource = boto3.resource('iam')
accessanalyzer_client = boto3.client('accessanalyzer')
def handler(event, context):
  access_analyzer_findings=""
  for p in iam_resource.policies.filter(Scope='Local'):
    if p.policy_name.startswith('jam-'):
      try:
        #TODO Add the missing Access Analyzer API call <to_be_completed>
        access_analyzer_response = accessanalyzer_client.validate_policy(                  
          policyDocument=json.dumps(p.default_version.document),
          policyType='IDENTITY_POLICY'
        )
        if access_analyzer_response["findings"]:
          access_analyzer_findings = access_analyzer_response["findings"]

      except ClientError as error:
        logger.error("An error occured: {0}".format(error))
        raise error
      if access_analyzer_findings:
        return {
          'statusCode': 200,
          'policyName': p.policy_name,
          'findings': json.dumps(access_analyzer_findings)
        }
      else:
        return {
          'statusCode': 201,
          'body': json.dumps('No findings !')
        }
