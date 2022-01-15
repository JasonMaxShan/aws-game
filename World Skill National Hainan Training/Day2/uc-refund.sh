#!/bin/bash

echo "Enter your Game ID: "
read GAME_ID
echo "Enter your Team ID: "
read TEAM_ID
echo "Enter the Request ID: "
read UUID

echo "Sent Refund request for ID: $UUID"
REQ_RESP=`curl -i -H "Accept: application/json" -X POST -d '{"game":"'"$GAME_ID"'", "team":"'"$TEAM_ID"'", "order":"'"$UUID"'"}' https://stats.aws.dev-null.link/proc/refund`

echo "Request response: "
echo $REQ_RESP
