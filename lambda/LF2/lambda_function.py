import json
import boto3
import logging
from requests_aws4auth import AWS4Auth
import requests
import os

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# AWS Credentials and Configuration
AWS_ACCESS_KEY = "AKIA6GBMDD6SZYOGLXR3"
AWS_SECRET_KEY = "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"
region = 'us-east-1'

# Lex Bot Configuration
lex_bot_id = 'B65UNTXULI'
lex_bot_alias_id = 'TSTALIASID'
lex_locale_id = 'en_US'

# OpenSearch Configuration
es_endpoint = "https://search-photos-f6pyca26k2g3p6doauwlowykqe.us-east-1.es.amazonaws.com"
index_name = "photos"
s3_bucket_url = "https://assignment3photobucket.s3.us-east-1.amazonaws.com"

# AWS4Auth for OpenSearch
auth = AWS4Auth(AWS_ACCESS_KEY, AWS_SECRET_KEY, region, 'es')

def lambda_handler(event, context):
    logger.debug("Received Event: %s", json.dumps(event, indent=2))

    try:
        # Extract query parameter
        q = event["queryStringParameters"]["q"]
        logger.debug(f"Extracted query parameter: {q}")

        # Call Lex
        lex_client = boto3.client('lexv2-runtime', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=region)
        lex_response = lex_client.recognize_text(
            botId=lex_bot_id,
            botAliasId=lex_bot_alias_id,
            localeId=lex_locale_id,
            sessionId="sessionId",
            text=q
        )
        logger.debug("Lex Response: %s", json.dumps(lex_response))

        q1 = ""
        q2 = ""

        # Use the correct variable `lex_response`
        if 'sessionState' in lex_response and 'intent' in lex_response['sessionState'] and 'slots' in lex_response['sessionState']['intent']:
            if 'query1' in lex_response['sessionState']['intent']['slots'] and lex_response['sessionState']['intent']['slots']['query1'] is not None and 'value' in lex_response['sessionState']['intent']['slots']['query1'] and lex_response['sessionState']['intent']['slots']['query1']['value'] is not None:
                q1 = lex_response['sessionState']['intent']['slots']['query1']['value']['interpretedValue']
            if 'query2' in lex_response['sessionState']['intent']['slots'] and lex_response['sessionState']['intent']['slots']['query2'] is not None and 'value' in lex_response['sessionState']['intent']['slots']['query2'] and lex_response['sessionState']['intent']['slots']['query2']['value'] is not None:
                q2 = lex_response['sessionState']['intent']['slots']['query2']['value']['interpretedValue']
        # search_intent_slots = lex_response["interpretations"][1]["intent"]["slots"]

        # Extract query1
        # if search_intent_slots["query1"] and search_intent_slots["query1"]["value"] and search_intent_slots["query1"]["value"]["interpretedValue"]:
        #     q1 = search_intent_slots["query1"]["value"]["interpretedValue"]

        # # Extract query2
        # if search_intent_slots["query2"] and search_intent_slots["query2"]["value"] and search_intent_slots["query2"]["value"]["interpretedValue"]:
        #     q2 = search_intent_slots["query2"]["value"]["interpretedValue"]

        logger.info(f"Extracted slots - q1: {q1}, q2: {q2}")

        # slots = lex_response.get("sessionState", {}).get("intent", {}).get("slots", {})
        # q1 = extract_slot_value(slots, "query1")
        # q2 = extract_slot_value(slots, "query2")
        logger.info(f"Extracted slots - q1: {q1}, q2: {q2}")

        # Query OpenSearch
        images = []
        if q1:
            images.extend(search_opensearch(q1))
        if q2:
            images.extend(search_opensearch(q2))
        logger.debug(images)
        # Return results
        # return {
        #     'headers': {
        #         'Access-Control-Allow-Headers': 'Content-Type',
        #         'Access-Control-Allow-Origin': '*',
        #         'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        #     },
        #     'statusCode': 200,
        #     'messages': "Search completed successfully.",
        #     'images': images
        # }
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },
            "body": json.dumps({
                "messages": "Search completed successfully.",
                "images": images
        })
        }

    except Exception as e:
        logger.error("Error processing request: %s", str(e), exc_info=True)
        return {
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            },
            'statusCode': 500,
            'messages': f"An error occurred: {str(e)}"
        }


def extract_slot_value(slots, slot_name):
    """
    Extracts the interpreted value of a given slot.
    """
    slot = slots.get(slot_name, {})
    return slot.get('value', {}).get('interpretedValue', "").strip()


def search_opensearch(keyword):
    """
    Searches OpenSearch for photos matching the given keyword.
    """
    try:
        headers = {"Content-Type": "application/json"}
        query = {
            "query": {
                "bool": {
                    "should": [{"match": {"labels": keyword}}]
                }
            }
        }
        es_url = f"{es_endpoint}/{index_name}/_search"

        logger.debug(f"Querying OpenSearch with keyword: {keyword}")
        response = requests.post(es_url, auth=auth, headers=headers, json=query)
        response.raise_for_status()

        hits = response.json().get('hits', {}).get('hits', [])
        logger.debug(f"Found {len(hits)} hits in OpenSearch.")

        # Construct image URLs
        images = [
            f"{s3_bucket_url}/{hit['_source']['objectKey']}" for hit in hits if '_source' in hit and 'objectKey' in hit['_source']
        ]
        return images

    except requests.exceptions.RequestException as re:
        logger.error(f"OpenSearch RequestException: {str(re)}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"Unexpected error querying OpenSearch: {str(e)}", exc_info=True)
        return []
