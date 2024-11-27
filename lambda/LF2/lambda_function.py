import json
import logging
import boto3
import base64
from requests_aws4auth import AWS4Auth
import requests

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# AWS credentials and region
AWS_ACCESS_KEY = "AKIA6GBMDD6S7T6F7O5D"
AWS_SECRET_KEY = "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"
region = 'us-east-1'

# Authentication for OpenSearch
auth = AWS4Auth(AWS_ACCESS_KEY, AWS_SECRET_KEY, region, 'es')

# OpenSearch configuration
es_endpoint = 'https://search-photos-f6pyca26k2g3p6doauwlowykqe.us-east-1.es.amazonaws.com'
index_name = 'photos'


def lambda_handler(event, context):
    logger.debug("Function invoked: %s", event)
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    created_timestamp = event['Records'][0]['eventTime']
    
    logger.debug("Bucket: %s, Object Key: %s, Timestamp: %s", bucket_name, object_key, created_timestamp)
    
    s3_client = boto3.client('s3')
    rekognition_client = boto3.client('rekognition')
    
    # Get the image from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    base64_encoded_image = response['Body'].read()
    
    try:
        decoded_image = base64.b64decode(base64_encoded_image)
    except Exception as e:
        logger.error("Error decoding base64 image: %s", e)
        return {
            'statusCode': 400,
            'body': json.dumps('Error decoding the base64 image')
        }
    # Detect labels using Rekognition
    labels = rekognition_client.detect_labels(Image={'Bytes': decoded_image}, MaxLabels=5)
    detected_labels = [label['Name'] for label in labels['Labels']]
    logger.debug("Rekognition Detected Labels: %s", detected_labels)
    # logger.debug("Custom Labels: %s", custom_labels)
    # Fetch custom labels from S3 object metadata
    metadata_response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    logger.debug("Metadata Response: %s", metadata_response)
    custom_labels = metadata_response.get('Metadata', {}).get('customlabels', '').split(',')
    logger.debug("Custom Labels: %s", custom_labels)
    detected_labels.extend(custom_labels)
    
    # Prepare the document to be indexed in OpenSearch
    es_document = {
        "objectKey": object_key,
        "bucket": bucket_name,
        "createdTimestamp": created_timestamp,
        "labels": detected_labels
    }
    logger.debug("Detected Labels%s", es_document["labels"])
    # Index the document in OpenSearch
    url = f"{es_endpoint}/{index_name}/_doc/{object_key}"
    headers = {"Content-Type": "application/json"}
    es_response = requests.post(url, auth=auth, json=es_document, headers=headers)
    
    logger.debug("OpenSearch Response: %s", es_response.text)
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT',
        },
        'body': json.dumps('Successfully processed image and indexed in OpenSearch!')
    }
