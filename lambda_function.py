import json
import boto3
import os
from os import getenv
from botocore.exceptions import ClientError
from datetime import datetime
import traceback

s3_client = boto3.client('s3',region_name=getenv('AWS_REGION', 'us-east-1'),config=boto3.session.Config(signature_version='s3v4'))
client = boto3.client('secretsmanager', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

dynamo_db_table = json.loads((client.get_secret_value(SecretId='img_process_table')).get("SecretString")).get("img_process_table")
img_process_table = dynamodb.Table(dynamo_db_table)
bucket_name = (json.loads((client.get_secret_value(SecretId='bucket')).get("SecretString"))).get("bucket")

def lambda_handler(event, context):
    """
    This is the main function that AWS Lambda will execute when triggered.
    It acts as a backend for an image processing application, generating
    presigned URLs for S3 uploads and storing image metadata in DynamoDB.

    Args:
        event (dict): Contains data about the event that triggered the Lambda function.
                      For API Gateway, this includes HTTP method, query parameters, etc.
        context (object): Provides runtime information about the invocation, function,
                          and execution environment.
    Returns:
        dict: An API Gateway-compatible response containing a status code,
              headers (especially for CORS), and a JSON body.
    """
    try:
        if event['httpMethod'] == 'GET':
            file_name = event['queryStringParameters'].get('fileName')
            file_type = event['queryStringParameters'].get('fileType')

            image_file_name = event.get('queryStringParameters', {}).get('fileName', None)
            image_type = str(event.get('queryStringParameters', {}).get('fileType', None))[5:]
            image_size = event.get('queryStringParameters', {}).get('imageSize', None)
            extracted_text_from_image = event.get('queryStringParameters', {}).get('extractedText', None)
            process_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            unique_id = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "|" + str(image_file_name)
            
            try:
                response = img_process_table.update_item(
                    Key={
                        'unique_id': unique_id
                    },
                    UpdateExpression="SET extracted_text = :text, image_file_name = :image_file_name, \
                                      image_type = :image_type, image_size = :image_size, process_time = :process_time",
                    ExpressionAttributeValues={
                        ':text': extracted_text_from_image,
                        ':image_file_name': image_file_name,
                        ':image_type': image_type,
                        ':image_size': image_size,
                        ':process_time': process_time
                    },
                    ReturnValues="UPDATED_NEW"
                )
                
            except ClientError as e:
                print("exception 63",traceback.print_exc())
                return None

            if not file_name or not file_type:
                return {
                    'statusCode': 400,
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': 'Content-Type',
                        'Access-Control-Allow-Methods': 'OPTIONS,GET'
                    },
                    'body': json.dumps({'error': 'Missing fileName or fileType. Provide both in query parameters.'})
                }


            if not bucket_name:
                return {
                    'statusCode': 500,
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': 'Content-Type',
                        'Access-Control-Allow-Methods': 'OPTIONS,GET'
                    },
                    'body': json.dumps({'error': 'Bucket environment variable not configured.'})
                }


            presigned_url = s3_client.generate_presigned_url(
                ClientMethod='put_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': file_name,
                    'ContentType': file_type
                },
                ExpiresIn=300
            )

            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'OPTIONS,GET'
                },
                'body': json.dumps({'uploadURL': presigned_url})
            }
        else:
            print("exception 110",traceback.print_exc())
            return {
                'statusCode': 405,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'OPTIONS,GET'
                },
                'body': json.dumps({'error': 'Method Not Allowed'})
            }

    except Exception as e:
        print("exceptionn",traceback.print_exc())
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,GET'
            },
            'body': json.dumps({'error': f"An unexpected error occurred: {str(e)}"})
        }
