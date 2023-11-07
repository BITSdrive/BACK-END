import json
import boto3
import datetime
import uuid

# SageMaker runtime client creation
smruntime_client = boto3.client('sagemaker-runtime')
ENDPOINT_NAME = "endpoint"  # Set the SageMaker endpoint name
bucket_name = "s3-event-20231028"
result_bucket_name = "s3-final-20231028"  # Specify the destination S3 bucket name

def callSagemakerEndpoint(payload, endpoint_name):
    response = smruntime_client.invoke_endpoint(
        EndpointName=endpoint_name,
        ContentType='application/json',
        Body=json.dumps(payload)
    )
    return response['Body'].read().decode()

def lambda_handler(event, context):
    try:
        # S3 client creation
        s3 = boto3.client('s3')

        # List objects in the S3 bucket
        response = s3.list_objects_v2(Bucket=bucket_name)

        # Check if the bucket contains any objects
        if 'Contents' not in response:
            return {
                'statusCode': 400,
                'body': 'No objects found in the specified bucket.'
            }

        # Sort objects by last modified date
        sorted_objects = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)

        # Get the latest 2 object keys
        img1_path = sorted_objects[0]['Key']
        img2_path = sorted_objects[1]['Key'] if len(sorted_objects) >= 2 else None

        # SageMaker endpoint call payload
        payload = {
            'img1_path': img1_path,
            'img2_path': img2_path,
            'bucket_name': bucket_name
        }
        result = callSagemakerEndpoint(payload, ENDPOINT_NAME)

        result_file_name = f"result_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:4]}.txt"
        s3.put_object(Body=result, Bucket=result_bucket_name, Key=result_file_name)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'result_file': result_file_name,
                'result': result
            })
        }

    except Exception as e:
        print("Error:", str(e))
        return {
            'statusCode': 500,
            'body': str(e)
        }
