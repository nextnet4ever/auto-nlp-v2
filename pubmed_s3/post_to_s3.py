import boto3
import os

s3 = boto3.resource('s3', 
        aws_access_key_id='AKIARLSKRVQP433MXUZG',
        aws_secret_access_key='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'
        )
bucket_name = 'derek-use-case-literature'
folder_path = 'processed/'

for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)
    if os.path.isfile(file_path):
        with open(file_path, 'rb') as file:
            s3.Object(bucket_name, file_name).put(Body=file)
