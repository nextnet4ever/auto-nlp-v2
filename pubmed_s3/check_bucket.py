import boto3

# create an S3 client object
s3 = boto3.client('s3')

# specify the S3 bucket and prefix to list objects from
bucket_name = 'pmc-oa-opendata'
prefix = 'oa_comm/txt/all/'

# list the first 100 objects in the S3 bucket
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=100)

# loop through the object keys and print them
for obj in response['Contents']:
    print(obj['Key'])
