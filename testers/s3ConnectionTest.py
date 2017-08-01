
import s3Utils as s3
s3.awsauth();


# using boto3 directly - it worked
# botocore.exceptions.ClientError: An error occurred (403) when calling the HeadObject operation: Forbidden
import boto3
s3Conn = boto3.resource('s3')
bucketName = 'beamly-metrics-data-stage';
currentKey = 'data/raw/adform/masterdataset/Click_70925.csv.gz';
s3Conn.meta.client.download_file(bucketName, currentKey, 'tmpHello2.csv.gz')

folderName = 'data/raw/adform/masterdataset'
folderList = s3.getFolderList(folderName);

bucketName = 'beamly-metrics-data-stage';
currentKey = 'data/ideal/adform/masterdataset/Click_70510.json';

# it fails
import boto3
import io
s3Conn = boto3.resource('s3')
obj = s3Conn.Object(bucket_name=bucketName, key=currentKey)
data = obj.get()["Body"].read()
a    = io.BytesIO(data)
df = pandas.read_json(a)

put_object(Key='test.jpg', Body=data)



a = s3.read_content_as_string(currentKey);


s3Conn     = boto3.resource('s3')
thisBucket = s3Conn.Bucket(bucketName)

s3_bucket_key = s3Conn.meta.client.get_key(currentKey)

file_key   = thisBucket.get_key(filename)
file_content = file_key.get_contents_as_string()
if file_key.content_encoding == 'gzip':
    return decompress(file_content)
return file_content








bucketName = 'beamly-metrics-data-stage';
currentKey = 'data/ideal/adform/masterdataset/Click_70510.json';
client = boto3.client('s3')
s3_obj = client.get_object(Key=currentKey, Bucket=bucketName)
dataAsStr = io.BytesIO(s3_obj['Body'].read())
df =  pandas.read_json(dataAsStr)





# get a list of the files within the bucket
bucketName = 'beamly-metrics-data-stage';
thisBucket = s3Conn.Bucket(bucketName)
for object in thisBucket.objects.all():
    print(object.key)

buckets = list(thisBucket.objects.all())


# using boto3 directly - also forbidden
import boto3
s3Conn = boto3.resource('s3')
bucketName = 'beamly-metrics-data-stage';
currentKey = 'data/raw/adform/masterdataset/Click_70925.csv.gz';
s3Conn.Bucket(bucketName).download_file(currentKey, 'my_local_copy.csv.gz')

# using boto3 directly - also forbidden
import boto3
s3Conn = boto3.resource('s3')
bucketName = 'beamly-metrics-data-stage';
s3Conn.Bucket(bucketName)
currentKey = 'data/raw/adform/masterdataset/Click_70183.csv.gz';
s3Conn.Bucket(bucketName).download_file(currentKey, 'my_local_copy.csv.gz')


# using boto3 directly - also forbidden
import boto3
import botocore
s3Conn = boto3.resource('s3')
bucketName = 'beamly-metrics-data-stage';
try:
    s3Conn.Bucket(bucketName).download_file(currentKey, 'my_local_copy.csv.gz');
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise


# using boto3 - check the HeadObject first - also forbidden
import boto3
import botocore

s3Conn = boto3.resource('s3')
s3Conn.Bucket(bucketName)
exists = True
try:
    s3Conn.meta.client.head_bucket(Bucket=bucketName)
except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
        exists = False


# using boto3 directly
import boto3
# boto3 will automatically look up for the credentials (https://boto3.readthedocs.io/en/latest/guide/configuration.html#guide-configuration)
client = boto3.client('s3')
bucketName = 'beamly-metrics-data-stage';
currentKey = 'data/raw/adform/masterdataset/Click_70183.csv.gz';
s3_obj = client.get_object(currentKey, bucketName)


# Example: get a file from s3
s3 = boto3.resource('s3')
bucketName = 'beamly-metrics-data-stage';
s3.Bucket(bucketName)

currentKey = 'data/ideal/adform/masterdataset/Click_70510.json';
s3.Bucket(bucketName).download_file(currentKey, 'my_local_copy2.json')


# get a list of files
s3 = boto3.resource('s3')
bucketName = 'beamly-metrics-data-stage';
s3.Bucket(bucketName)
bucket = s3.get_bucket(bucketName)
a = list(s3.Bucket.list("", "/"))

# try out s3 connection
# --------------------------------------------
import s3Utils
s3Utils.awsauth();
crd = s3Utils.get_aws_credentials()
bucketName = 'beamly-metrics-data-stage';
df = s3Utils.getJSON('data/ideal/adform/masterdataset/Click_70510.json', bucketName)




bucketName = 'beamly-metrics-data-stage';
currentKey = 'data/ideal/adform/masterdataset/Click_70510.json';
client = boto3.client('s3')
s3_obj = client.get_object(Key=currentKey, Bucket=bucketName)
dataAsStr = io.BytesIO(s3_obj['Body'].read())
df =  pandas.read_json(dataAsStr)


bucketName = 'beamly-metrics-data-stage';

import botocore

s3Conn = boto3.resource('s3')
bucket = s3Conn.Bucket(bucketName)
exists = True
try:
    s3Conn.meta.client.head_bucket(Bucket=bucketName)
except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
        exists = False

for key in bucket.objects.all():
    print(key.key)




session = boto3.Session)
dev_s3_client = session.client('s3')