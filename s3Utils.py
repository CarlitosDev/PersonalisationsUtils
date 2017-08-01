import boto3
import io
import pandas
from os import path
from subprocess import call
import gzip


def awsauth():
    call('awsauth')

def getCSV(key, bucket='beamly-marketing-science'):
    keys = get_aws_credentials()
    client = boto3.client(
        's3',
        aws_access_key_id=keys['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=keys['AWS_SECRET_ACCESS_KEY']
    )
    s3_obj = client.get_object(Key=key, Bucket=bucket)
    return pandas.read_csv(io.BytesIO(s3_obj['Body'].read()))


def put(dataframe, key, bucket='beamly-marketing-science'):
    keys = get_aws_credentials()
    client = boto3.client(
        's3',
        aws_access_key_id=keys['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=keys['AWS_SECRET_ACCESS_KEY']
    )
    body = dataframe.to_csv(index=False)
    client.put_object(Body=body, Bucket=bucket, Key=key)


def get_aws_credentials():
    with open(path.expanduser('~/.aws/env')) as f:
        env_vars_tuple = [(l.split(' ')[1].split('=')) for l in f]
        env_vars_dict = {var[0]: var[1].replace('\n', '') for var in env_vars_tuple}
        return env_vars_dict


# -----------------------------
# Carlos' functions added here:

# S3 list all keys with the prefix
def getFolderList(folderName, bucketName = 'beamly-metrics-data-stage'):
    s3Conn     = boto3.resource('s3')
    thisBucket = s3Conn.Bucket(bucketName)
    thisPrefix = folderName + '/'
    bucketList = thisBucket.objects.filter(Prefix=thisPrefix);
    folderList = [currentKey.key for currentKey in bucketList];
    return folderList

# get a list of the files within the bucket
def getBucketContentsList(bucketName = 'beamly-metrics-data-stage'):
    s3Conn     = boto3.resource('s3')
    thisBucket = s3Conn.Bucket(bucketName)
    bucketContents = list(thisBucket.objects.all())
    return bucketContents


def transferKeyFromBucket(currentKey, localDestination, bucketName = 'beamly-metrics-data-stage'):
    s3Conn = boto3.resource('s3')
    s3Conn.meta.client.download_file(bucketName, currentKey, localDestination)


def getKeyFromBucket(currentKey, localDestination, bucketName = 'beamly-metrics-data-stage'):
    s3Conn = boto3.resource('s3')
    obj    = s3Conn.Object(bucket_name=bucketName, key=currentKey)
    data = obj.get()["Body"].read()


# -----------------------------
# From dojo's s3/s3.py:

def compress(string):
    """
    Gzips a string using default compression level of 9, which is maximum
    :param string:
    :return: gzipped string
    """

    out = StringIO.StringIO()
    with gzip.GzipFile(fileobj=out, mode="w") as f:
        f.write(string)

    return out.getvalue()


def decompress(string):
    """
    Decompresses a gzipped string
    :param string:
    :return: unzipped string; None if error
    """

    decompressed_string = gzip.GzipFile('', 'r', 0, StringIO.StringIO(string)).read()
    return decompressed_string


def read_content_as_string(filename, bucketName = 'beamly-metrics-data-stage'):
    """
    Reads from S3 and decompresses any gzip encoded content
    :param file_key:
    :return:
    """
    s3Conn     = boto3.resource('s3')
    thisBucket = s3Conn.Bucket(bucketName)
    file_key   = thisBucket.get_key(filename)
    file_content = file_key.get_contents_as_string()
    if file_key.content_encoding == 'gzip':
        return decompress(file_content)
    return file_content