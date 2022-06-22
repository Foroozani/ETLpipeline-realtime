import os
import sys
import subprocess

# pip install custom package to /tmp/ and add to path
subprocess.call('pip install fsspec s3fs -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
sys.path.insert(1, '/tmp/')

import fsspec
import s3fs
import json

from datetime import timedelta, datetime
import boto3
import pyarrow.csv as pv
import pyarrow.parquet as pq 
import gzip
import urllib.parse
import awswrangler
import pandas as pd


source_bucket = "degruyter-live-cdnathena"
source_prefix = "partitioned/"
dest_bucket = "degruyter-realtime-usagedata"
dest_key = "raw/cloudfront-logs/"

def day_before(days_to_subtract):
    from datetime import datetime
    
    date = datetime.now() - timedelta(days=days_to_subtract, hours = 1)
    return date.strftime("%Y-%m-%d-%H")
    
def get_year_month_date(date): 
    return date.split("-")[0], date.split("-")[1], date.split("-")[2], date.split("-")[3]
    
def transformation_to_parquet(source_bucket, obj, dest_bucket, dest_key, year, month, day, hour):
    in_path = "s3://" + source_bucket + "/" + obj 
    s3 = boto3.resource("s3")
    df = pd.read_csv(in_path, compression='gzip', sep='\t', header = None,  error_bad_lines=True, skiprows = 2, names = [
        'date_view',
        'time_view',
        'location',
        'bytes',
        'request_ip',
        'method',
        'host',
        'uri',
        'status',
        'referrer',
        'user_agent',
        'query_string',
        'cookie',
        'result_type',
        'request_id',
        'host_header',
        'request_protocol',
        'request_bytes',
        'time_taken',
        'xforwarded_for',
        'ssl_protocol',
        'ssl_cipher',
        'response_result_type',
        'http_version',
        'fle_status',
        'fle_encrypted_fields',
        'c_port',
        'time_to_first_byte',
        'x_edge_detailed_result_type',
        'sc_content_type',
        'sc_content_len',
        'sc_range_start',
        'sc_range_end'])
    
    out_path = "s3://" + dest_bucket + "/" + dest_key + "partition_year=" + year + "/partition_month=" + month + "/partition_day=" + day + "/partition_hour=" + hour + "/" + obj.split('/')[-1] + ".parquet"
    df.to_parquet(out_path, engine = "pyarrow")
    

def lambda_handler(event, context):
    
    # TODO implement
    delay = 0 
    hour_to_process = day_before(delay)
    year, month, day, hour = get_year_month_date(hour_to_process)
    source_prefix_now = "partitioned/" + year + "/"+month + "/"+ day + "/" + hour + "/cf-logs" 
    
    s3 = boto3.client('s3')
    my_bucket = s3.list_objects_v2(Bucket= source_bucket, Prefix = source_prefix_now)['Contents']

    for my_bucket_object in my_bucket:
        transformation_to_parquet(source_bucket, my_bucket_object['Key'], dest_bucket, dest_key, year, month, day, hour )
       

    return hour_to_process, year, month, day, hour
    
