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
import urllib.parse
import gzip
import awswrangler
import pandas as pd


source_bucket = "live-analyticstransfer-data"
source_prefix = "analyticstransfer/"
dest_bucket = "realtime-usagedata"
dest_key = "raw/analytic-logs/"

def day_before(days_to_subtract):
    from datetime import datetime
    
    date = datetime.now() - timedelta(days=days_to_subtract, hours = 3)
    return date.strftime("%Y-%m-%d-%H")
    
def get_year_month_date(date): 
    return date.split("-")[0], date.split("-")[1], date.split("-")[2], date.split("-")[3]
    
def transformation_to_parquet(source_bucket, obj, dest_bucket, dest_key, year, month, day, hour):
    in_path = "s3://" + source_bucket + "/" + obj 
    s3 = boto3.resource("s3")
    df = pd.read_csv(in_path, header=0, sep=',', quotechar='"', error_bad_lines = False)
    out_path = "s3://" + dest_bucket + "/" + dest_key + "partition_year=" + year + "/partition_month=" + month + "/partition_day=" + day + "/partition_hour=" + hour + "/" + obj.split('/')[-1] + ".parquet"
    df.to_parquet(out_path, engine = "pyarrow")
    

def lambda_handler(event, context):
    
    # TODO implement
    delay = 0
    hour_to_process = day_before(delay)
    year, month, day, hour = get_year_month_date(hour_to_process)
    source_prefix_now = "analyticstransfer/" + year + "-"+ month + "-"+ day
    
    s3 = boto3.client('s3')
    my_bucket = s3.list_objects_v2(Bucket= source_bucket, Prefix = source_prefix_now)['Contents']
    
    for my_bucket_object in my_bucket:
        if hour == my_bucket_object['Key'].split('/')[-1].split('-')[-1].split('.')[0]:
            print(my_bucket_object['Key'])
            transformation_to_parquet(source_bucket, my_bucket_object['Key'], dest_bucket, dest_key, year, month, day, hour )
            
    return hour_to_process, year, month, day, hour
    
