import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from pyspark import SparkConf, SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


bucket_name = "realtime-usagedata"

source_key_cloudfront = "raw/cloudfront-logs"
source_key_platform = "cleansed/analytic-logs"

days_to_subtract = 1

def day_before(days_to_subtract):
    from datetime import datetime
    
    date = datetime.now() - timedelta(days=days_to_subtract)
    return date.strftime("%Y-%m-%d")
    
def get_year_month_date(date): 
    return date.split("-")[0], date.split("-")[1], date.split("-")[2]

    
date = day_before(days_to_subtract)  
year, month, day = get_year_month_date(date)

def get_date(date):
    year = date.split(" ")[0].split("-")[0]
    month = date.split(" ")[0].split("-")[1]
    day = date.split(" ")[0].split("-")[2]
    hour = date.split(" ")[1].split(":")[0]
    return year, month, day , hour

def daterange(start_date, end_date):
    delta = timedelta(hours=1)
    while start_date < end_date:
        yield start_date
        start_date += delta
        
def merge_cloud_server(year, month, day, hour):
    # --- READ CloudFront -------
    input_path_cloudfront = "s3://" + bucket_name + '/' + source_key_cloudfront + '/partition_year='+ year +'/partition_month=' + month +'/partition_day=' + day + '/partition_hour='+ hour + '/'
    read_cloud_raw = spark.read.parquet(input_path_cloudfront)
    print('cloudfront count RAW is:',read_cloud_raw.count())

    # --------- Cleaning cloudfront ----------
    # 1) clean URL
    cloudfront = read_cloud_raw.withColumn("cf_uri",
                                  expr("CASE WHEN query_string LIKE 'query%' THEN CONCAT(uri, '?', query_string)" +
                                       "WHEN query_string LIKE 'uri=%' THEN SUBSTRING(query_string, 5, 999)" +
                                       "ELSE uri END"))

    # 2) Clean timestamp and change data type
    cloudfront = cloudfront.withColumn("cf_timestamp_view", concat(col('date_view'), lit(' '), col('time_view')))
    cloudfront = cloudfront.withColumn('cf_timestamp_view', to_timestamp(col("cf_timestamp_view"), 'yyyy-MM-dd HH:mm:ss')) 

    # 3) Add UNIX column
    cloudfront = cloudfront.withColumn("unix1", cloudfront.cf_timestamp_view.cast("long"))
    print('cloudfront count CLEANSED is:',cloudfront.count())
    
    # -------- READ data  -------------
    input_path_platform = "s3://" + bucket_name + '/' + source_key_platform + '/partition_year='+ year +'/partition_month=' + month +'/partition_day=' + day  + '/partition_hour=' + hour +'/'
    read_server = spark.read.option("mergeSchema","true").parquet(input_path_platform)
    
    # 1) clean from bugs and NULL USERS
    server = read_server.filter(~(read_server.request_uri.like("%.replace(%")))
    server = server.filter(server.user_ip.isNotNull())

    # 2) clean timestamp
    server = server.withColumn('timestamp_view', to_timestamp(server.timestamp_view, 'yyyy-MM-dd HH:mm:ss'))

    # 3) Add UNIX timestamp column
    server = server.withColumn("unix2", server.timestamp_view.cast("long"))

    # 4) add "normalized_isbn" colomn
    server =  server.withColumn('normalized_isbn', translate(col('content_isbn'), "-", ""))

    server = server.fillna(-999)
    # 5) change data type of two columns
    from pyspark.sql.types import IntegerType,BooleanType,DateType
    server = server.withColumn("was_authorized",server.was_authorized.cast(BooleanType())).withColumn("was_successful",server.was_successful.cast(BooleanType()))

    # join cf and server
    join_cloud_server = server.join(cloudfront, (server.user_ip == cloudfront.request_ip)
                         & (abs(server.unix2 - cloudfront.unix1) <= 10)
                         & (server.request_uri == cloudfront.cf_uri), how='left'
                           
    join_cloud_server = join_cloud_server.fillna(-999)

    write_to_s3(join_cloud_server, year, month, day, hour)

BucketData = '****-realtime-usagedata'
def write_to_s3(df, year, month, day, hour):
    path = (
            "s3://"
            + BucketData
            + "/TEST_etl"
            + "/partition_year="
            + year
            + "/partition_month="
            + month
            + "/partition_day="
            + day
            + "/partition_hour="
            + hour

    )
    df.repartition(5).write.option("header", False).mode("append").parquet(path)
    print(path)

start_date = datetime(int(year), int(month), int(day), 0, 00)
end_date = datetime(int(year), int(month), int(day), 23, 59)

for single_date in daterange(start_date, end_date):
    try:
        year, month, day, hour = get_date(single_date.strftime("%Y-%m-%d %H:%M"))
        #print('--->', year, month, day, hour)
        merge_cloud_server(year, month, day, hour)
    except Exception:
        continue

job.commit()

