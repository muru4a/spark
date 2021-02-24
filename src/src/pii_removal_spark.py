from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,StringType,md5,sha2
from pyspark.sql import functions as F
import os
import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import hashlib
import sys

#Spark Intialization
spark= SparkSession.builder.master("local[*]").\
               appName("hash").\
               getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.server-side-encryption-algorithm","AES256")

#Get the Processing Date
#today = date.today()
today = date(2019,6,20)
months_back = today - relativedelta(months=1)
delta = today - months_back

#S3 source location
source_bucket = 'com-fngn-prod-dataeng'
source_folder = 'raw'
#S3 traget location
target_bucket = 'com-fngn-prod-dataeng'
target_folder = 'sf'

#Encryption method
def encrypt_value(prefered_name):
    name=prefered_name.text
    has_vale=hashlib.sha256(name.encode('utf8')).hexdigest()
    return has_vale

hash_udf = udf(encrypt_value,StringType())

#Loop through processing dates
for i in range(delta.days):
    dt = str(months_back + timedelta(i)).replace("-", "")
    print(dt)
    source_location = ("s3a://{}/{}/{}/sf/FSC_Opportunity.csv".format(source_bucket,source_folder,dt))
    print(source_location)
    #Read from S3 Source File Location
    reads3DF=spark.read.option("header", "true").csv(source_location)
    reads3DF.printSchema()
    #Create a Temp View on Dataframe
    reads3DF.createOrReplaceTempView("sf_opporunity")
    res=spark.sql("select * from sf_opporunity")
    #res1=spark.sql("select md5(preferred_name) from sf_opporunity")
    #Apply the cleansing
    result=res.withColumn("Preferred_Name__c",F.when(F.col("Preferred_Name__c").isNotNull(),0).otherwise(F.col("Preferred_Name__c")))
    result.show()
    target_location=("s3a://{}/{}/{}".format(target_bucket,target_folder,dt))
    print(target_location)
    #Write into S3
    print("Writing the cleansed file into S3 location:",target_location)
    result.coalesce(1).write.mode("append").option("header","true").csv(target_location)
spark.stop()
