#### import python module
import sys

#import from python modules
from datetime import datetime

#import pyspark module
from pyspark.context import SparkContext
import pyspark.sql.functions as f


#import glue modules
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

#initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

#parameters
glue_db = "imdb"
glue_tbl = "input"
s3_write_path = "s3://demo-glue-pavan/output"

############################################
#  Extract (read DAta)
############################################

#Log stream time
dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print ("start time :",dt_start)

#read movi data to Glue Dybamic frame
dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = glue_db,table_name = glue_tbl)

#Convert dynamic frame to data frame to use standard pyspark function
data_frame = dynamic_frame_read.toDF()

####################################################
#### TRANSFORM (MODIFY DATA)
####################################################

#Create a decade cloume from year
decade_col = f.floor(data_frame["year"]/10)*10
data_frame = data_frame.withColumn("decade",decade_col)

#group by decade : Count movies ,get avareage ratting

data_frame_aggerafated = data_frame.groupby("decade").agg(f.count(f.col("original_title")).alias('movie_count'),f.mean(f.col("avg_vote")).alias("rating_mean"))

#Sort by the number of movies per decade
data_frame_aggerafated  = data_frame_aggerafated.orderBy(f.desc("movie_count"))

#pint the table
#Note show function is as action . Action fources the execution of the data frame plan.
# With big data he slowdown would be significant without cacching

data_frame_aggerafated.show(10)

###################################################################
########## LOAD (WRITE DATA)
###################################################################

# CREATE JUST 1 PARTATION , BECAUSE there is little data

data_frame_aggerafated = data_frame_aggerafated.repartition(10)

#Convert back to dynamic frame

dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggerafated,glue_context,"dynamic_frame_write")

#write data back to s3
glue_context.write_dynamic_frame.from_options(
frame = dynamic_frame_write,
connection_type = "s3",
connection_options = {
    "path" : s3_write_path,
    #hrere you cloud create s3 prefic according to a value in seperate cloums 
    #"partationKeys":["decate"]
},
    format = "csv"
)

#log end time
dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("end time :",dt_end)