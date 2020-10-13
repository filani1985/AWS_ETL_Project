# ETL Project

# Project Proposal
We are using imdb movies data set as input 

We downloaded the db from following link
https://www.kaggle.com/stefanoleone992/imdb-extensive-dataset#

we are using IMDb movies.csv as input file and finding avarage movie rating in each year .
our data size is 45.7MB

We have follwoing columes in the file

imdb_title_id,title,original_title,year,date_published,genre,duration,country,language,director,writer,production_company,actors,description,avg_vote,votes,budget,usa_gross_income,worlwide_gross_income,metascore,reviews_from_users,reviews_from_critics

outpt will be 

```
{"decade":{"long":2017,"string":null},"movie_count":3329,"rating_mean":5.696785821568039}
{"decade":{"long":2018,"string":null},"movie_count":3257,"rating_mean":5.688609149524094}
{"decade":{"long":2016,"string":null},"movie_count":3138,"rating_mean":5.658062460165704}
{"decade":{"long":2015,"string":null},"movie_count":2977,"rating_mean":5.616560295599591}
{"decade":{"long":2014,"string":null},"movie_count":2942,"rating_mean":5.6712780421481925}
{"decade":{"long":2019,"string":null},"movie_count":2841,"rating_mean":5.791270679338264}
{"decade":{"long":2013,"string":null},"movie_count":2783,"rating_mean":5.619762845849813}
{"decade":{"long":2012,"string":null},"movie_count":2560,"rating_mean":5.6283984375000085}
{"decade":{"long":2011,"string":null},"movie_count":2429,"rating_mean":5.62087278715522}

```




## Data Cleanup & Analysis

Please check the job code in the job1.py file

we are storing data in aws s3 bucket and run aws clue jobs.

our job code is like parametrised we hardcoed the glue_db = gluse database name , glue_tbl = gluse table name , s3_write_path = location to store output file.

```
#parameters
glue_db = "imdb"
glue_tbl = "input"
s3_write_path = "s3://demo-glue-pavan/output"

####
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
```
data will be available in data_frame for processing 

we are using aggregating fundtion to transorm data 

```

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

```

last setp is save the output data into s3 bucket as single json file


```

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
```

While executing job on aws output will looks like below 




```
Oct 12, 2020, 6:15:43 PM Pending execution
start time : 2020-10-12 12:46:28
+-------+-----------+------------------+
| decade|movie_count| rating_mean| 
+-------+-----------+------------------+ 
|[2017,]| 3329| 5.696785821568039| 
|[2018,]| 3257| 5.688609149524094| 
|[2016,]| 3138| 5.658062460165704| 
|[2015,]| 2977| 5.616560295599591| 
|[2014,]| 2942|5.6712780421481925| 
|[2019,]| 2841| 5.791270679338264| 
|[2013,]| 2783| 5.619762845849813| 
|[2012,]| 2560|5.6283984375000085| 
|[2011,]| 2429| 5.62087278715522| 
|[2009,]| 2298|5.6398607484769565| 
+-------+-----------+------------------+ 
only showing top 10 rows
end time : 2020-10-12 12:46:52
```
