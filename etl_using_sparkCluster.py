import configparser
import os
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, hour, weekofyear,date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType,TimestampType
import boto3

config = configparser.ConfigParser()
# below conifg file is in CentOS linux user home directory
config.read_file(open('/home/durgadas/dl.cfg'))

KEY=config.get('AWS','AWS_ACCESS_KEY_ID')
SECRET= config.get('AWS','AWS_SECRET_ACCESS_KEY')
#print(KEY)
#print(SECRET)

#Reading Data from AWS S3 in ITVersity Spark Cluster
def create_spark_session():
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName("Durgadas_SparkApp") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.executor.instances", "7") \
        .getOrCreate()
    sc=spark.sparkContext
    #Files will be written in ITVersity Hadoop files sytem
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3a.awsAccessKeyId", KEY)
    hadoop_conf.set("fs.s3a.awsSecretAccessKey", SECRET)
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    print("Reading songs data files")
    
    song_data = input_data + "/song_data/*/*/*/*.json" 
    
    # read song data file        
    schema = StructType([        
        StructField('artist_id', StringType()),
        StructField('artist_latitude', DoubleType()),
        StructField('artist_longitude', DoubleType()),
        StructField('artist_location', StringType()),
        StructField('artist_name', StringType()),
        StructField('duration', DoubleType()),
        StructField('num_songs', IntegerType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),        
        StructField('year', IntegerType())
    ])

    df = spark.read.schema(schema).json(song_data)
    #df=spark.read.json(song_data)   

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration')    
    songs_table.select('song_id','title','artist_id','year','duration').dropDuplicates().collect()    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.format("parquet").partitionBy("year","artist_id").mode("overwrite").save(output_data + "/songs.parquet")
    
    # extract columns to create artists table    
    artists_table = df.select('artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude')    
    artists_table.select('artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude').dropDuplicates().collect()
    artists_table=artists_table.withColumnRenamed('artist_id','artist_id') \
                                .withColumnRenamed('artist_name','name') \
                                .withColumnRenamed('artist_location','location') \
                                .withColumnRenamed('artist_latitude','lattitude') \
                                .withColumnRenamed('artist_longitude','longitude') 
    # write artists table to parquet files    
    artists_table.write.parquet(output_data + "/artists.parquet",mode='overwrite',compression='snappy')

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    print("Reading logs Data Files")
    log_data = input_data + 'log_data/*/*/*.json'
    #    
    # read log data file
    schema = StructType([
        StructField('artist', StringType()),
        StructField('auth', StringType()),
        StructField('firstName', StringType()),
        StructField('gender', StringType()),
        StructField('itemInSession', IntegerType()),         
        StructField('lastName', StringType()),
        StructField('length', DoubleType()),         
        StructField('level', StringType()),
        StructField('location', StringType()),
        StructField('method', StringType()),
        StructField('page', StringType()),
        StructField('registration', StringType()),         
        StructField('sessionId', IntegerType()),
        StructField('song', StringType()),
        StructField('status', IntegerType()),  
        StructField('ts', IntegerType()), 
        StructField('userAgent', StringType()),        
        StructField('userId', IntegerType())
    ])
    # when applying schema to log_data files, it is not working thatswhy kept without schema
    #df = spark.read.schema(schema).json(log_data)    
    df = spark.read.json(log_data)         
       
    # extract columns for users table    
    users_table = df.select('userId','firstName','lastName','gender','level')   
    users_table.select('userId','firstName','lastName','gender','level').dropDuplicates().collect()
    users_table.filter(users_table.userId!=" ").count()
    users_table=users_table.withColumnRenamed('userId','user_id') \
                                .withColumnRenamed('firstName','first_name') \
                                .withColumnRenamed('lastName','last_name')      
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "/users.parquet",mode='overwrite',compression='snappy')     

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).datetime)
    df = df.withColumn("datetime", get_datetime(col("ts")))    
    
    get_hour = udf(lambda x: datetime.fromtimestamp(x / 1000.0).hour)
    df = df.withColumn("hour", get_hour(df.ts))

    # create day column from datetime
    get_day = udf(lambda x: datetime.fromtimestamp(x / 1000.0).day)
    df = df.withColumn("day", get_day(df.ts))
    
    # create week column from datetime
    get_week = udf(lambda x: datetime.fromtimestamp(x / 1000.0).isocalendar()[1])
    df = df.withColumn("week", get_week(df.ts))
        
    # create month column from datetime
    get_month = udf(lambda x: datetime.fromtimestamp(x / 1000.0).month)
    df = df.withColumn("month", get_month(df.ts))
    
    # create year column from datetime
    get_year = udf(lambda x: datetime.fromtimestamp(x / 1000.0).year)
    df = df.withColumn("year", get_year(df.ts))

    # create weekday column from datetime
    get_weekday = udf(lambda x: datetime.fromtimestamp(x / 1000.0).weekday())
    df = df.withColumn("weekday", get_weekday(df.ts))
            
    # extract columns to create time table
    time_table = df.select(["ts", "hour", "day", "week", "month", "year", "weekday"])

    print('--- Saving time_table')
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('append').partitionBy('year', 'month').parquet(output_data + "time_data")    
    
    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/A/A/*/*.json')
    song_df = spark.read.json(song_data)
    
    print('--Preparing Songs Play Table--')
    
    #this UDF is used for conevrt ts into time stamp filed.    
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    
    getsongplays_table = df.join(song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title),'inner') \
        .withColumn('start_time', get_timestamp(df.ts))\
        .withColumn("songplay_id", monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = getsongplays_table.selectExpr(
        ['songplay_id', 'start_time', 'userId as user_id', 'level', 'song_id', 'artist_id', 'sessionId as session_id',
         'location', 'userAgent as user_agent']) \
        .withColumn('year', year('start_time')) \
        .withColumn('month', month('start_time'))
    
    #write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "/songplays.parquet",mode='overwrite',compression='snappy')            
    
def main():
    spark = create_spark_session()
    #input path which is AWS S3
    input_data = "s3a://udacity-dend/"
    
    #output is Hadoop Cluster
    output_data = "/user/durgadas/output1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
