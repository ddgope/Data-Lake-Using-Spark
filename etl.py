import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as udf
from pyspark.sql.functions import unix_timestamp

config = configparser.ConfigParser()
#config.read('dl.cfg')
config.read_file(open('dl.cfg'))

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
KEY=config.get('AWS','AWS_ACCESS_KEY_ID')
SECRET= config.get('AWS','AWS_SECRET_ACCESS_KEY')
#print(KEY)

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()  
    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3a.awsAccessKeyId", KEY)
    hadoop_conf.set("fs.s3a.awsSecretAccessKey", SECRET)
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "/song_data/A/A/A/*.json"
    
    # read song data file    
    df=spark.read.json(song_data)

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
    log_data = input_data + "/log_data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays -> THis I am not able to understand. Filter I can do but in which filed or column I need to do.    
    #df = 

    # extract columns for users table    
    users_table = df.select('userId','firstName','lastName','gender','level')   
    users_table.select('userId','firstName','lastName','gender','level').dropDuplicates().collect()
    users_table.filter(users_table.userId!=" ").count()
    users_table=users_table.withColumnRenamed('userId','user_id') \
                                .withColumnRenamed('firstName','first_name') \
                                .withColumnRenamed('lastName','last_name')      
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "/users.parquet",mode='overwrite',compression='snappy')

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = df.select('ts')      
    time_table.select('ts').dropDuplicates().collect()
    time_table = time_table.withColumn("ts",udf.to_timestamp(udf.from_unixtime(udf.col("ts")/1000))) \
                           .withColumn("year", udf.year("ts")) \
                           .withColumn("month", udf.month("ts")) \
                           .withColumn("dayofmonth", udf.dayofmonth("ts")) \
                           .withColumn("hour", udf.hour("ts")) \
                           .withColumn("weekofyear", udf.weekofyear("ts")) \
                           .withColumn("weekday", udf.dayofweek("ts")) \
                           .withColumn("date_format", udf.date_format("ts",'MM/dd/yyy'))    
    
    # write time table to parquet files partitioned by year and month        
    time_table.write.format("parquet").partitionBy("year","month").mode("overwrite").save(output_data + "/time.parquet")

    # read in song data to use for songplays table
    song_df=spark.read.json("s3a://udacity-dend/song_data/A/A/A/*.json")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df    
    songsplay_final=songplays_table.join(song_df,songplays_table.song==song_df.title,'inner') \
                                   .drop('auth','itemInSession','method','page','registration','status','firstName','lastName','gender','artist_latitude', \
                                          'artist_location','artist_longitude','artist_name','duration','num_songs','year','song','artist','title')  
    
    songsplay_final=songsplay_final.withColumnRenamed('userId','user_id') \
                                   .withColumnRenamed('sessionId','session_id') \
                                   .withColumnRenamed('userAgent','user_agent')

    # write songplays table to parquet files partitioned by year and month
    songsplay_final.write.parquet(output_data + "/songplays.parquet",mode='overwrite',compression='snappy')
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "data/output6/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
