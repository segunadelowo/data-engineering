import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    This procedure creates a SparkSession and returns a running session if it exisits.
    
    Args:
        None
    Returns:
        SparkSession
    """
    
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This procedure processes the song data extracted from S3 bucket
    and the tables for songs and artists records are created
    
    Args:
        spark: SparkSession
        input_data: S3 bucket location 
        output_data: output directory to hold processed records
    Returns:
        None
    """
    
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
     
    
    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'artist_name','year', 'duration']
    
    # remove duplicate song records
    songs_table = songs_table.drop_duplicates(subset=['song_id'])
        
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data+'songs_table', 'overwrite')
    
    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_latitude', 'artist_location', 'artist_longitude', 'artist_name']
    
    # remove duplicate artist records
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists_table', 'overwrite')

    
    
def process_log_data(spark, input_data, output_data):
    """
    This procedure processes the log data extracted from S3 bucket
    and the tables for users, time, and songplays records are created
    
    Args:
        spark: SparkSession
        input_data: S3 bucket location 
        output_data: output directory to hold processed records
    Returns:
        None
    """
    
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df= df.where(col("page")=="NextSong")

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level','ts']
    
    # remove duplicate user records
    users_table = users_table.drop_duplicates(subset=["userId"])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table', 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create the columns start_time, hour, day, week, month, year, weekday from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), DateType())
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    get_hour = udf(lambda x: x.hour)
    df = df.withColumn('hour', get_hour(df.start_time))

    get_day = udf(lambda x : x.day)
    df = df.withColumn('day', get_day(df.start_time))
       
    get_week = udf(lambda x: datetime.isocalendar(x)[1])
    df = df.withColumn('week', get_week(df.start_time))
       
    get_month = udf(lambda x: x.month)
    df = df.withColumn('month', get_month(df.start_time))
      
    get_year = udf(lambda x: x.year)
    df = df.withColumn('year', get_year(df.start_time))
       
    get_weekday = udf(lambda x: x.weekday())
    df = df.withColumn('weekday', get_weekday(df.start_time))
    

    # extract columns to create time table
    time_table  = df['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    # remove duplicates
    time_table = time_table.drop_duplicates(subset=['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time_table', 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table')
    
    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df['song_id', 'title', 'artist_id','artist_name'], (song_df.title == df.song) & (song_df.artist_name == df.artist))
    
    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=monotonically_increasing_id#pyspark.sql.functions.monotonically_increasing_id
    # The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive. 
    df = df.withColumn('songplay_id', monotonically_increasing_id())
    songplays_table = df['songplay_id','start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent','year','month']

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays_table', 'overwrite')

    
def main():
    """
    This procedure excutes the steps needed to create a spark session required in reading from the S3 bucket
    and executes ETL which later stores output into to S3.
    Returns:
        None
    """
    
    print('--creating spark session--')
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-sparkify-datalake01/data/out_put/"
    
    print('--executing process_song_data--')
    process_song_data(spark, input_data, output_data)    
    
    print('--executing process_log_data--')
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
