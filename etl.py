import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.functions import to_timestamp, date_format
from pyspark.sql import types as t

"""
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
"""

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function loads data from song_data dataset, extracts columns
    for songs and artist tables, and writes the data to S3 buckets
    in parquet format.
    
    Parameters
    ----------
    spark: session
        This is the spark session that is created.
    input_data: path
        This is the path to the song_data s3 bucket.
    output_data: path
        This is the path to the output S3 bucket.
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration')
    songs_table.createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), mode='overwrite')
    artists_table.createOrReplaceTempView('artists')
    

def process_log_data(spark, input_data, output_data):
    """
    This function loads data from log_data dataset, extracts columns
    for users/time/songplays tables, and writes the data to S3 buckets
    in parquet format.
    
    Parameters
    ----------
    spark: session
        This is the spark session that is created.
    input_data: path
        This is the path to the song_data s3 bucket.
    output_data: path
        This is the path to the output S3 bucket.
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), mode='overwrite')
    users_table.createOrReplaceTempView('users')
    
    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp', (df.ts/1000).cast(dataType=t.TimestampType()))

    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                           hour('timestamp').alias('hour'), 
                           dayofmonth('timestamp').alias('day'),
                           weekofyear('timestamp').alias('week'),
                           month('timestamp').alias('month'),
                           year('timestamp').alias('year'),
                           dayofweek('timestamp').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'), mode='overwrite')
    time_table.createOrReplaceTempView('time')
    
    df = df.withColumn('year', year(df.timestamp))
    df = df.withColumn('month', month(df.timestamp))
    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    df_song = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    df_join = df.join(df_song, (df.song==df_song.title) & (df.artist==df_song.artist_name) & (df.length==df_song.duration), 'left')
    songplays_table = df_join.select('timestamp', df.year, df.month, 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'), mode='overwrite')
    songplays_table.createOrReplaceTempView('songplays')

def main():
    spark = create_spark_session()
    input_data = 's3a://datalbc/input/'
    output_data = 's3a://datalbc/output/'
    #output_data = '~/output/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
