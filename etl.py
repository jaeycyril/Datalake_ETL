import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, \
                                FloatType, DoubleType, LongType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data")

    song_schema = StructType([
    StructField("num_songs", IntegerType(), True),
    StructField("artist_id", StringType(), False),
    StructField("artist_latitude", DoubleType(), True),
    StructField("artist_longitude", DoubleType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("song_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("Year", IntegerType(), True)
    ])
    
    # read song data file
    df = spark.read.json(f'{song_data}/*/*/*/*.json', multiLine=True, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.filter(df.song_id.isNotNull()).select("song_id", "title", "artist_id", "artist_name", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f"{output_data}/songs_table/", mode='overwrite', partitionBy=('year', 'artist_id'))

    # extract columns to create artists table
    artists_table = df.filter(df.artist_id.isNotNull())\
                            .select("artist_id", F.col("artist_name").alias("name"),
                            F.col("artist_location").alias("location"), 
                            F.col("artist_latitude").alias("latitude"), 
                            F.col("artist_longitude").alias("longitude")).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}/artists_table/", mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data")

    # read log data file
    log_df = spark.read.json(f"{log_data}/*/*/*.json")

    # Parse timestamp column to get start_time
    log_df = log_df.withColumn("start_time", F.from_unixtime(F.col("ts")/1000).alias("start_time"))

    # extract columns for users table    
    users_table = log_df.filter(log_df.userId.isNotNull())\
                        .select("userId", "firstName", "lastName", "gender", "level")\
                        .distinct()
    
    # write users table to parquet files
    users_table.write.parquet(f"{output_data}/users_table/", mode='overwrite')
    
    # extract columns to create time table
    time_table = log_df.filter(log_df.start_time.isNotNull())\
                            .select(F.col("start_time"),\
                           F.hour("start_time").alias("hour"),\
                           F.dayofmonth("start_time").alias("day"),\
                           F.weekofyear("start_time").alias("week"),\
                           F.month("start_time").alias("month"),\
                           F.year("start_time").alias("year"),\
                           F.dayofweek("start_time").alias("weekday"))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f"{output_data}/time_table/", mode='overwrite', partitionBy=('year', 'month'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(f"{output_data}/songs_table/")

    # Clean the columns to be used for joining and create join conditions
    log_df = log_df.withColumn("artist_cleaned", F.lower(F.trim(F.col("artist"))))
    log_df = log_df.withColumn("song_cleaned", F.lower(F.trim(F.col("song"))))

    song_df = song_df.withColumn("artist_name_cleaned", F.lower(F.trim(F.col("artist_name"))))
    song_df = song_df.withColumn("title_cleaned", F.lower(F.trim(F.col("title"))))

    cond = [log_df.artist_cleaned==song_df.artist_name_cleaned,\
            log_df.song_cleaned == song_df.title_cleaned, log_df.length == df.duration]

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(song_df, cond).filter(F.col("page")=="NextSong")\
                            .select(F.monotonically_increasing_id().alias("songplay_id"), "start_time",
                            "userId", "level", "song_id", "artist_id", "sessionId",
                            "location", "userAgent",
                            F.month("start_time").alias("month"),\
                            F.year("start_time").alias("year"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f"{output_data}/songplays_table/", mode='overwrite', partitionBy=('year', 'month'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-etl-output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
