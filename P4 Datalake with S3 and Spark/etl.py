import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read song data and process it and save to provided output location
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
        
    # get filepath to song data file
    song_data = input_data +'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    song_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(song_cols)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data+'songs.parquet', 'overwrite')

    # extract columns to create artists table
    artist_cols = ['artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude']
    artists_table = df.selectExpr(artist_cols)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists.parquet', 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Read log data and process it and save to provided output location
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
        
    # get filepath to log data file
    log_data = input_data +'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table   
    user_cols = ['userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level'] 
    users_table = df.selectExpr(user_cols) 
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'users.parquet', 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf( lambda x: datetime.datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select( 'datetime'
                           , hour('datetime').alias('hour')
                           , dayofmonth('datetime').alias('day')
                           , weekofyear('datetime').alias('week')
                           , month('datetime').alias('month')
                           , year('datetime').alias('year')
                           , dayofweek('datetime').alias('weekday') )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data+'time.parquet', 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title).select('datetime', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data+'songplays.parquet','overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
