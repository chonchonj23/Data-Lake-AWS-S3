import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
#     song_data = input_data + "song_data/A/A/*/*.json"
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs_data/songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude' )\
                      .withColumnRenamed('artist_name', 'name')\
                      .withColumnRenamed('artist_location', 'location')\
                      .withColumnRenamed('artist_latitude', 'latitude')\
                      .withColumnRenamed('artist_longitude', 'longitude')\
                      .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'songs_data/artists/artists.parquet'), 'overwrite') 
    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
#     log_data = input_data + "log-data/2018/11/*.json"
     log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    filter_df = df.filter(df.page == 'NextSong')\
                  .select('page', 'ts', 'userId', 'level', 'song', 'length', 'artist', 'sessionId', 'location', 'userAgent')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'log_data/users/users.parquet'), 'overwrite')
           
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x)/1000))
    filter_df = filter_df.withColumn('timestamp', get_timestamp(filter_df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    filter_df = filter_df.withColumn('datetime', get_datetime(filter_df.ts))
        
    # extract columns to create time table
    filter_df = filter_df.withColumn('hour', hour(filter_df.datetime))\
            .withColumn('day', dayofmonth(filter_df.datetime))\
            .withColumn('week', weekofyear(filter_df.datetime))\
            .withColumn('month', month(filter_df.datetime)) \
            .withColumn('year', year(filter_df.datetime)) \
            .withColumn('weekday', dayofweek(filter_df.datetime))

    time_table = filter_df.select('datetime', 'hour', 'day', 'week', 'month', 'year', 'weekday').dropDuplicates()

    time_table = time_table.withColumnRenamed('datetime','start_time')  
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'log_data/time/time_table.parquet'), 'overwrite') 

    # read in song data to use for songplays table
    songdata_df = spark.read.json(input_data + "song_data/*/*/*/*.json")
    
    # create view for song data and log data accordingly for SQL merger in next step
    songdata_df.createOrReplaceTempView('song_df_view')
    filter_df.createOrReplaceTempView('log_df_view')
        
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
    select 
    ld.datetime, 
    ld.year,
    ld.month,
    ld.userID, 
    ld.level, 
    sd.song_id,
    sd.artist_id, 
    ld.sessionID,
    ld.location, 
    ld.userAgent
    from log_df_view ld, song_df_view sd
    where ( ld.page == "NextSong"
    AND ld.artist == sd.artist_name 
    AND ld.song == sd.title 
    AND ld.length == sd.duration )
    '''
    )

    songplays_table = songplays_table.withColumnRenamed('datetime','start_time')
        
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'merged_data/songplays/songplays.parquet'), 'overwrite')

    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://superchon-udacity/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
