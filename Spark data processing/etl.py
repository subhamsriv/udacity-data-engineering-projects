import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types 
from pyspark.sql import functions as F





def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Loads data from s3 to spark
       creates song and artist dataframe
       write back the song and artist dataframe to s3 in parquet format"""
    
    # read song data file
    df = spark.read.json(input_data +'song_data/A/*/*/*.json')
    df.persist()

    # extract columns to create songs table
    songs_table = df.dropDuplicates(['song_id']) \
                    .select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data+'udacity/song')

    # extract columns to create artists table
    artists_table = df.dropDuplicates(['artist_id']) \
                      .select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']) \
                      .toDF(*['artist_id', 'name', 'location', 'lattitude', 'longitude'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'udacity/artist')


def process_log_data(spark, input_data, output_data):
    """Loads data from s3 to spark
       creates user, songplays and time dataframe
       write back the user, songplays and time dataframe to s3 in parquet format"""

    # read log data file
    df = spark.read.json(input_data+'log_data/2018/11/*.json')
    df.persist()
    
    # filter by actions for song plays
    df = df.filter(df_event.page == "NextSong")

    # extract columns for users table    
    user_table = df.dropDuplicates(['userId']) \
                   .select(['userId','firstName','lastName','gender','level'])
    
    # write users table to parquet files
    user_table.write.parquet(output_data+'udacity/user')

    # create start_time column from original timestamp column 
    df = df.withColumn('start_time',(df_event['ts']/1000).cast(types.TimestampType()))
    
    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+"song_data/A/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, \
                [song_df.artist_name == df.artist, song_df.duration == df.length, song_df.title == df.song],'inner') \
                .select(['start_time','userId','level','song_id','artist_id','sessionId','location','userAgent']) 
    
    # Adding year and month for partitioning the dataframe
    songplays_table = songplays_table.withColumn('month',F.month('start_time')) \
                                     .withColumn('year',F.year('start_time'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data+'udacity/songplay')
    
    # create time column list
    time_column_list = ['start_time',
        F.hour('start_time').alias('hour'),\
        F.dayofmonth('start_time').alias('day'),\
        F.date_trunc('week', 'start_time').alias('week'),\
        F.month('start_time').alias('month'),\
        F.year('start_time').alias('year'), \
        F.dayofweek('start_time').alias('weekday')]
    
    # extract columns to create time table
    time_table = songplays_table.select(*time_columns)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data+'udacity/time')

    
def main():
    """Creates spark session and process the data from s3 and writes back s3 in parquet format"""
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    print(config)

    os.environ['AWS_ACCESS_KEY_ID']=config.get('IAM','AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY']=config.get('IAM','AWS_SECRET_ACCESS_KEY')
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://subh-test/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
