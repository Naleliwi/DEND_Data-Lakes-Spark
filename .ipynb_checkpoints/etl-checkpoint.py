import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["AWS"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["AWS"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    This function is used to create or retreive spark session
    
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function is used to
    - Read songs dataset from s3 bucket (input_data)
    - Extract songs & artists tables
    - Write the generated tables into a parquet files then save it in the new s3 bucket (output_data)

    '''
    song_data = input_data+"song_data/*/*/*/*.json"
    
    df = spark.read.json(song_data)
    
    #create a view to use with SQL queries
    df.createOrReplaceTempView("VIW_SONGS")

    # SONG TABLE 
    songs_table = spark.sql("""
    
        SELECT DISTINCT song_id, title, artist_id, year,duration
        FROM VIW_SONGS 
        
        """)
    
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data+'song/') 

    # ARTIST TABLE
    artists_table = spark.sql("""
    
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM VIW_SONGS 
        
        """)
    
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
 
    '''
    This function is used to
    - Read logs dataset from s3 bucket (input_data)
    - Extract users, time & songplays tables
    - Write the generated tables into a parquet files then save it in the new s3 bucket (output_data)
 
    '''    

    log_data = input_data+'log-data/*.json'

    df = spark.read.json(log_data)
    
    df = df.filter(df.page == 'NextSong')

    #create a view to use with SQL queries
    df.createOrReplaceTempView("VIW_LOGS")
    
    # USER TABLE
    users_table = spark.sql("""
        
        SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM VIW_LOGS
        
        """)
    
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')
    

    # TIME TABLE
    time_table = spark.sql(""" 
    
        SELECT  DISTINCT temp.start_time, 
        hour( temp.start_time) as hour, 
        dayofmonth( temp.start_time) as day,
        weekofyear( temp.start_time) as week, 
        month( temp.start_time) as month, 
        year(temp.start_time) as year,
        dayofweek(temp.start_time) as weekday
        FROM (SELECT to_timestamp(ts/1000) as start_time
        FROM VIW_LOGS ) temp
                             
        """)
    
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time/')

    song_df = spark.read.parquet(output_data+'song/')


    # SONGPLAY TABLE
    
    songplays_table = spark.sql(""" 
    
        SELECT  DISTINCT monotonically_increasing_id() as songplay_id,
        to_timestamp(ts/1000) as start_time,
        year(start_time) as year,
        month(start_time) as month,
        userId, song_id, artist_id, sessionId, location, userAgent
        FROM VIW_LOGS 
        JOIN VIW_SONGS 
        ON  VIW_LOGS.song = VIW_SONGS.title
        AND VIW_LOGS.artist = VIW_SONGS.artist_name
        AND VIW_LOGS.length = VIW_SONGS.duration
                                 
        """)


    songplays_table.write.partitionBy("year","month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://sparkify3/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
