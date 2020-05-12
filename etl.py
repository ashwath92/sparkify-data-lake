#import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType

#config = configparser.ConfigParser()
#config.read('dl.cfg')

# Not necessary while running directly on EMR.
#os.environ['AWS_ACCESS_KEY_ID']=config.get('IAM_USER', 'AWS_ACCESS_KEY_ID')
#os.environ['AWS_SECRET_ACCESS_KEY']=config.get('IAM_USER', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """ Creates a spark session, which it returns. config not necessary while running
    directly on EMR."""
    spark = SparkSession \
        .builder \
        .appName('Sparkify ETL') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def create_spark_context(spark):
    """ Creates a Spark context object in the current Spark Session and
    sets the AWS keys."""
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    #sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
    #sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])
    return sc

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*', '*.json')
    # read song data file
    
    song_schema = StructType([
        StructField('num_songs', IntegerType()),
        StructField('artist_id', StringType()),
        StructField('artist_latitude', DoubleType()),
        StructField('artist_longitude', DoubleType()),
        StructField('artist_location', StringType()),
        StructField('artist_name', StringType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('duration', DoubleType()),
        StructField('year', IntegerType())
    ])
    df = spark.read.json(song_data, schema=song_schema)
    # extract columns to create songs table
    df.createOrReplaceTempView('parquet_songs_db')
    songs_table = spark.sql('''
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM parquet_songs_db
    ''')
    print(songs_table.take(3))
    print(songs_table.count())
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet('{}songs/'.format(output_data))
    print('Wrote songs to parquet')

    # extract columns to create artists table.
    artist_columns = ['artist_id',
                  'artist_name as name',
                  'artist_location as location', 
                  'artist_latitude as latitude',
                  'artist_longitude as longitude']

    artists_table = df.selectExpr(artist_columns).dropDuplicates()
    
    # write artists table to parquet files
    #artists_table
    artists_table.write.parquet('{}artists/'.format(output_data))
    print('Wrote artists to parquet')

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*', '*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_columns = ['userId as user_id', 'firstName as first_name', 'lastName as last_name',
                     'gender', 'level']
    users_table = df.selectExpr(users_columns)

    # write users table to parquet files
    users_table.write.parquet('{}users/'.format(output_data))
    print("Wrote users to parquet")
    # create timestamp column from original timestamp column: divide by 1000 as it is epochtime in ms
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    # First get all unique timestamps
    time_table = df.select('start_time').dropDuplicates()
    time_table = time_table.select('start_time', hour('start_time').alias('hour'),\
                 dayofmonth('start_time').alias('day'), \
                 weekofyear('start_time').alias('week'),\
                 month('start_time').alias('month'),\
                 year('start_time').alias('year'),\
                 date_format('start_time', 'u').alias('weekday')
                 )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet('{}time/'.format(output_data))
    print("Wrote time table to parquet")

    # read in song data to use for songplays table
    songs_df = spark.read.parquet('{}songs/'.format(output_data))
    print("Read songs from parquet")
    artists_df = spark.read.parquet('{}artists/'.format(output_data))
    print("Read artists from parquet")
    songs_and_artists_df = songs_df.join(artists_df, songs_df.artist_id == artists_df.artist_id)\
                            .drop(songs_df.artist_id)
    print("Joined songs and artists")
    # Keep only columns we need in and for songplays:
    # song_id, title, artist_id, name
    songs_and_artists_df = songs_and_artists_df.select('song_id', 'title', 'artist_id', 
                                                  col('name').alias('artist_name'))
    songs_logs_df = df.join(songs_and_artists_df,
                            (df.artist==songs_and_artists_df.artist_name)\
                            & (df.song==songs_and_artists_df.title))
    print("Joined songs (and artists) and logs")

    # extract columns from joined song and log datasets to create songplays table 
    # songplay_id (generated), start_time (from logfile), user_id (from logfile),
    # level (from logfile), song_id (from songs), artist_id (from artists),
    # session_id (from logfile), location (from logfile), user_agent (from logfile)
    # Also add extra year and month columns
    songplays_table = songs_logs_df.select(monotonically_increasing_id().alias('songplay_id'),\
                                get_timestamp('ts').alias('start_time'),\
                                col('userId').alias('user_id'),\
                                'level', 'song_id', 'artist_id',\
                                col('sessionId').alias('session_id'),\
                                'location', col('userAgent').alias('user_agent'),\
                                month('start_time').alias('month'),year('start_time').alias('year') 
                               )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet('{}songplays/'.format(output_data))
    print("Wrote songplays to parquet")

def main():
    spark = create_spark_session()
    sc = create_spark_context(spark)
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://ashsparkifydatalake/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()

if __name__ == "__main__":
    main()
