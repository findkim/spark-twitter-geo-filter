'''
Kim Ngo
31 Jan 2016

Filters Twitter tweets by coordinate points of interest
Assumes hdfs filepath
Coordinates csv file format: location,latitude,longitude

spark-submit --driver-library-path /usr/lib/hadoop/lib/native \
--master yarn-cluster \
--num-executors 100 \
--executor-memory 4G \
--packages com.databricks:spark-csv_2.10:1.3.0 \
geo-filter.py 
'''

from pyspark import SparkContext
from pyspark.sql import SQLContext

### VARIABLES TO CHANGE ###
MILE_RADIUS = 10
COORDS_FILEPATH = '/user/bases-sample.csv'
TWEETS_FILEPATH = '/data/*.json.gz'
OUTPUT_FILEPATH = '/user/test-run'
### END ###

def miles_to_degrees(miles):
    return (1/69.172)*miles # mile per degree

if __name__ == '__main__':
    sc = SparkContext(appName='geo-filter')
    sqlContext = SQLContext(sc)

    # Calculates mile radius in degrees
    radius = miles_to_degrees(float(MILE_RADIUS))
    sc.broadcast(radius)

    # Read coordinates file
    raw_coords = sqlContext.read.format('com.databricks.spark.csv') \
        .option('header', 'true') \
        .option('inferschema', 'true') \
        .option('mode', 'DROPMALFORMED') \
        .load(COORDS_FILEPATH)
    coords = raw_coords.dropna().collect()
    sc.broadcast(coords)

    # Read tweets from datasource
    raw_tweets = sqlContext.read.json(TWEETS_FILEPATH).dropna(how='any', subset=['geo'])

    # Adds geo_lat and geo_lon column to DF that contains geo.coordinate
    # Hack because SqlContext doesn't explode on StructType
    tweets = raw_tweets.withColumn('geo_lon', raw_tweets.geo.getField('coordinates')[0]) \
        .withColumn('geo_lat', raw_tweets.geo.getField('coordinates')[1])
    tweets.persist()

    # Filter tweets based on coordinates and writes to output
    for coord in coords:
        #http://www.nodalpoint.com/unexpected-behavior-of-spark-dataframe-filter-method/
        filtered_tweets = tweets.filter(tweets.geo_lon > coord['longitude']-radius) \
            .filter(tweets.geo_lon < coord['longitude']+radius) \
            .filter(tweets.geo_lat > coord['latitude']-radius) \
            .filter(tweets.geo_lat < coord['latitude']+radius)
        if not filtered_tweets.rdd.isEmpty():
            filtered_tweets.drop('geo_lat').drop('geo_lon') \
                .write.mode('append').json(OUTPUT_FILEPATH)
        filtered_tweets.unpersist()

    sc.stop()
