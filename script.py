from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sc = SparkSession.builder.master("local[*]").getOrCreate()
print(sc.version())

input_file = './data/pq/2024/10/*'
df = sc.read.parquet(input_file)
# q2
df.repartition(4).write.parquet('./data/pq/parted/2024/10')

# q3
df = df\
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')\
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
df = df.withColumn('pickup_day', F.date_part(F.lit('D'), 'pickup_datetime'))
result = df.filter(df.pickup_day == 15).count()
print('Count: ' + str(result))

# q4 
df.registerTempTable('yellow_trips_202410')
sc.sql(
    '''
    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, timestampdiff(hour, tpep_pickup_datetime, tpep_dropoff_datetime) as hours
    FROM yellow_trips_202410
    ORDER BY 3 DESC
    LIMIT 2
    '''
).show() 

# q6
zones_file = "taxi_zone_lookup.csv"
df_zones = sc.read.option("header", "true").csv(zones_file)
df_zones.registerTempTable('zones')

sc.sql(
    '''
    SELECT z.LocationID, z.Zone, COUNT(t.trip_distance)
    FROM zones AS z 
    LEFT JOIN yellow_trips_202410 AS t
    ON z.LocationID=t.PULocationID
    GROUP BY 1,2
    ORDER BY 3
    LIMIT 5
    '''
).show()
