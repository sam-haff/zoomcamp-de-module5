1. **Install Spark and PySpark**
   **Answer:** '3.5.4'

   Explanation:
    - Do the method <em>version</em> on the <em>SparkSession</em> object:
        ~~~
        sc = SparkSession.builder.master("local[*]").getOrCreate()
        print(sc.version())

        ~~~
2. **Yellow October 2024**
    **Answer:** 25 MB

    Explanation:
     - Create the dataframe for the input file by using <em>read.parquet</em> on session object:
        ~~~
        df = sc.read.parquet(input_file)
        ~~~
        Chain <em>repartition</em> transformation with <em>write.parquet</em> to get the partitions written to the specified folder:
        ~~~
        df.repartition(4).write.parquet('./data/pq/parted/2024/10')
        ~~~
        Then, given the folder with the output partitions is defined by ${PQ_OUTPUT}, do the followong commands in the terminal:
        ~~~
        ls ${PQ_OUTPUT} | grep '*parquet$'
        ~~~
        This will give the list of the output files. Then calc the average of the file sizes(they all are the same, so it'll be the value of any of them).
3. **Count records**
   **Answer:** 125,567

   Explanation: 
    - First we make some preparational transformation, to get the required calculation done in the neat way:
        ~~~
        df = df.withColumn('pickup_day', F.date_part(F.lit('D'), 'pickup_datetime'))
        ~~~
        The final transformation and subsequent action to get the required answer:
        ~~~
        result = df.filter(df.pickup_day == 15).count()
        ~~~
4. **Longest trip**
    **Answer:** 162

    Explanation:
    - First, we create a temp table to then use it to get the required task done using SQl query:
        ~~~
        df.createTempTable('yellow_trips_202410')
        ~~~
        The query:
        ~~~
        sc.sql(
            '''
            SELECT tpep_pickup_datetime, tpep_dropoff_datetime, timestampdiff(hour, tpep_pickup_datetime, tpep_dropoff_datetime) as hours
            FROM yellow_trips_202410
            ORDER BY 3 DESC
            LIMIT 1 
            '''
        ).show() 
        ~~~
5. **User Interface**
   **Answer:** 4040
6. **Least frequent pickup location zone**
   **Answer:** Governor's Island/Ellis Island/Liberty Island

   Explanation:
   - Because there could be zones with 0 trips(and they qualify for the answer to the question at most), the correct way of quering is to do zones LEFT JOIN  yellow_trips_202410.\
    First we prepare the zones <em>DataFrame</em> and register a table for it:
        ~~~
        df_zones = sc.read.option("header", "true").csv(zones_file)
        df_zones.registerTempTable('zones')
        ~~~
        Then we perform the actual query that will bring us the required results:
        ~~~
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
        ~~~