from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

if __name__ == '__main__':
    """
    create spark session using builder patter in local mode.

    Error - if you get error to read the file, use os.getcwd
    and chang the working directory accordingly

    Spark UI - you can see the dag and other details on
    http://localhost:4040/jobs/

    """
    spark = SparkSession.builder \
        .master('local[2]') \
        .appName("Hell spark world") \
        .getOrCreate()

    flightTimeParDF = spark.read \
        .format("parquet") \
        .load("data2/flight*.parquet")
    print(" check number of partition before writing the data",
          flightTimeParDF.rdd.getNumPartitions())
    # check the number of records in each partition
    flightTimeParDF.groupBy(spark_partition_id()).count().show()

    # """ writing without partitioning """
    # flightTimeParDF.write.format("json").mode("overwrite") \
    #     .option("path", "output").save()

    """ now repartition and write"""
    # partdf = flightTimeParDF.repartition(5)
    # partdf.write.format("json").mode("overwrite") \
    #         .option("path", "output").save()

    """ now we got 5 json part file in output dir"""

    """ lets wirte it based on some column """
    flightTimeParDF.write.format("json").mode("overwrite") \
            .option("path", "output") \
        .partitionBy("op_carrier","origin") \
        .option("maxRecordsPerFile",10000)\
        .save()

    # input("Press enter key to break the program")
    print("Ended successfully")
