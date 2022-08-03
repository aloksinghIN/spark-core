from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .enableHiveSupport() \
        .getOrCreate()

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("data2/flight*.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # flightTimeParquetDF.write \
    #     .mode("overwrite") \
    #     .saveAsTable("flight_data_tbl")

    """ now use partitioned by and bucket by to store the data in better way
    Before bucket by you can use .partitionBy("op_carrier","origin") also"""

    flightTimeParquetDF.write.format("csv") \
        .mode("overwrite") \
        .bucketBy(5,"op_carrier","origin")\
        .sortBy("op_carrier","origin").saveAsTable("flight_tbl")

    print("list of tables--> ", spark.catalog.listTables("AIRLINE_DB"))
