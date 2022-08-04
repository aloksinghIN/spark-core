from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, when, expr
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Misc Demo") \
        .master("local[2]") \
        .getOrCreate()


    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981
    dummy_schema = """Name string,day string,month string,year string"""
    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)

    final_df = raw_df.withColumn("id", monotonically_increasing_id()) \
        .withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("year", col("year").cast(IntegerType())) \
        .withColumn("year", when(col("year") < 20, col("year") + 2000)
                    .when(col("year") < 100, col("year") + 1900)
                    .otherwise(col("year"))) \
        .withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')")) \
        .drop("day", "month", "year") \
        .dropDuplicates(["name", "dob"]) \
        .sort(col("dob").desc())
    # .sort(expr("dob desc")) This doesn't seem to be working
    final_df.show()
