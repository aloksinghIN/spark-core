from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, substring_index

if __name__ == "__main__":
    """
       create spark session using builder patter in local mode.

       Error - if you get error to read the file, use os.getcwd
       and chang the working directory accordingly

       Spark UI - you can see the dag and other details on
       http://localhost:4040/jobs/

       """

    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("ROW transformation") \
        .getOrCreate()
    file_df = spark.read.text("data/apache_logs.txt")
    file_df.printSchema()
    # regex to parse the file

    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    logs_df = file_df.select(regexp_extract('value', log_reg, 1).alias('ip'),
                             regexp_extract('value', log_reg, 4).alias('date'),
                             regexp_extract('value', log_reg, 6).alias('request'),
                             regexp_extract('value', log_reg, 10).alias('referrer'))
    logs_df.printSchema()
    logs_df.show(3,False)
    # do some tranformation

    logs_df.where("trim(referrer) != '-' " ) \
        .withColumn("referrer",substring_index("referrer","/",3)) \
        .groupBy("referrer").count().show(5,False)