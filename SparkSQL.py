import sys, os
from pyspark.sql import *

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

    # print("********",os.getcwd())
    source_df = spark.read.format('csv').option("header", "true") \
        .option("inferschema","true") \
        .load('./data/sample.csv')
    source_df.show()
    source_df.createOrReplaceTempView("survey")
    count_df = spark.sql("select country,count(*) as country_count from survey group by Country")
    count_df.show()
    # input("Press enter key to break the program")
    print("Ended successfully")