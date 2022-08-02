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
        .appName("Hell spark world")\
        .getOrCreate()

    # print("********",os.getcwd())
    source_df = spark.read.format('csv').option("header","true")\
        .load('./data/sample.csv')
    leave_df = source_df.select(['leave'])
    leave_df.show()
    ct_df = leave_df.groupBy('leave').count()
    ct_df.show()
    # input("Press enter key to break the program")
    print("Ended successfully")