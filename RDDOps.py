from pyspark.sql import *


def printRDD(rdd):
    """printing content of rdd """
    lis = (partitioned_rdd.collect())
    print("list length", len(lis))
    for col in lis:
        print(col)
        print('\n')


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
    sc = spark.sparkContext
    source_rdd = sc.textFile('./data/sparkdoc.text')
    partitioned_rdd = source_rdd.repartition(2)
    # printRDD(partitioned_rdd)

    # getting partition count
    # print("source_rdd partiton count", source_rdd.getNumPartitions())
    # print("partitioned_rdd partiton count", partitioned_rdd.getNumPartitions())


    # input("Press enter key to break the program")
    print("Ended successfully")