from pyspark.sql import *
from pyspark.sql.functions import *
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
        .appName("Column ops ex") \
        .getOrCreate()

    """ remember without infershcema , spark will give the schema for all 
    the cols as string but infer schema will give better guess like int,
    string etc.. 
    """
    flightTimeCsvDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferschema","true") \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y")\
        .load("data2/flight*.csv")
    flightTimeCsvDF.show()
    # print schema
    print(flightTimeCsvDF.schema.simpleString())

    # select col using col string

    flightTimeCsvDF.select("origin","dest").show(2)

    #select using col object
    flightTimeCsvDF.select(col("dest"),col("origin")).show(3)

    # select using mix of col string and col object

    flightTimeCsvDF.select(col("dest"), col("origin"), "distance").show(4)

    # col and column in single select will throw an error in new version

    # applying functions/expr on the col using string expr method

    flightTimeCsvDF.select("origin","dest","distance",
                           expr("concat(origin,dest) as org_dest") ).show(5)

    # transform using col expression

    flightTimeCsvDF.select("origin", "dest", "distance",
                           concat("origin","dest").alias("org_dest_using_col")).show(5)