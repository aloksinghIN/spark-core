from pyspark.sql import *
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType

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

    """ remember without infershcema , spark will give the schema for all 
    the cols as string but infer schema will give better guess like int,
    string etc.. 
    """
    flightTimeCsvDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferschema","true") \
        .option("mode", "FAILFAST") \
        .load("data2/flight*.csv")
    flightTimeCsvDF.show()
    # print schema
    print(flightTimeCsvDF.schema.simpleString())

    # Reading json

    flightTimeJSONDF = spark.read \
        .format("json") \
        .option("inferschema","true") \
        .option("mode", "FAILFAST") \
        .load("data2/flight*.json")
    flightTimeJSONDF.show()
    # print schema
    """ you see date is still treated as string """
    print(flightTimeJSONDF.schema.simpleString())

    flightTimeParDF = spark.read \
        .format("parquet") \
        .load("data2/flight*.parquet")
    flightTimeParDF.show()
    # print schema
    """ it gives the best schema  """
    print(flightTimeParDF.schema.simpleString())


    """ Now specfiy the schema manually 
    1- using Struct
    2- using ddl 
    """
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
             ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
             WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    flightTimeCsvDF2 = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data2/flight*.csv")

    flightTimeCsvDF2.show(5)
    print("Csv Schema using struct",flightTimeCsvDF.schema.simpleString())

    flightTimeJsonDF2 = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .option("dateFormat", "M/d/y") \
        .load("data2/flight*.json")

    flightTimeJsonDF2.show(5)
    print("JSON Schema using ddl:" + flightTimeJsonDF2.schema.simpleString())

    # input("Press enter key to break the program")
    print("Ended successfully")