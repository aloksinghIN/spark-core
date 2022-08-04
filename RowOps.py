from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


def to_date_df(dataframe, format, field ):
    """
    :param dataframe:
    :type dataframe:
    :param format:
    :type format:
    :param field:
    :type field:
    :return: converts string date format to data format for a column
    :rtype:
    """
    return dataframe.withColumn(field, to_date(col(field),format))

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
        .appName("ROW ops") \
        .enableHiveSupport() \
        .getOrCreate()

    # make the schema first
    sample_schema = StructType([
        StructField("ID", StringType()),
        StructField("Event Date",StringType())])

    # make sample data
    sample_rows = [Row("11","01/06/2022"),Row("12","01/07/2018"),
                   Row("13","01/07/2021"),Row("14","04/08/2022")]

    # make dataframe using above schema and row

    my_rdd = spark.sparkContext.parallelize(sample_rows,2)
    my_df = my_rdd.toDF(sample_schema)

    # or another way to create df
    # my_df = spark.createDataFrame(my_rdd,sample_schema)

    my_df.printSchema()
    my_df.show()
    new_df = to_date_df(my_df,"M/d/y","Event Date")
    new_df.printSchema()
    new_df.show()
