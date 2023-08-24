from pyspark.sql import SparkSession

def create_session():
    spark = SparkSession.builder.appName("extract data from hive").enableHiveSupport().getOrCreate()
    return spark

def extract_table(spark, table_name):
    return spark.sql(f"select * from ecommercedb.{table_name}")