from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Postgres to Hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Defining PostgreSQL properties
url = "jdbc:postgresql://localhost:5432/ecommerce"
properties = {
    "user": "postgres",
    "password": "admin@123",
    "driver": "org.postgresql.Driver"
}

#Reading Data from Postgres and storing to dataframe
customers_df = spark.read.jdbc(url=url, table="customers", properties=properties)
products_df = spark.read.jdbc(url=url, table="products", properties=properties)
orders_df = spark.read.jdbc(url=url, table="orders", properties=properties)
sales_df = spark.read.jdbc(url=url, table="sales", properties=properties)


#Writing Data to Hve Tables
customers_df.write.mode("overwrite").saveAsTable("ecommerceDB.hive_customers")
products_df.write.mode("overwrite").saveAsTable("ecommerceDB.hive_products")
orders_df.write.mode("overwrite").saveAsTable("ecommerceDB.hive_orders")
sales_df.write.mode("overwrite").saveAsTable("ecommerceDB.hive_sales")

spark.stop()