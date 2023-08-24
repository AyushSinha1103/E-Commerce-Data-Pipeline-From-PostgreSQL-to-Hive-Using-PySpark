from pyspark.sql import DataFrame, functions as F

# Data Enrichment

def categorize_age(customers_df: DataFrame) -> DataFrame:
    age_conditions = [
        (F.col("age").between(1, 15), "01-15"),
        (F.col("age").between(16, 25), "16-25"),
        (F.col("age").between(26, 35), "26-35"),
        (F.col("age").between(36, 45), "36-45"),
        (F.col("age").between(46, 55), "46-55"),
        (F.col("age").between(56, 65), "56-65"),
        (F.col("age").between(66, 75), "66-75"),
        (F.col("age").between(76, 85), "76-85"),
        (F.col("age").between(86, 95), "86-95")
        
    ]
    
    return customers_df.withColumn(
        "age_group", 
        F.coalesce(*[F.when(condition, result).otherwise(None) for condition, result in age_conditions])
    )

# Aggregation for Insights

def top_products_by_quantity_sold(products_df: DataFrame, sales_df: DataFrame) -> DataFrame:
    aggregated_sales = sales_df.groupBy("product_id").agg(F.sum("quantity").alias("total_quantity_sold"))
    return products_df.join(aggregated_sales, on="product_id", how="left").fillna(0, subset=["total_quantity_sold"]).orderBy(F.desc("total_quantity_sold"))

def customer_segmentation(customers_df: DataFrame, sales_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    # Join sales and orders to get customer_id in sales data
    sales_with_customer = sales_df.join(orders_df.select("order_id", "customer_id"), on="order_id", how="left")
    
    # Aggregate the joined data
    customer_sales = sales_with_customer.groupBy("customer_id").agg(
        F.count("sales_id").alias("purchase_frequency"),
        F.sum("total_price").alias("total_spent")
    )

    # Left join with customers to keep all customers even if they don't have sales records
    return customers_df.join(customer_sales, on="customer_id", how="left")


def monthly_sales(sales_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    # Join sales_df with orders_df to get the order_date for each sale
    sales_with_date = sales_df.join(orders_df.select("order_id", "order_date"), on="order_id", how="left")
    
    # Aggregate based on the month of order_date to compute the monthly sales
    return sales_with_date.groupBy(F.month("order_date").alias("month")).agg(F.sum("total_price").alias("monthly_sales"))

def quarterly_sales(sales_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    joined_df = sales_df.join(orders_df, on="order_id", how="left")
    return joined_df.groupBy(F.quarter("order_date").alias("quarter")).agg(F.sum("total_price").alias("quarterly_sales"))

def geographical_analysis_sales(sales_df: DataFrame, orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    sales_with_customer = sales_df.join(orders_df, "order_id").select("customer_id", "total_price")
    return sales_with_customer.join(customers_df, "customer_id", "left").groupBy("city", "state").agg(F.sum("total_price").alias("sales")).orderBy(F.desc("sales"))


def geographical_analysis_orders(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    orders_with_geo = orders_df.join(customers_df, "customer_id")
    return orders_with_geo.groupBy("city", "state").count().withColumnRenamed("count", "number_of_orders").orderBy(F.desc("number_of_orders"))

def order_to_delivery_duration(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    duration_calculation = (
        (F.unix_timestamp("delivery_date") - F.unix_timestamp("order_date")) / 86400
    )

    # Join with customers to get the 'city' column
    enriched_orders = orders_df.join(customers_df.select("customer_id", "city"), "customer_id", "left")
    
    return (enriched_orders
            .withColumn("delivery_duration", duration_calculation)
            .groupBy("city")
            .agg(F.avg("delivery_duration").alias("avg_delivery_duration")))


def repeat_customers(orders_df: DataFrame, threshold: int = 3) -> DataFrame:
    return orders_df.groupBy("customer_id").count().filter(F.col("count") > threshold).withColumnRenamed("count", "number_of_orders")
