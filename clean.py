from pyspark.sql import DataFrame, functions as F

def clean_customers(df: DataFrame) -> DataFrame:
    # Drop rows where customer_id is missing
    df = df.dropna(subset=["customer_id"])

    # Remove duplicates based on customer_id
    df = df.dropDuplicates(["customer_id"])

    # Validate zip_code length and replace invalid ones with 9999
    df = df.withColumn("zip_code", F.when(F.length(df["zip_code"]) == 4, df["zip_code"]).otherwise("9999"))

    return df

def clean_products(df: DataFrame) -> DataFrame:
    # Drop rows where product_id is missing
    df = df.dropna(subset=["product_id"])

    # Remove duplicates based on product_id
    df = df.dropDuplicates(["product_id"])

    # Validate price and quantity, set to 1 if not >= 0
    df = df.withColumn("price", F.when(df["price"] > 0, df["price"]).otherwise(1))
    df = df.withColumn("quantity", F.when(df["quantity"] >= 0, df["quantity"]).otherwise(0))

    return df

def clean_orders(df: DataFrame) -> DataFrame:
    # Drop rows where either customer_id or order_id is missing
    df = df.dropna(subset=["customer_id", "order_id"])

    # Remove duplicates based on order_id
    df = df.dropDuplicates(["order_id"])

    # Ensure delivery_date is not before order_date, if so, set delivery_date = order_date
    df = df.withColumn("delivery_date", F.when(df["delivery_date"] < df["order_date"], df["order_date"]).otherwise(df["delivery_date"]))

    return df

def clean_sales(df: DataFrame) -> DataFrame:
    # Drop rows where sales_id, product_id or order_id are missing
    df = df.dropna(subset=["sales_id", "product_id", "order_id"])

    # Remove duplicates based on sales_id
    df = df.dropDuplicates(["sales_id"])

    df = df.withColumn("total_price", 
                       F.when(df["price_per_unit"] * df["quantity"] != df["total_price"], 
                              df["price_per_unit"] * df["quantity"]).otherwise(df["total_price"]))


    return df
