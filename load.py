from pyspark.sql import DataFrame


def save_to_hive(transformed_df: DataFrame, table_name: str):
    #Writing Data to Hve Tables
    transformed_df.write.mode("overwrite").saveAsTable(f"ecommerceDB.{table_name}")




