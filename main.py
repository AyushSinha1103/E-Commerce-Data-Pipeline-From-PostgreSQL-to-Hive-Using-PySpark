from extract import create_session, extract_table
import clean
import transform
import load

def main():
    spark = create_session()

    #Extracting data from hive
    ext_customers_df = extract_table(spark, "hive_customers")
    ext_products_df = extract_table(spark, "hive_products")
    ext_orders_df = extract_table(spark, "hive_orders")
    ext_sales_df = extract_table(spark, "hive_sales")

    #Cleaning data
    cleaned_customers_df = clean.clean_customers(ext_customers_df)
    cleaned_products_df = clean.clean_products(ext_products_df)
    cleaned_orders_df = clean.clean_orders(ext_orders_df)
    cleaned_sales_df = clean.clean_sales(ext_sales_df)

    #Transforming data
    # Age Groups
    customers_df_age_categorized = transform.categorize_age(cleaned_customers_df)
    customers_df_age_categorized.show()

    # Top Products based on Quantity Sold
    total_quantity_sold_df = transform.top_products_by_quantity_sold(cleaned_products_df, cleaned_sales_df)
    total_quantity_sold_df.show()

    # Customer Segmentation
    segmented_customers_df = transform.customer_segmentation(cleaned_customers_df, cleaned_sales_df, cleaned_orders_df)
    segmented_customers_df.show()

    # Monthly Sales
    monthly_sales_df = transform.monthly_sales(cleaned_sales_df, cleaned_orders_df)
    monthly_sales_df.show()

    # Quarterly Sales
    quarterly_sales_df = transform.quarterly_sales(cleaned_sales_df, cleaned_orders_df)
    quarterly_sales_df.show()

    # Geographical Analysis: Sales
    city_sales_df = transform.geographical_analysis_sales(cleaned_sales_df, cleaned_orders_df, cleaned_customers_df)
    city_sales_df.show()

    # Geographical Analysis: Orders
    city_orders_df = transform.geographical_analysis_orders(cleaned_orders_df, cleaned_customers_df)
    city_orders_df.show()

    # Order to Delivery Duration
    avg_delivery_duration_df = transform.order_to_delivery_duration(cleaned_orders_df, cleaned_customers_df)
    avg_delivery_duration_df.show()

    # Repeat Customers
    loyal_customers_df = transform.repeat_customers(cleaned_orders_df)
    loyal_customers_df.show()

    #Saving Data to hive 
    load.save_to_hive(customers_df_age_categorized, "customers_age_categorized")
    load.save_to_hive(total_quantity_sold_df, "total_quantity_sold")
    load.save_to_hive(segmented_customers_df, "customers_total_spent")
    load.save_to_hive(monthly_sales_df, "monthly_sales")
    load.save_to_hive(quarterly_sales_df, "quarterly_sales")
    load.save_to_hive(city_orders_df, "city_total_orders")
    load.save_to_hive(city_sales_df, "city_total_sales")
    load.save_to_hive(avg_delivery_duration_df, "city_avg_delivery_duration")
    load.save_to_hive(loyal_customers_df, "customers_order_gt3")



if __name__ == "__main__":
    main()