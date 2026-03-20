import os
import random
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType
    
def setup_01(spark): 
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS demo_01")
    spark.sql(f"USE SCHEMA demo_01")
    
    # Drop existing orders table if it exists
    spark.sql("DROP TABLE IF EXISTS orders")
    
    print("- Creating sample orders data...")
    
    # Define sample data
    regions = ['North America', 'Europe', 'Asia Pacific', 'South America', 'Middle East', 'Africa']
    
    # Generate sample orders data    
    sample_data = []
    start_date = datetime(2023, 1, 1)
    
    for i in range(1000):
        order_date = start_date + timedelta(days=random.randint(0, 730))  # 2 years of data
        customer_region = random.choice(regions)
        order_amount = round(random.uniform(50.0, 5000.0), 2)
        
        sample_data.append(Row(
            order_id=i+1,
            customer_region=customer_region,
            order_date=order_date.date(),
            order_amount=order_amount
        ))
    
    # Create DataFrame
    orders_df = spark.createDataFrame(sample_data)
    
    # Write to Delta table
    orders_df.write.format("delta").mode("overwrite").saveAsTable("orders")
    
    print(f"- Created orders table with {orders_df.count()} sample records")
    
def setup(spark):
    print("Creating catalog trainer_demo")
    
    spark.sql(f"CREATE CATALOG IF NOT EXISTS trainer_demo")
    spark.sql(f"USE CATALOG trainer_demo")
    
    setup_01(spark)
    
    print("Setup complete")
