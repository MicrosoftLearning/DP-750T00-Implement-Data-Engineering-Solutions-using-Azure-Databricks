import os
import random
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType
    
def setup_01(spark): 
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS demo_01")
    spark.sql(f"USE SCHEMA demo_01")
    
    # Create volume for landing data
    spark.sql("CREATE VOLUME IF NOT EXISTS landing")
    print("- Created landing volume")
    
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

def setup_02(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS demo_02")
    spark.sql(f"USE SCHEMA demo_02")
    
    print("- Creating healthcare sample data...")
    
    # Create volume for shared libraries
    spark.sql("CREATE VOLUME IF NOT EXISTS shared_libraries")
    print("  ✓ Created shared_libraries volume")
    
    # Drop existing tables if they exist
    spark.sql("DROP TABLE IF EXISTS patient_visits")
    spark.sql("DROP TABLE IF EXISTS lab_results")
    spark.sql("DROP TABLE IF EXISTS medical_devices")
    
    # 1. Patient Visits Table
    print("- Generating patient visits data...")
    departments = ['Emergency', 'Cardiology', 'Neurology', 'Orthopedics', 'Pediatrics', 'Oncology', 'Internal Medicine']
    diagnosis_codes = ['I10', 'E11.9', 'J44.9', 'M25.511', 'F41.1', 'R50.9', 'K21.9', 'N18.9']
    
    visit_data = []
    start_date = datetime(2023, 1, 1)
    
    for i in range(5000):
        visit_date = start_date + timedelta(days=random.randint(0, 730))
        patient_id = random.randint(1000, 9999)
        department = random.choice(departments)
        diagnosis = random.choice(diagnosis_codes)
        duration = random.randint(15, 480)  # 15 mins to 8 hours
        
        visit_data.append(Row(
            visit_id=i+1,
            patient_id=patient_id,
            visit_date=visit_date.date(),
            department=department,
            diagnosis_code=diagnosis,
            treatment_duration_mins=duration
        ))
    
    visits_df = spark.createDataFrame(visit_data)
    visits_df.write.format("delta").mode("overwrite").saveAsTable("patient_visits")
    print(f"  ✓ Created patient_visits table with {visits_df.count():,} records")
    
    # 2. Lab Results Table
    print("- Generating lab results data...")
    test_types = ['Blood Glucose', 'Cholesterol', 'Hemoglobin A1C', 'WBC Count', 'Platelet Count', 
                  'Creatinine', 'ALT', 'AST', 'TSH', 'Vitamin D']
    
    # Reference ranges for each test type (min, max)
    reference_ranges = {
        'Blood Glucose': (70, 100),
        'Cholesterol': (125, 200),
        'Hemoglobin A1C': (4.0, 5.6),
        'WBC Count': (4.5, 11.0),
        'Platelet Count': (150, 400),
        'Creatinine': (0.7, 1.3),
        'ALT': (7, 56),
        'AST': (10, 40),
        'TSH': (0.4, 4.0),
        'Vitamin D': (30, 100)
    }
    
    lab_data = []
    
    for i in range(15000):
        test_type = random.choice(test_types)
        min_val, max_val = reference_ranges[test_type]
        
        # Generate result values (some within normal range, some outside)
        if random.random() < 0.7:  # 70% within normal range
            result_value = round(random.uniform(min_val, max_val), 2)
        else:  # 30% outside normal range
            if random.random() < 0.5:
                result_value = round(random.uniform(min_val * 0.5, min_val), 2)  # Low
            else:
                result_value = round(random.uniform(max_val, max_val * 1.5), 2)  # High
        
        # Categorize result
        if result_value < min_val:
            category = "Low"
        elif result_value > max_val:
            category = "High"
        else:
            category = "Normal"
        
        test_date = start_date + timedelta(days=random.randint(0, 730))
        patient_id = random.randint(1000, 9999)
        
        lab_data.append(Row(
            test_id=i+1,
            patient_id=patient_id,
            test_date=test_date.date(),
            test_type=test_type,
            result_value=result_value,
            normal_min=min_val,
            normal_max=max_val,
            result_category=category
        ))
    
    labs_df = spark.createDataFrame(lab_data)
    labs_df.write.format("delta").mode("overwrite").saveAsTable("lab_results")
    print(f"  ✓ Created lab_results table with {labs_df.count():,} records")
    
    # 3. Medical Devices Table
    print("- Generating medical devices data...")
    device_types = ['Ventilator', 'Infusion Pump', 'Patient Monitor', 'Defibrillator', 
                    'X-Ray Machine', 'MRI Scanner', 'CT Scanner', 'Ultrasound']
    locations = ['ICU-1', 'ICU-2', 'ER-1', 'ER-2', 'OR-1', 'OR-2', 'OR-3', 'Radiology', 'Cardiology']
    statuses = ['operational', 'operational', 'operational', 'maintenance', 'offline']
    
    device_data = []
    
    for i in range(200):
        device_type = random.choice(device_types)
        location = random.choice(locations)
        status = random.choice(statuses)
        last_maintenance = start_date + timedelta(days=random.randint(-90, 0))
        requires_maintenance = (datetime.now() - last_maintenance).days > 60
        
        device_data.append(Row(
            device_id=f"DEV-{i+1:04d}",
            device_type=device_type,
            location=location,
            status=status,
            last_maintenance_date=last_maintenance.date(),
            requires_maintenance=requires_maintenance
        ))
    
    devices_df = spark.createDataFrame(device_data)
    devices_df.write.format("delta").mode("overwrite").saveAsTable("medical_devices")
    print(f"  ✓ Created medical_devices table with {devices_df.count():,} records")
    
    print("\n✓ Healthcare data setup complete!")
    print(f"  Schema: trainer_demo.demo_02")
    print(f"  Tables: patient_visits, lab_results, medical_devices")
    print(f"  Volume: shared_libraries")
    
def setup(spark):
    print("Creating catalog trainer_demo")
    
    spark.sql(f"CREATE CATALOG IF NOT EXISTS trainer_demo")
    spark.sql(f"USE CATALOG trainer_demo")
    
    setup_01(spark)
    setup_02(spark)
    
    print("Setup complete")
