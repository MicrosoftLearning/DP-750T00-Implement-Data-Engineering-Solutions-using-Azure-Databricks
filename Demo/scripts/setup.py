import os
import random
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType, DoubleType, BooleanType
    
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
    
    # Define schema for orders
    orders_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_region", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("order_amount", DoubleType(), True)
    ])
    
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
    
    # Create DataFrame with explicit schema
    orders_df = spark.createDataFrame(sample_data, schema=orders_schema)
    
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
    
    # Define schema for patient visits
    visits_schema = StructType([
        StructField("visit_id", IntegerType(), False),
        StructField("patient_id", IntegerType(), True),
        StructField("visit_date", DateType(), True),
        StructField("department", StringType(), True),
        StructField("diagnosis_code", StringType(), True),
        StructField("treatment_duration_mins", IntegerType(), True)
    ])
    
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
    
    visits_df = spark.createDataFrame(visit_data, schema=visits_schema)
    visits_df.write.format("delta").mode("overwrite").saveAsTable("patient_visits")
    print(f"  ✓ Created patient_visits table with {visits_df.count():,} records")
    
    # 2. Lab Results Table
    print("- Generating lab results data...")
    
    # Define schema for lab results
    labs_schema = StructType([
        StructField("test_id", IntegerType(), False),
        StructField("patient_id", IntegerType(), True),
        StructField("test_date", DateType(), True),
        StructField("test_type", StringType(), True),
        StructField("result_value", DoubleType(), True),
        StructField("normal_min", DoubleType(), True),
        StructField("normal_max", DoubleType(), True),
        StructField("result_category", StringType(), True)
    ])
    
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
            normal_min=float(min_val),
            normal_max=float(max_val),
            result_category=category
        ))
    
    labs_df = spark.createDataFrame(lab_data, schema=labs_schema)
    labs_df.write.format("delta").mode("overwrite").saveAsTable("lab_results")
    print(f"  ✓ Created lab_results table with {labs_df.count():,} records")
    
    # 3. Medical Devices Table
    print("- Generating medical devices data...")
    
    # Define schema for medical devices
    devices_schema = StructType([
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("last_maintenance_date", DateType(), True),
        StructField("requires_maintenance", BooleanType(), True)
    ])
    
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
    
    devices_df = spark.createDataFrame(device_data, schema=devices_schema)
    devices_df.write.format("delta").mode("overwrite").saveAsTable("medical_devices")
    print(f"  ✓ Created medical_devices table with {devices_df.count():,} records")
    
    print("\n✓ Healthcare data setup complete!")
    print(f"  Schema: trainer_demo.demo_02")
    print(f"  Tables: patient_visits, lab_results, medical_devices")
    print(f"  Volume: shared_libraries")

def setup_03(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS demo_03")
    spark.sql(f"USE SCHEMA demo_03")
    
    print("- Creating education sample data...")
    
    # Create volume for course materials
    spark.sql("CREATE VOLUME IF NOT EXISTS course_materials")
    print("  ✓ Created course_materials volume")
    
    # Drop existing tables if they exist
    spark.sql("DROP TABLE IF EXISTS students")
    spark.sql("DROP TABLE IF EXISTS courses")
    spark.sql("DROP TABLE IF EXISTS enrollments")
    spark.sql("DROP TABLE IF EXISTS assessments")
    
    # 1. Students Table
    print("- Generating students data...")
    
    students_schema = StructType([
        StructField("student_id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("enrollment_date", DateType(), True),
        StructField("program", StringType(), True),
        StructField("year_level", IntegerType(), True),
        StructField("gpa", DoubleType(), True)
    ])
    
    first_names = ['Emma', 'Liam', 'Olivia', 'Noah', 'Ava', 'Ethan', 'Sophia', 'Mason', 'Isabella', 'William',
                   'Mia', 'James', 'Charlotte', 'Benjamin', 'Amelia', 'Lucas', 'Harper', 'Henry', 'Evelyn', 'Alexander']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
                  'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin']
    programs = ['Computer Science', 'Data Engineering', 'Business Analytics', 'Information Systems', 'Software Engineering']
    
    student_data = []
    start_date = datetime(2021, 9, 1)
    
    for i in range(500):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        email = f"{first_name.lower()}.{last_name.lower()}{i}@university.edu"
        enrollment_date = start_date + timedelta(days=random.randint(0, 1095))  # 3 years of enrollment dates
        program = random.choice(programs)
        year_level = random.randint(1, 4)
        gpa = round(random.uniform(2.0, 4.0), 2)
        
        student_data.append(Row(
            student_id=i+1000,
            first_name=first_name,
            last_name=last_name,
            email=email,
            enrollment_date=enrollment_date.date(),
            program=program,
            year_level=year_level,
            gpa=gpa
        ))
    
    students_df = spark.createDataFrame(student_data, schema=students_schema)
    students_df.write.format("delta").mode("overwrite").saveAsTable("students")
    print(f"  ✓ Created students table with {students_df.count():,} records")
    
    # 2. Courses Table
    print("- Generating courses data...")
    
    courses_schema = StructType([
        StructField("course_id", StringType(), False),
        StructField("course_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("credits", IntegerType(), True),
        StructField("level", StringType(), True),
        StructField("instructor", StringType(), True)
    ])
    
    course_list = [
        ('CS101', 'Introduction to Programming', 'Computer Science', 3, 'Undergraduate', 'Dr. Sarah Mitchell'),
        ('CS201', 'Data Structures', 'Computer Science', 4, 'Undergraduate', 'Prof. James Chen'),
        ('CS301', 'Database Systems', 'Computer Science', 3, 'Undergraduate', 'Dr. Michael Torres'),
        ('CS401', 'Machine Learning', 'Computer Science', 4, 'Graduate', 'Prof. Lisa Anderson'),
        ('DE101', 'Introduction to Data Engineering', 'Data Engineering', 3, 'Undergraduate', 'Dr. Robert Kim'),
        ('DE201', 'ETL and Data Pipelines', 'Data Engineering', 4, 'Undergraduate', 'Prof. Maria Garcia'),
        ('DE301', 'Big Data Processing', 'Data Engineering', 4, 'Graduate', 'Dr. David Lee'),
        ('DE401', 'Advanced Data Architecture', 'Data Engineering', 3, 'Graduate', 'Prof. Jennifer Martinez'),
        ('BA101', 'Business Analytics Fundamentals', 'Business Analytics', 3, 'Undergraduate', 'Dr. Emily Brown'),
        ('BA201', 'Statistical Analysis', 'Business Analytics', 4, 'Undergraduate', 'Prof. Thomas Wilson'),
        ('BA301', 'Predictive Analytics', 'Business Analytics', 3, 'Graduate', 'Dr. Amanda Taylor'),
        ('IS101', 'Information Systems', 'Information Systems', 3, 'Undergraduate', 'Prof. Christopher Davis'),
        ('IS201', 'Systems Analysis and Design', 'Information Systems', 4, 'Undergraduate', 'Dr. Jessica Moore'),
        ('SE101', 'Software Development', 'Software Engineering', 4, 'Undergraduate', 'Prof. Daniel Jackson'),
        ('SE201', 'Software Testing', 'Software Engineering', 3, 'Undergraduate', 'Dr. Nicole White')
    ]
    
    course_data = [Row(
        course_id=c[0],
        course_name=c[1],
        department=c[2],
        credits=c[3],
        level=c[4],
        instructor=c[5]
    ) for c in course_list]
    
    courses_df = spark.createDataFrame(course_data, schema=courses_schema)
    courses_df.write.format("delta").mode("overwrite").saveAsTable("courses")
    print(f"  ✓ Created courses table with {courses_df.count():,} records")
    
    # 3. Enrollments Table
    print("- Generating enrollments data...")
    
    enrollments_schema = StructType([
        StructField("enrollment_id", IntegerType(), False),
        StructField("student_id", IntegerType(), True),
        StructField("course_id", StringType(), True),
        StructField("semester", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("enrollment_status", StringType(), True)
    ])
    
    semesters = ['Fall', 'Spring', 'Summer']
    statuses = ['Enrolled', 'Enrolled', 'Enrolled', 'Completed', 'Completed', 'Completed', 'Withdrawn', 'In Progress']
    years = [2022, 2023, 2024]
    
    enrollment_data = []
    enrollment_id = 1
    
    # Generate enrollments for students
    for student in student_data[:300]:  # Not all students have enrollments
        num_enrollments = random.randint(3, 8)
        enrolled_courses = random.sample([c[0] for c in course_list], num_enrollments)
        
        for course_id in enrolled_courses:
            enrollment_data.append(Row(
                enrollment_id=enrollment_id,
                student_id=student.student_id,
                course_id=course_id,
                semester=random.choice(semesters),
                year=random.choice(years),
                enrollment_status=random.choice(statuses)
            ))
            enrollment_id += 1
    
    enrollments_df = spark.createDataFrame(enrollment_data, schema=enrollments_schema)
    enrollments_df.write.format("delta").mode("overwrite").saveAsTable("enrollments")
    print(f"  ✓ Created enrollments table with {enrollments_df.count():,} records")
    
    # 4. Assessments Table
    print("- Generating assessments data...")
    
    assessments_schema = StructType([
        StructField("assessment_id", IntegerType(), False),
        StructField("enrollment_id", IntegerType(), True),
        StructField("assessment_type", StringType(), True),
        StructField("assessment_date", DateType(), True),
        StructField("score", DoubleType(), True),
        StructField("max_score", IntegerType(), True),
        StructField("percentage", DoubleType(), True)
    ])
    
    assessment_types = ['Midterm Exam', 'Final Exam', 'Quiz', 'Project', 'Assignment', 'Lab Work']
    
    assessment_data = []
    assessment_id = 1
    
    # Generate assessments for completed enrollments
    for enrollment in enrollment_data:
        if enrollment.enrollment_status in ['Completed', 'In Progress']:
            num_assessments = random.randint(3, 6)
            
            for _ in range(num_assessments):
                assessment_type = random.choice(assessment_types)
                max_score = 100 if assessment_type in ['Midterm Exam', 'Final Exam', 'Project'] else random.choice([20, 50, 100])
                score = round(random.uniform(max_score * 0.5, max_score), 1)
                percentage = round((score / max_score) * 100, 2)
                assessment_date = datetime(enrollment.year, random.randint(1, 12), random.randint(1, 28))
                
                assessment_data.append(Row(
                    assessment_id=assessment_id,
                    enrollment_id=enrollment.enrollment_id,
                    assessment_type=assessment_type,
                    assessment_date=assessment_date.date(),
                    score=score,
                    max_score=max_score,
                    percentage=percentage
                ))
                assessment_id += 1
    
    assessments_df = spark.createDataFrame(assessment_data, schema=assessments_schema)
    assessments_df.write.format("delta").mode("overwrite").saveAsTable("assessments")
    print(f"  ✓ Created assessments table with {assessments_df.count():,} records")
    
    print("\n✓ Education data setup complete!")
    print(f"  Schema: trainer_demo.demo_03")
    print(f"  Tables: students, courses, enrollments, assessments")
    print(f"  Volume: course_materials")
    
def setup_04(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS demo_04")
    spark.sql(f"USE SCHEMA demo_04")

    print("- Creating retail sample data...")

    # Drop existing tables if they exist
    spark.sql("DROP TABLE IF EXISTS customers")
    spark.sql("DROP TABLE IF EXISTS products")
    spark.sql("DROP TABLE IF EXISTS stores")
    spark.sql("DROP TABLE IF EXISTS transactions")

    # 1. Customers Table (includes PII columns for masking demos)
    print("- Generating customers data...")

    customers_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("region", StringType(), True),
        StructField("customer_segment", StringType(), True)
    ])

    first_names = ['Emma', 'Liam', 'Olivia', 'Noah', 'Ava', 'Ethan', 'Sophia', 'Mason', 'Isabella', 'William',
                   'Mia', 'James', 'Charlotte', 'Benjamin', 'Amelia', 'Lucas', 'Harper', 'Henry', 'Evelyn', 'Alexander']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
    regions = ['North', 'South', 'East', 'West']
    segments = ['Bronze', 'Silver', 'Gold', 'Platinum']
    segment_weights = [0.4, 0.35, 0.2, 0.05]

    customer_data = []
    for i in range(500):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        region = random.choice(regions)
        segment = random.choices(segments, weights=segment_weights)[0]
        phone = f"{random.randint(200,999)}-{random.randint(200,999)}-{random.randint(1000,9999)}"
        email = f"{first_name.lower()}.{last_name.lower()}{i}@email.com"

        customer_data.append(Row(
            customer_id=i+1,
            first_name=first_name,
            last_name=last_name,
            email=email,
            phone=phone,
            region=region,
            customer_segment=segment
        ))

    customers_df = spark.createDataFrame(customer_data, schema=customers_schema)
    customers_df.write.format("delta").mode("overwrite").saveAsTable("customers")
    print(f"  ✓ Created customers table with {customers_df.count():,} records")

    # 2. Products Table
    print("- Generating products data...")

    products_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("cost", DoubleType(), True)
    ])

    product_list = [
        ("PROD-001", "Wireless Headphones", "Electronics", 89.99, 35.00),
        ("PROD-002", "Running Shoes", "Sports", 129.99, 52.00),
        ("PROD-003", "Yoga Mat", "Sports", 34.99, 12.00),
        ("PROD-004", "Coffee Maker", "Home", 79.99, 30.00),
        ("PROD-005", "Winter Jacket", "Clothing", 149.99, 60.00),
        ("PROD-006", "Moisturizer SPF 50", "Beauty", 24.99, 8.00),
        ("PROD-007", "Bluetooth Speaker", "Electronics", 59.99, 22.00),
        ("PROD-008", "Denim Jeans", "Clothing", 54.99, 20.00),
        ("PROD-009", "Protein Powder", "Food", 44.99, 18.00),
        ("PROD-010", "Smart Watch", "Electronics", 249.99, 100.00),
        ("PROD-011", "Cookware Set", "Home", 119.99, 45.00),
        ("PROD-012", "T-Shirt Pack", "Clothing", 29.99, 10.00),
        ("PROD-013", "Hiking Boots", "Sports", 179.99, 72.00),
        ("PROD-014", "Face Serum", "Beauty", 39.99, 14.00),
        ("PROD-015", "Organic Tea Set", "Food", 19.99, 7.00),
        ("PROD-016", "Laptop Bag", "Electronics", 69.99, 26.00),
        ("PROD-017", "Throw Blanket", "Home", 39.99, 15.00),
        ("PROD-018", "Sports Socks 6-Pack", "Sports", 14.99, 5.00),
        ("PROD-019", "Hair Dryer", "Beauty", 49.99, 18.00),
        ("PROD-020", "Granola Bars 12-Pack", "Food", 12.99, 4.50)
    ]

    product_data = [Row(
        product_id=p[0],
        product_name=p[1],
        category=p[2],
        price=p[3],
        cost=p[4]
    ) for p in product_list]

    products_df = spark.createDataFrame(product_data, schema=products_schema)
    products_df.write.format("delta").mode("overwrite").saveAsTable("products")
    print(f"  ✓ Created products table with {products_df.count():,} records")

    # 3. Stores Table
    print("- Generating stores data...")

    stores_schema = StructType([
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("region", StringType(), True)
    ])

    store_list = [
        ("STORE-001", "RetailNow Chicago", "Chicago", "IL", "North"),
        ("STORE-002", "RetailNow Minneapolis", "Minneapolis", "MN", "North"),
        ("STORE-003", "RetailNow Detroit", "Detroit", "MI", "North"),
        ("STORE-004", "RetailNow Milwaukee", "Milwaukee", "WI", "North"),
        ("STORE-005", "RetailNow Houston", "Houston", "TX", "South"),
        ("STORE-006", "RetailNow Atlanta", "Atlanta", "GA", "South"),
        ("STORE-007", "RetailNow Miami", "Miami", "FL", "South"),
        ("STORE-008", "RetailNow Nashville", "Nashville", "TN", "South"),
        ("STORE-009", "RetailNow New York", "New York", "NY", "East"),
        ("STORE-010", "RetailNow Boston", "Boston", "MA", "East"),
        ("STORE-011", "RetailNow Philadelphia", "Philadelphia", "PA", "East"),
        ("STORE-012", "RetailNow Washington DC", "Washington", "DC", "East"),
        ("STORE-013", "RetailNow Los Angeles", "Los Angeles", "CA", "West"),
        ("STORE-014", "RetailNow Seattle", "Seattle", "WA", "West"),
        ("STORE-015", "RetailNow Phoenix", "Phoenix", "AZ", "West"),
        ("STORE-016", "RetailNow Denver", "Denver", "CO", "West")
    ]

    store_data = [Row(
        store_id=s[0],
        store_name=s[1],
        city=s[2],
        state=s[3],
        region=s[4]
    ) for s in store_list]

    stores_df = spark.createDataFrame(store_data, schema=stores_schema)
    stores_df.write.format("delta").mode("overwrite").saveAsTable("stores")
    print(f"  ✓ Created stores table with {stores_df.count():,} records")

    # 4. Transactions Table
    print("- Generating transactions data...")

    transactions_schema = StructType([
        StructField("transaction_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("store_region", StringType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("amount", DoubleType(), True)
    ])

    store_ids_by_region = {
        "North": ["STORE-001", "STORE-002", "STORE-003", "STORE-004"],
        "South": ["STORE-005", "STORE-006", "STORE-007", "STORE-008"],
        "East":  ["STORE-009", "STORE-010", "STORE-011", "STORE-012"],
        "West":  ["STORE-013", "STORE-014", "STORE-015", "STORE-016"]
    }
    store_region_map = {s[0]: s[4] for s in store_list}
    product_price_map = {p[0]: p[3] for p in product_list}
    product_ids = [p[0] for p in product_list]

    start_date = datetime(2023, 1, 1)
    transaction_data = []

    for i in range(5000):
        customer = customer_data[random.randint(0, len(customer_data) - 1)]
        store_id = random.choice(store_ids_by_region[customer.region])
        store_region = store_region_map[store_id]
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        amount = round(product_price_map[product_id] * quantity * random.uniform(0.85, 1.0), 2)
        transaction_date = start_date + timedelta(days=random.randint(0, 730))

        transaction_data.append(Row(
            transaction_id=i + 1,
            customer_id=customer.customer_id,
            product_id=product_id,
            store_id=store_id,
            store_region=store_region,
            transaction_date=transaction_date.date(),
            quantity=quantity,
            amount=amount
        ))

    transactions_df = spark.createDataFrame(transaction_data, schema=transactions_schema)
    transactions_df.write.format("delta").mode("overwrite").saveAsTable("transactions")
    print(f"  ✓ Created transactions table with {transactions_df.count():,} records")

    print("\n✓ Retail data setup complete!")
    print(f"  Schema: trainer_demo.demo_04")
    print(f"  Tables: customers, products, stores, transactions")


def setup_05(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS demo_05")
    spark.sql(f"USE SCHEMA demo_05")

    print("- Creating automotive sample data...")

    # Drop existing tables if they exist
    spark.sql("DROP TABLE IF EXISTS vehicles")
    spark.sql("DROP TABLE IF EXISTS customers")
    spark.sql("DROP TABLE IF EXISTS telemetry_events")
    spark.sql("DROP TABLE IF EXISTS service_records")

    import random

    # 1. Vehicles Table
    print("- Generating vehicles data...")

    vehicles_schema = StructType([
        StructField("vehicle_id", IntegerType(), False),
        StructField("vin", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("region", StringType(), True),
        StructField("color", StringType(), True)
    ])

    makes_models = [
        ("AutoNova", "Sedan X1", "Sedan"),
        ("AutoNova", "SUV Q5", "SUV"),
        ("AutoNova", "Truck T3", "Truck"),
        ("AutoNova", "EV Pulse", "Electric"),
        ("AutoNova", "Hatchback Z2", "Hatchback"),
        ("AutoNova", "Coupe R8", "Coupe"),
    ]
    regions = ["North America", "Europe", "Asia Pacific", "South America"]
    colors = ["White", "Black", "Silver", "Blue", "Red", "Gray", "Green"]
    years = list(range(2018, 2026))

    vehicle_data = []
    for i in range(300):
        make, model, vtype = random.choice(makes_models)
        vehicle_data.append(Row(
            vehicle_id=i + 1,
            vin=f"1AUTONOVA{str(i + 1).zfill(8)}",
            make=make,
            model=model,
            year=random.choice(years),
            vehicle_type=vtype,
            region=random.choice(regions),
            color=random.choice(colors)
        ))

    vehicles_df = spark.createDataFrame(vehicle_data, schema=vehicles_schema)
    vehicles_df.write.format("delta").mode("overwrite").saveAsTable("vehicles")
    print(f"  ✓ Created vehicles table with {vehicles_df.count():,} records")

    # 2. Customers Table (includes PII columns for governance demos)
    print("- Generating customers data...")

    customers_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("region", StringType(), True),
        StructField("customer_since_date", DateType(), True),
        StructField("vehicle_id", IntegerType(), True)
    ])

    first_names = ["Luca", "Emma", "Omar", "Yuki", "Carlos", "Sophie", "Arjun", "Mei",
                   "James", "Fatima", "Henrik", "Amara", "Diego", "Sakura", "Ivan"]
    last_names = ["Rossi", "Müller", "Al-Rashid", "Tanaka", "García", "Dubois",
                  "Sharma", "Chen", "Smith", "Ahmed", "Andersen", "Okonkwo",
                  "Lopez", "Yamamoto", "Petrov"]
    customer_regions = ["North America", "Europe", "Asia Pacific", "South America"]
    start_date = datetime(2015, 1, 1)

    customer_data = []
    for i in range(300):
        first = random.choice(first_names)
        last = random.choice(last_names)
        region = random.choice(customer_regions)
        since_days = random.randint(0, 3000)
        customer_data.append(Row(
            customer_id=i + 1,
            first_name=first,
            last_name=last,
            email=f"{first.lower()}.{last.lower()}{i}@autonova-demo.com",
            phone=f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            region=region,
            customer_since_date=(start_date + timedelta(days=since_days)).date(),
            vehicle_id=random.randint(1, 300)
        ))

    customers_df = spark.createDataFrame(customer_data, schema=customers_schema)
    customers_df.write.format("delta").mode("overwrite").saveAsTable("customers")
    print(f"  ✓ Created customers table with {customers_df.count():,} records")

    # 3. Telemetry Events Table
    print("- Generating telemetry events data...")

    telemetry_schema = StructType([
        StructField("event_id", IntegerType(), False),
        StructField("vehicle_id", IntegerType(), True),
        StructField("event_date", DateType(), True),
        StructField("speed_kmh", DoubleType(), True),
        StructField("engine_temp_c", DoubleType(), True),
        StructField("battery_level_pct", DoubleType(), True),
        StructField("odometer_km", DoubleType(), True),
        StructField("event_type", StringType(), True)
    ])

    event_types = ["normal", "normal", "normal", "warning", "critical", "idle"]
    tel_start = datetime(2024, 1, 1)

    telemetry_data = []
    for i in range(10000):
        etype = random.choice(event_types)
        speed = round(random.uniform(0, 180), 1) if etype != "idle" else 0.0
        temp = round(random.uniform(70, 120) if etype == "normal" else random.uniform(100, 150), 1)
        battery = round(random.uniform(20, 100), 1)
        telemetry_data.append(Row(
            event_id=i + 1,
            vehicle_id=random.randint(1, 300),
            event_date=(tel_start + timedelta(days=random.randint(0, 365))).date(),
            speed_kmh=speed,
            engine_temp_c=temp,
            battery_level_pct=battery,
            odometer_km=round(random.uniform(0, 200000), 1),
            event_type=etype
        ))

    telemetry_df = spark.createDataFrame(telemetry_data, schema=telemetry_schema)
    telemetry_df.write.format("delta").mode("overwrite").saveAsTable("telemetry_events")
    print(f"  ✓ Created telemetry_events table with {telemetry_df.count():,} records")

    # 4. Service Records Table
    print("- Generating service records data...")

    service_schema = StructType([
        StructField("service_id", IntegerType(), False),
        StructField("vehicle_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("service_date", DateType(), True),
        StructField("service_type", StringType(), True),
        StructField("cost_eur", DoubleType(), True),
        StructField("dealer_id", StringType(), True),
        StructField("technician_id", StringType(), True)
    ])

    service_types = ["Oil Change", "Brake Inspection", "Tire Rotation", "Battery Check",
                     "Software Update", "Annual Service", "Warranty Repair", "Recall Fix"]
    dealer_ids = [f"DEALER-{str(d).zfill(3)}" for d in range(1, 21)]
    svc_start = datetime(2022, 1, 1)

    service_data = []
    for i in range(2000):
        cust_row = customer_data[random.randint(0, len(customer_data) - 1)]
        service_data.append(Row(
            service_id=i + 1,
            vehicle_id=cust_row.vehicle_id,
            customer_id=cust_row.customer_id,
            service_date=(svc_start + timedelta(days=random.randint(0, 1095))).date(),
            service_type=random.choice(service_types),
            cost_eur=round(random.uniform(50, 2500), 2),
            dealer_id=random.choice(dealer_ids),
            technician_id=f"TECH-{random.randint(1, 50):03d}"
        ))

    service_df = spark.createDataFrame(service_data, schema=service_schema)
    service_df.write.format("delta").mode("overwrite").saveAsTable("service_records")
    print(f"  ✓ Created service_records table with {service_df.count():,} records")

    print("\n✓ Automotive data setup complete!")
    print(f"  Schema: trainer_demo.demo_05")
    print(f"  Tables: vehicles, customers, telemetry_events, service_records")


def setup(spark):
    print("Creating catalog trainer_demo")

    spark.sql(f"CREATE CATALOG IF NOT EXISTS trainer_demo")
    spark.sql(f"USE CATALOG trainer_demo")

    setup_01(spark)
    setup_02(spark)
    setup_03(spark)
    setup_04(spark)
    setup_05(spark)

    print("Setup complete")
