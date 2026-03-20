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
    
def setup(spark):
    print("Creating catalog trainer_demo")
    
    spark.sql(f"CREATE CATALOG IF NOT EXISTS trainer_demo")
    spark.sql(f"USE CATALOG trainer_demo")
    
    setup_01(spark)
    setup_02(spark)
    setup_03(spark)
    
    print("Setup complete")
