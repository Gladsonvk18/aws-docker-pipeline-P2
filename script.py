import csv
import io
import os
import boto3
import mysql.connector

# Configuration from environment variables
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
S3_BUCKET = os.getenv("S3_BUCKET", "gladson-practice-pipeline-2")
S3_KEY = os.getenv("S3_KEY", "s3bucket.csv")

RDS_HOST = os.getenv("RDS_HOST")
RDS_USER = os.getenv("RDS_USER", "admin")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
RDS_DB = os.getenv("RDS_DB", "ingestion_db")

GLUE_DB = os.getenv("GLUE_DB", "gladson_pipeline_2_glue_db")
GLUE_TABLE = os.getenv("GLUE_TABLE", "users_fallback")

def read_csv_from_s3():
    print(f"Reading s3://{S3_BUCKET}/{S3_KEY} in {AWS_REGION}...")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    content = response["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    rows = []
    for row in reader:
        rows.append((int(row["id"]), row["name"].strip(), int(row["age"])))
    print(f"Successfully read {len(rows)} rows from S3.")
    return rows

def load_to_rds(rows):
    print(f"Connecting to RDS MySQL at {RDS_HOST}...")
    conn = mysql.connector.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DB,
        connection_timeout=10
    )
    cursor = conn.cursor()
    sql = """
        INSERT INTO users (id, name, age)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE name = VALUES(name), age = VALUES(age)
    """
    cursor.executemany(sql, rows)
    conn.commit()
    print(f"Inserted/updated {cursor.rowcount} rows in RDS table '{RDS_DB}.users'.")
    cursor.close()
    conn.close()

def register_glue_fallback():
    print(f"RDS unavailable. Registering fallback table in AWS Glue ({GLUE_DB}.{GLUE_TABLE})...")
    glue = boto3.client("glue", region_name=AWS_REGION)
    table_input = {
        "Name": GLUE_TABLE,
        "StorageDescriptor": {
            "Columns": [
                {"Name": "id", "Type": "int"},
                {"Name": "name", "Type": "string"},
                {"Name": "age", "Type": "int"},
            ],
            "Location": f"s3://{S3_BUCKET}/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                "Parameters": {"separatorChar": ",", "skip.header.line.count": "1"},
            },
        },
        "TableType": "EXTERNAL_TABLE",
    }
    try:
        glue.create_table(DatabaseName=GLUE_DB, TableInput=table_input)
        print(f"Created Glue fallback table '{GLUE_TABLE}'.")
    except glue.exceptions.AlreadyExistsException:
        glue.update_table(DatabaseName=GLUE_DB, TableInput=table_input)
        print(f"Updated existing Glue fallback table '{GLUE_TABLE}'.")

if __name__ == "__main__":
    records = read_csv_from_s3()
    try:
        load_to_rds(records)
        print("Pipeline completed via primary RDS path.")
    except Exception as e:
        print(f"RDS load failed: {e}")
        register_glue_fallback()
        print("Pipeline completed via AWS Glue fallback path.")
