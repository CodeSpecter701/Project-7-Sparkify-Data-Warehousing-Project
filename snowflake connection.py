from dotenv import load_dotenv
import snowflake.connector
import os

# Load environment variables
load_dotenv()

# Get credentials from .env
sf_user = os.getenv("ANOOSH707")
sf_password = os.getenv("CloudEngineer101")
sf_account = os.getenv("WD65648")
sf_database = os.getenv("SPARKIFY_DB")
sf_schema = os.getenv("PUBLIC")
sf_warehouse = os.getenv("COMPUTE_WH")
sf_role = os.getenv("ACCOUNTADMIN")

aws_key = os.getenv("AKIA5H2PNENJSYFG6T6A")
aws_secret = os.getenv("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")
s3_bucket = os.getenv("sparkify-warehousing-project")
s3_path = os.getenv("https://us-east-1.console.aws.amazon.com/s3/buckets/sparkify-warehousing-project?region=us-east-1&bucketType=general&tab=objects")

# ✅ Connect to Snowflake
conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema,
    role=sf_role
)
cursor = conn.cursor()
print("✅ Connected to Snowflake successfully!")

# ✅ Create target table (example)
cursor.execute("""
    CREATE OR REPLACE TABLE staging_events (
        artist STRING,
        auth STRING,
        firstName STRING,
        gender STRING,
        itemInSession INTEGER,
        lastName STRING,
        length FLOAT,
        level STRING,
        location STRING,
        method STRING,
        page STRING,
        registration FLOAT,
        sessionId INTEGER,
        song STRING,
        status INTEGER,
        ts FLOAT,
        userAgent STRING,
        userId STRING
    );
""")
print("✅ Table created or replaced.")

# ✅ Create Snowflake stage for S3
cursor.execute(f"""
    CREATE OR REPLACE STAGE my_s3_stage
    URL='s3://{s3_bucket}/{s3_path}'
    CREDENTIALS=(
        AWS_KEY_ID='{aws_key}'
        AWS_SECRET_KEY='{aws_secret}'
    )
    FILE_FORMAT = (TYPE = 'JSON');
""")
print("✅ Stage created successfully!")

# ✅ Copy data from S3 into Snowflake table
cursor.execute("""
    COPY INTO staging_events
    FROM @sparkify-warehouseing-project
    FILE_FORMAT = (TYPE = 'JSON')
    ON_ERROR = 'CONTINUE';
""")
print("✅ Data copied from S3 to Snowflake table!")

# ✅ Check row count
cursor.execute("SELECT COUNT(*) FROM staging_events;")
count = cursor.fetchone()[0]
print(f"✅ Total records loaded: {count}")

# ✅ Close connection
cursor.close()
conn.close()
