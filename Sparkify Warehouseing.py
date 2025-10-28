from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

# Access environment variables
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")

print("Snowflake user:", user)
