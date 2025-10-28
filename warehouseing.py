from dotenv import load_dotenv
import os

# Load environment variables from .env file in the same directory
load_dotenv()

# Fetch and print to verify
print("Snowflake user:", os.getenv("SNOWFLAKE_USER"))
