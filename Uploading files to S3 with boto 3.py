import boto3, os
s3 = boto3.client('s3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1" # change as needed
)
bucket = "your-sparkify-bucket-<unique-suffix>"
s3.upload_file("song_data/song1.json", bucket, "song_data/song1.json")
