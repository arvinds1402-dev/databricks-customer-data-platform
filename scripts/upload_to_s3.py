
import boto3
from pathlib import Path
from botocore.exceptions import ClientError, ProfileNotFound

# AWS configuration
AWS_PROFILE = "python-etl"
AWS_REGION = "ap-south-1"
BUCKET_NAME = "python-etl-learning-2026-arvind-unique123"
S3_PREFIX = "raw/python_upload_test/"

# Local source files
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_FOLDER = PROJECT_ROOT / "data"

FILES_TO_UPLOAD = [
    DATA_FOLDER / "customers.csv",
    DATA_FOLDER / "transactions.csv",
]


def upload_files():
    try:
        # Authenticate using the existing AWS SSO profile
        session = boto3.Session(
            profile_name=AWS_PROFILE,
            region_name=AWS_REGION
        )

        s3_client = session.client("s3")

        for file_path in FILES_TO_UPLOAD:
            if not file_path.exists():
                print(f"File not found: {file_path}")
                continue

            s3_key = S3_PREFIX + file_path.name

            s3_client.upload_file(
                str(file_path),
                BUCKET_NAME,
                s3_key
            )

            print(f"Uploaded: {file_path.name}")
            print(f"S3 location: s3://{BUCKET_NAME}/{s3_key}")

        print("\nS3 upload process completed.")

    except ProfileNotFound:
        print(f"AWS profile '{AWS_PROFILE}' was not found.")

    except ClientError as error:
        print(f"AWS error: {error}")


if __name__ == "__main__":
    upload_files()
