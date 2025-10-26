#!/usr/bin/env python3
"""
Download all files from a Supabase storage bucket using S3 access
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import boto3
from botocore.config import Config

# Load environment variables from .env
load_dotenv()


def get_s3_client():
    """Initialize S3 client with provided credentials"""
    access_key_id = os.getenv("access_key_id")
    secret_access_key = os.getenv("secret_access_key")
    s3_endpoint = os.getenv("s3_endpoint", "https://storage.googleapis.com")
    s3_region = os.getenv("s3_region", "auto")
    
    if not access_key_id or not secret_access_key:
        raise ValueError("Missing access_key_id or secret_access_key in .env file")
    
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=s3_region,
        config=Config(max_pool_connections=50)
    )
    
    return s3_client


def download_files(bucket_name: str = None, output_dir: str = ""):
    """Download all files from S3 storage bucket"""
    if bucket_name is None:
        bucket_name = os.getenv("storage_bucket", "papers")

    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    print(f"📦 Initializing S3 client...")
    s3_client = get_s3_client()

    print(f"📂 Downloading files from bucket '{bucket_name}' to '{output_dir}'...")

    try:
        # List all objects in the bucket
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name)
        
        file_list = []
        for page in pages:
            if "Contents" in page:
                file_list.extend(page["Contents"])
        
        if not file_list:
            print("❌ No files found in bucket or bucket doesn't exist")
            return

        print(f"✅ Found {len(file_list)} files")

        downloaded_count = 0
        failed_count = 0
        
        for file_obj in file_list:
            file_key = file_obj["Key"]
            
            # Skip if it's a directory placeholder
            if file_key.endswith("/"):
                continue
            
            local_file_name = file_key.replace("/", "_")  # Flatten folders
            file_path = output_path / local_file_name

            try:
                print(f"⬇️  Downloading: {file_key}...", end=" ", flush=True)
                s3_client.download_file(bucket_name, file_key, str(file_path))

                file_size = file_path.stat().st_size / 1024
                print(f"✅ ({file_size:.1f} KB)")
                downloaded_count += 1

            except Exception as e:
                print(f"❌ Error: {str(e)}")
                failed_count += 1
                continue

        print(f"\n✨ Download complete! {downloaded_count} files saved to '{output_dir}'")
        if failed_count > 0:
            print(f"⚠️  {failed_count} files failed to download")

    except Exception as e:
        print(f"❌ Error accessing bucket: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        output_dir = sys.argv[1] if len(sys.argv) > 1 else "downloaded_files"
        download_files(output_dir=output_dir)
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        sys.exit(1)
