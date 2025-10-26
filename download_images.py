#!/usr/bin/env python3
"""
Download all files from a Supabase storage bucket
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from supabase.client import create_client, Client

# Load environment variables from .env
load_dotenv()


def get_supabase_client() -> Client:
    """Initialize Supabase client"""
    supabase_url = os.getenv("supabase_url")
    supabase_key = os.getenv("supabase_key")

    if not supabase_url or not supabase_key:
        raise ValueError("Missing supabase_url or supabase_key in .env file")

    return create_client(supabase_url, supabase_key)


def download_files(bucket_name: str = None, output_dir: str = "questions"):
    """Download all files from a Supabase storage bucket"""
    if bucket_name is None:
        bucket_name = os.getenv("storage_bucket", "papers")

    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    print(f"📦 Initializing Supabase client...")
    supabase = get_supabase_client()

    print(f"📂 Downloading files from bucket '{bucket_name}' to '{output_dir}'...")

    try:
        file_list = supabase.storage.from_(bucket_name).list()
        if not file_list:
            print("❌ No files found in bucket or bucket doesn't exist")
            return

        print(f"✅ Found {len(file_list)} files")

        downloaded_count = 0
        for file_obj in file_list:
            file_name = file_obj["name"]
            local_file_name = file_name.replace("/", "_")  # Flatten folders
            file_path = output_path / local_file_name

            try:
                print(f"⬇️  Downloading: {file_name}...", end=" ", flush=True)
                # download() returns a response object with data
                response = supabase.storage.from_(bucket_name).download(file_name)

                if not response:
                    print("❌ No data received")
                    continue

                with open(file_path, "wb") as f:
                    f.write(response)

                file_size = file_path.stat().st_size / 1024
                print(f"✅ ({file_size:.1f} KB)")
                downloaded_count += 1

            except Exception as e:
                print(f"❌ Error downloading {file_name}: {str(e)}")
                continue

        print(f"\n✨ Download complete! {downloaded_count} files saved to '{output_dir}'")

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
