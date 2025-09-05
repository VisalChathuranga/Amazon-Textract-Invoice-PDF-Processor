"""
S3 folder synchronization functionality.
Handles bidirectional sync between local folder and S3 bucket with change detection.
"""

import boto3
import json
import os
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple
from botocore.exceptions import ClientError
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class S3FolderSync:
    """
    Manages synchronization between local PDF folder and S3 bucket.
    Uses MD5 hashing to detect file changes and avoid unnecessary uploads.
    """
    
    def __init__(self, s3_client, bucket_name: str, s3_prefix: str = "invoices/"):
        self.s3 = s3_client
        self.bucket_name = bucket_name
        self.s3_prefix = s3_prefix
        self.sync_metadata_file = ".s3_sync_metadata.json"
        
    def get_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of a file"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def load_sync_metadata(self) -> Dict:
        """Load synchronization metadata from local cache"""
        if os.path.exists(self.sync_metadata_file):
            try:
                with open(self.sync_metadata_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def save_sync_metadata(self, metadata: Dict):
        """Save synchronization metadata to local cache"""
        with open(self.sync_metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def get_s3_files(self) -> Dict[str, Dict]:
        """Get list of files currently in S3"""
        s3_files = {}
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.s3_prefix)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('.pdf'):
                            filename = os.path.basename(key)
                            s3_files[filename] = {
                                'key': key,
                                'size': obj['Size'],
                                'last_modified': obj['LastModified'].isoformat()
                            }
        except ClientError as e:
            logger.error(f"Error listing S3 files: {e}")
        
        return s3_files
    
    def sync_folder(self, local_folder: str) -> Tuple[List[str], List[str], List[str]]:
        """
        Main sync method that performs three operations:
        1. Upload new or modified local files to S3
        2. Skip unchanged files (based on hash comparison)
        3. Delete S3 files that no longer exist locally
        Returns lists of uploaded, skipped, and deleted files.
        """
        uploaded = []
        skipped = []
        deleted = []
        
        # Load metadata
        sync_metadata = self.load_sync_metadata()
        s3_files = self.get_s3_files()
        
        # Get local PDF files
        local_files = {}
        for file_path in Path(local_folder).glob("*.pdf"):
            filename = file_path.name
            file_hash = self.get_file_hash(str(file_path))
            local_files[filename] = {
                'path': str(file_path),
                'hash': file_hash,
                'size': file_path.stat().st_size
            }
        
        # Upload new or modified files
        for filename, file_info in local_files.items():
            s3_key = f"{self.s3_prefix}{filename}"
            
            # Check if file needs to be uploaded
            needs_upload = False
            
            if filename not in s3_files:
                # File doesn't exist in S3
                needs_upload = True
                logger.info(f"New file detected: {filename}")
            elif filename not in sync_metadata or sync_metadata[filename]['hash'] != file_info['hash']:
                # File has been modified
                needs_upload = True
                logger.info(f"Modified file detected: {filename}")
            else:
                # File unchanged
                skipped.append(filename)
                logger.info(f"Skipping unchanged file: {filename}")
            
            if needs_upload:
                try:
                    logger.info(f"Uploading {filename} to s3://{self.bucket_name}/{s3_key}")
                    self.s3.upload_file(file_info['path'], self.bucket_name, s3_key)
                    uploaded.append(filename)
                    
                    # Update metadata
                    sync_metadata[filename] = {
                        'hash': file_info['hash'],
                        'size': file_info['size'],
                        's3_key': s3_key,
                        'last_synced': datetime.now().isoformat()
                    }
                except ClientError as e:
                    logger.error(f"Failed to upload {filename}: {e}")
        
        # Remove files from S3 that no longer exist locally
        for filename in s3_files:
            if filename not in local_files:
                s3_key = s3_files[filename]['key']
                try:
                    logger.info(f"Deleting {filename} from S3 (no longer exists locally)")
                    self.s3.delete_object(Bucket=self.bucket_name, Key=s3_key)
                    deleted.append(filename)
                    
                    # Remove from metadata
                    if filename in sync_metadata:
                        del sync_metadata[filename]
                except ClientError as e:
                    logger.error(f"Failed to delete {filename} from S3: {e}")
        
        # Save updated metadata
        self.save_sync_metadata(sync_metadata)
        
        return uploaded, skipped, deleted