import base64
import hashlib
import os
from functools import lru_cache
from pathlib import Path
from datetime import datetime, UTC
import configparser

from typing import Any, Iterator, List, Optional, Tuple

from skyplane import exceptions, compute
from skyplane.exceptions import NoSuchObjectException
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.config_paths import cloud_config
from skyplane.utils import logger, imports
from skyplane.utils.generator import batch_generator
from skyplane.obj_store.s3_interface import S3Interface, S3Object
from botocore.config import Config
from skyplane.compute.nebius.nebius_auth import NebiusAuthentication
from skyplane.compute.nebius.nebius_s3_auth import NebiusS3Authentication
import boto3


class NBS3Interface(S3Interface):
    # Available Nebius regions with their endpoints
    NEBIUS_REGIONS = {
        "eu-north1": "https://storage.eu-north1.nebius.cloud",  # Finland
        # "eu-west1": "https://storage.eu-west1.nebius.cloud",  # France - Not supported yet
    }

    def __init__(self, bucket_name: str):
        super().__init__(bucket_name)
        
        # Try to read credentials from ~/.skyplane/config first
        config_path = os.path.expanduser("~/.skyplane/config")
        logger.info(f"Looking for Nebius S3 credentials in {config_path}")
        
        try:
            # Use the new S3 authentication class
            self.auth = NebiusS3Authentication.from_config_file(config_path)
            logger.info(f"Successfully initialized Nebius S3 authentication for bucket: {bucket_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Nebius S3 authentication: {e}")
            raise
        
        self._cached_s3_clients = {}
        logger.info(f"NBS3Interface initialized for bucket: {bucket_name}")

    @property
    def provider(self):
        return "nebius"

    def path(self):
        return f"nebius://{self.bucket_name}"

    @property
    def aws_region(self):
        if not self._region:
            # Default to eu-north1 if region not specified
            self._region = "eu-north1"
        return self._region

    def region_tag(self):
        return "nebius:" + self.aws_region

    def _s3_client(self, region=None):
        region = region if region is not None else self.aws_region
        if region not in self.NEBIUS_REGIONS:
            raise ValueError(f"Invalid Nebius region: {region}. Available regions: {list(self.NEBIUS_REGIONS.keys())}")
            
        if region not in self._cached_s3_clients:
            # Create proper Config object for S3
            s3_config = boto3.session.Config(
                signature_version='s3v4',
                s3={'addressing_style': 'path'}
            )
            
            self._cached_s3_clients[region] = self.auth.get_boto3_client(
                "s3",
                region,
                endpoint_url=self.NEBIUS_REGIONS[region],
                config=s3_config  # Pass config object instead of individual settings
            )
        return self._cached_s3_clients[region]

    def create_bucket(self, region_tag: str):
        # Extract region from region_tag (e.g., "nebius:eu-north1" -> "eu-north1")
        region = region_tag.split(":")[-1] if ":" in region_tag else region_tag
        if region not in self.NEBIUS_REGIONS:
            raise ValueError(f"Invalid Nebius region: {region}. Available regions: {list(self.NEBIUS_REGIONS.keys())}")
        
        self._region = region  # Set the region
        logger.info(f"Creating bucket {self.bucket_name} in region {region}")
        
        s3_client = self._s3_client(region)
        
        if not self.bucket_exists():
            # Don't specify LocationConstraint for Nebius S3
            try:
                logger.info(f"Bucket {self.bucket_name} doesn't exist, creating it now...")
                response = s3_client.create_bucket(Bucket=self.bucket_name)
                logger.info(f"Bucket created successfully: {response}")
                
                # Verify bucket exists
                all_buckets = s3_client.list_buckets()
                bucket_names = [b['Name'] for b in all_buckets.get('Buckets', [])]
                logger.info(f"All buckets: {bucket_names}")
                
                if self.bucket_name in bucket_names:
                    logger.info(f"Confirmed bucket {self.bucket_name} exists in bucket list")
                else:
                    logger.warning(f"Bucket {self.bucket_name} not found in bucket list after creation!")
            except Exception as e:
                logger.error(f"Failed to create bucket {self.bucket_name}: {e}")
                raise
        else:
            logger.info(f"Bucket {self.bucket_name} in region {region} already exists")

    def delete_bucket(self):
        # delete 1000 keys at a time
        for batch in batch_generator(self.list_objects(), 1000):
            self.delete_objects([obj.key for obj in batch])
        assert len(list(self.list_objects())) == 0, f"Bucket not empty after deleting all keys {list(self.list_objects())}"
        # delete bucket
        self._s3_client().delete_bucket(Bucket=self.bucket_name)

    def list_objects(self, prefix="", region=None) -> Iterator[S3Object]:
        paginator = self._s3_client(region).get_paginator("list_objects_v2")
        requester_pays = {"RequestPayer": "requester"} if self.requester_pays else {}
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix, **requester_pays)
        for page in page_iterator:
            objs = []
            for obj in page.get("Contents", []):
                # Convert to timezone-aware datetime if it's naive
                last_modified = obj["LastModified"]
                if last_modified.tzinfo is None:
                    last_modified = datetime.fromtimestamp(last_modified.timestamp(), UTC)
                
                objs.append(
                    S3Object(
                        obj["Key"],
                        provider=self.provider,
                        bucket=self.bucket(),
                        size=obj["Size"],
                        last_modified=last_modified,  # Use timezone-aware datetime
                        mime_type=obj.get("ContentType"),
                    )
                )
            yield from objs

    def delete_objects(self, keys: List[str]):
        s3_client = self._s3_client()
        while keys:
            batch, keys = keys[:1000], keys[1000:]  # take up to 1000 keys at a time
            s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": [{"Key": k} for k in batch]})

    @lru_cache(maxsize=1024)
    @imports.inject("botocore.exceptions", pip_extra="aws")
    def get_obj_metadata(botocore_exceptions, self, obj_name):
        s3_client = self._s3_client()
        try:
            return s3_client.head_object(Bucket=self.bucket_name, Key=str(obj_name))
        except botocore_exceptions.ClientError as e:
            raise NoSuchObjectException(f"Object {obj_name} does not exist, or you do not have permission to access it") from e

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name)["ContentLength"]

    def get_obj_last_modified(self, obj_name):
        return self.get_obj_metadata(obj_name)["LastModified"]

    def get_obj_mime_type(self, obj_name):
        return self.get_obj_metadata(obj_name)["ContentType"]

    def exists(self, obj_name):
        try:
            self.get_obj_metadata(obj_name)
            return True
        except NoSuchObjectException:
            return False

    def download_object(
        self,
        src_object_name,
        dst_file_path,
        offset_bytes=None,
        size_bytes=None,
        write_at_offset=False,
        generate_md5=False,
        write_block_size=2**16,
    ) -> Tuple[Optional[str], Optional[bytes]]:
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)

        s3_client = self._s3_client()
        assert len(src_object_name) > 0, f"Source object name must be non-empty: '{src_object_name}'"
        args = {"Bucket": self.bucket_name, "Key": src_object_name}
        assert not (offset_bytes and not size_bytes), f"Cannot specify {offset_bytes} without {size_bytes}"
        if offset_bytes is not None and size_bytes is not None:
            args["Range"] = f"bytes={offset_bytes}-{offset_bytes + size_bytes - 1}"
        if self.requester_pays:
            args["RequestPayer"] = "requester"
        response = s3_client.get_object(**args)

        # write response data
        if not os.path.exists(dst_file_path):
            open(dst_file_path, "a").close()
        if generate_md5:
            m = hashlib.md5()
        with open(dst_file_path, "wb+" if write_at_offset else "wb") as f:
            f.seek(offset_bytes if write_at_offset else 0)
            b = response["Body"].read(write_block_size)
            while b:
                if generate_md5:
                    m.update(b)
                f.write(b)
                b = response["Body"].read(write_block_size)
        response["Body"].close()
        md5 = m.digest() if generate_md5 else None
        mime_type = response["ContentType"]
        return mime_type, md5

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def upload_object(
        botocore_exceptions, self, src_file_path, dst_object_name, part_number=None, upload_id=None, check_md5=None, mime_type=None
    ):
        dst_object_name, src_file_path = str(dst_object_name), str(src_file_path)
        s3_client = self._s3_client()
        assert len(dst_object_name) > 0, f"Destination object name must be non-empty: '{dst_object_name}'"
        b64_md5sum = base64.b64encode(check_md5).decode("utf-8") if check_md5 else None
        checksum_args = dict(ContentMD5=b64_md5sum) if b64_md5sum else dict()

        logger.info(f"Uploading {src_file_path} to {self.bucket_name}/{dst_object_name}")
        
        try:
            with open(src_file_path, "rb") as f:
                if upload_id:
                    logger.info(f"Uploading part {part_number} of multipart upload {upload_id}")
                    response = s3_client.upload_part(
                        Body=f,
                        Key=dst_object_name,
                        Bucket=self.bucket_name,
                        PartNumber=part_number,
                        UploadId=upload_id.strip(),
                        **checksum_args,
                    )
                    logger.info(f"Part uploaded successfully: {response}")
                else:
                    mime_args = dict(ContentType=mime_type) if mime_type else dict()
                    logger.info(f"Uploading object with mime type: {mime_type}")
                    response = s3_client.put_object(
                        Body=f, 
                        Key=dst_object_name, 
                        Bucket=self.bucket_name, 
                        **checksum_args, 
                        **mime_args
                    )
                    logger.info(f"Object uploaded successfully: {response}")
                    
                    # Verify object exists
                    try:
                        head = s3_client.head_object(Bucket=self.bucket_name, Key=dst_object_name)
                        logger.info(f"Confirmed object exists: size={head.get('ContentLength')} bytes")
                    except Exception as e:
                        logger.warning(f"Could not verify object exists after upload: {e}")
        except botocore_exceptions.ClientError as e:
            # catch MD5 mismatch error and raise appropriate exception
            if "Error" in e.response and "Code" in e.response["Error"] and e.response["Error"]["Code"] == "InvalidDigest":
                raise exceptions.ChecksumMismatchException(f"Checksum mismatch for object {dst_object_name}") from e
            raise

    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        client = self._s3_client()
        assert len(dst_object_name) > 0, f"Destination object name must be non-empty: '{dst_object_name}'"
        response = client.create_multipart_upload(
            Bucket=self.bucket_name, Key=dst_object_name, **(dict(ContentType=mime_type) if mime_type else dict())
        )
        if "UploadId" in response:
            return response["UploadId"]
        else:
            raise exceptions.SkyplaneException(f"Failed to initiate multipart upload for {dst_object_name}: {response}")

    def complete_multipart_upload(self, dst_object_name, upload_id, metadata: Optional[Any] = None):
        s3_client = self._s3_client()
        all_parts = []
        while True:
            response = s3_client.list_parts(
                Bucket=self.bucket_name, Key=dst_object_name, MaxParts=100, UploadId=upload_id, PartNumberMarker=len(all_parts)
            )
            if "Parts" not in response:
                break
            else:
                if len(response["Parts"]) == 0:
                    break
                all_parts += response["Parts"]
        all_parts = sorted(all_parts, key=lambda d: d["PartNumber"])
        response = s3_client.complete_multipart_upload(
            UploadId=upload_id,
            Bucket=self.bucket_name,
            Key=dst_object_name,
            MultipartUpload={"Parts": [{"PartNumber": p["PartNumber"], "ETag": p["ETag"]} for p in all_parts]},
        )
        assert "ETag" in response, f"Failed to complete multipart upload for {dst_object_name}: {response}"

    def get_object(self, key: str) -> bytes:
        """Get object from bucket"""
        try:
            response = self._s3_client().get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except Exception as e:
            raise NoSuchObjectException(f"Failed to get object {key}: {str(e)}")

    def get_obj_size(self, key: str) -> int:
        """Get object size"""
        try:
            response = self._s3_client().head_object(Bucket=self.bucket_name, Key=key)
            return response['ContentLength']
        except Exception as e:
            raise NoSuchObjectException(f"Failed to get object size for {key}: {str(e)}")

    def exists(self, key: str) -> bool:
        """Check if object exists"""
        try:
            self._s3_client().head_object(Bucket=self.bucket_name, Key=key)
            return True
        except:
            return False

    def upload_object(self, local_path: str, remote_path: str):
        """Upload object to bucket"""
        try:
            self._s3_client().upload_file(local_path, self.bucket_name, remote_path)
        except Exception as e:
            raise Exception(f"Failed to upload {local_path} to {remote_path}: {str(e)}")

    def bucket_exists(self) -> bool:
        """Check if the bucket exists"""
        try:
            s3_client = self._s3_client()
            all_buckets = s3_client.list_buckets()
            bucket_names = [b['Name'] for b in all_buckets.get('Buckets', [])]
            exists = self.bucket_name in bucket_names
            logger.info(f"Bucket {self.bucket_name} exists: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Error checking if bucket {self.bucket_name} exists: {e}")
            return False
