import os
import uuid
import pytest
import boto3
import configparser
from pathlib import Path
from botocore.exceptions import ClientError

from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.obj_store.nbs3_interface import NBS3Interface
from skyplane.utils import logger


def check_nebius_creds():
    """Verify Nebius credentials by attempting to list buckets"""
    config_path = os.path.expanduser("~/.skyplane/config")
    if not os.path.exists(config_path):
        return False
        
    try:
        # Read config file directly
        config = configparser.ConfigParser()
        config.read(config_path)
        
        if 'nebius' not in config:
            return False
            
        # Create test client with credentials
        session = boto3.Session(
            aws_access_key_id=config['nebius']['aws_access_key_id'],
            aws_secret_access_key=config['nebius']['aws_secret_access_key'],
            region_name='eu-north1'
        )
        
        # Create proper Config object
        s3_config = boto3.session.Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        )
        
        client = session.client(
            's3',
            endpoint_url='https://storage.eu-north1.nebius.cloud',
            config=s3_config
        )
        
        # Test credentials by listing buckets
        client.list_buckets()
        return True
        
    except Exception as e:
        logger.warning(f"Failed to validate Nebius credentials: {str(e)}")
        return False


requires_nebius = pytest.mark.skipif(
    not check_nebius_creds(),
    reason="Valid Nebius credentials not found or invalid"
)


@requires_nebius
def test_nebius_basic_operations():
    """Test basic bucket and object operations"""
    bucket_name = f"test-skyplane-{uuid.uuid4()}"
    test_file = "test_file.txt"
    test_content = b"Hello Nebius!"
    
    logger.info(f"\n\n=== STARTING NEBIUS S3 TEST with bucket {bucket_name} ===\n")
    
    # Create interface
    try:
        logger.info(f"Creating NBS3Interface for bucket {bucket_name}")
        iface = NBS3Interface(bucket_name)
        
        # Create a temporary file
        logger.info(f"Creating temporary test file: {test_file}")
        with open(test_file, "wb") as f:
            f.write(test_content)
            
        # Create bucket
        logger.info(f"Creating bucket in eu-north1")
        iface.create_bucket("nebius:eu-north1")
        
        # Check if bucket exists
        logger.info(f"Checking if bucket exists")
        assert iface.bucket_exists(), f"Bucket {bucket_name} should exist"
        
        # Upload object
        logger.info(f"Uploading test file to bucket")
        iface.upload_object(test_file, "test_object.txt")
        
        # Check if object exists
        logger.info(f"Checking if object exists")
        assert iface.exists("test_object.txt"), "Object should exist"
        
        # Get object
        logger.info(f"Getting object content")
        content = iface.get_object("test_object.txt")
        logger.info(f"Retrieved content: {content}")
        assert content == test_content, "Content should match"
        
        # Delete object
        logger.info(f"Deleting object")
        iface.delete_objects(["test_object.txt"])
        
        # Verify object is gone
        logger.info(f"Verifying object is deleted")
        assert not iface.exists("test_object.txt"), "Object should be deleted"
        
        # Delete bucket
        logger.info(f"Deleting bucket")
        iface.delete_bucket()
        
        # List all buckets to verify deletion
        s3_client = iface._s3_client()
        all_buckets = s3_client.list_buckets()
        bucket_names = [b['Name'] for b in all_buckets.get('Buckets', [])]
        logger.info(f"All buckets after deletion: {bucket_names}")
        assert bucket_name not in bucket_names, f"Bucket {bucket_name} should be deleted"
        
        logger.info(f"\n=== NEBIUS S3 TEST COMPLETED SUCCESSFULLY ===\n")
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        raise
    finally:
        # Clean up
        if os.path.exists(test_file):
            os.remove(test_file)
            logger.info(f"Removed temporary file: {test_file}")


@requires_nebius
def test_nebius_regions():
    """Test region-specific operations"""
    bucket_name = f"test-skyplane-{uuid.uuid4()}"
    
    iface = NBS3Interface(bucket_name)
    
    try:
        # Create bucket in eu-north1 (Finland)
        iface.create_bucket("nebius:eu-north1")
        assert iface.bucket_exists()
        
        # Verify correct endpoint
        assert "eu-north1" in iface._s3_client().meta.endpoint_url
        
        # Test basic operations
        test_content = b"Hello Nebius!"
        iface._s3_client().put_object(
            Bucket=bucket_name,
            Key="test.txt",
            Body=test_content
        )
        
        # Verify object exists
        response = iface._s3_client().get_object(
            Bucket=bucket_name,
            Key="test.txt"
        )
        assert response['Body'].read() == test_content
        
    finally:
        # Clean up
        try:
            # Delete all objects first
            objects = list(iface.list_objects())
            if objects:
                iface.delete_objects([obj.key for obj in objects])
            # Then delete bucket
            iface._s3_client().delete_bucket(Bucket=bucket_name)
        except Exception as e:
            logger.warning(f"Cleanup failed: {str(e)}") 