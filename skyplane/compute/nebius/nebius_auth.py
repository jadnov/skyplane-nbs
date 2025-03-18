from typing import Dict, List, Optional, Union, Any
import os
import configparser
from pathlib import Path
import boto3
from botocore.config import Config
import json
import logging
import asyncio
import tempfile

from skyplane.config import SkyplaneConfig
from skyplane.config_paths import cloud_config
from skyplane.utils import logger

# Try to import Nebius SDK
try:
    from nebius.sdk import SDK
    from nebius.base.service_account.pk_file import Reader as PKReader
    from nebius.aio.service_error import RequestError
    HAVE_NEBIUS_SDK = True
except ImportError:
    HAVE_NEBIUS_SDK = False
    SDK = None
    PKReader = None
    RequestError = None

logger = logging.getLogger(__name__)

class NebiusAuthentication:
    """Authentication handler for Nebius cloud services."""
    
    def __init__(self, credentials=None):
        """Initialize Nebius authentication.
        
        Args:
            credentials (dict): Credentials dictionary with either:
                - credentials_file: Path to credentials JSON file
                - Or direct credentials: service_account_id, publickey_id, private_key
                - Or S3 credentials: access_key_id, secret_access_key (for S3 operations only)
        """
        if not HAVE_NEBIUS_SDK:
            raise ImportError("Nebius SDK not installed. Please install with 'pip install nebius'")

        self.credentials = credentials or {}
        self._sdk = None
        self._cleanup_tasks = []
        self._private_key_path = None
        
        # Check if we have S3-only credentials
        self._s3_only_mode = False
        if self.credentials.get("access_key_id") and self.credentials.get("secret_access_key"):
            self._s3_only_mode = True
            logger.info("Using S3-only mode with AWS-style credentials")
            return  # Skip SDK validation for S3-only mode
        
        # Validate credentials for SDK operations
        if not self.credentials.get("credentials_file") and not (
            "service_account_id" in self.credentials and "publickey_id" in self.credentials and "private_key" in self.credentials
        ):
            raise ValueError("Either credentials_file or service_account_id, publickey_id, and private_key must be provided")
            
    async def initialize(self) -> None:
        """Initialize SDK and verify authentication."""
        # Skip initialization in S3-only mode
        if getattr(self, '_s3_only_mode', False):
            logger.info("Skipping SDK initialization in S3-only mode")
            return

        if self._sdk is not None:
            logger.debug("SDK already initialized")
            return

        logger.info("Initializing Nebius authentication")
        
        # Get the service account credentials from file
        credentials_file = self.credentials.get("credentials_file")
        
        try:
            # Read the credentials file
            if credentials_file:
                with open(credentials_file, 'r') as f:
                    creds_data = json.load(f)
                
            # Determine if we need to convert the credentials format
            needs_conversion = "id" not in creds_data or "subject-credentials" not in creds_data
            
            if needs_conversion:
                logger.info("Converting credentials to required format")
                creds_data = self._convert_credentials_format(creds_data)
                
            # Extract subject credentials
            subject_creds = creds_data.get("subject-credentials", {})
            if not subject_creds:
                raise ValueError("Missing subject-credentials in credentials file")
                
            # Check required fields
            required_fields = ["service_account_id", "public_key_id", "private_key"]
            for field in required_fields:
                if field not in subject_creds:
                    raise ValueError(f"Missing {field} in subject-credentials")
                    
            # Create temporary private key file with proper PEM format
            self._private_key_path = await self._create_private_key_file(subject_creds["private_key"])
            
            # Initialize SDK with private key
            self._sdk = SDK(
                credentials=PKReader(
                    filename=self._private_key_path,
                    service_account_id=subject_creds["service_account_id"],
                    public_key_id=subject_creds["public_key_id"]
                )
            )
            
            # Verify authentication
            logger.info("Verifying authentication with whoami call")
            try:
                response = await asyncio.wait_for(self._sdk.whoami(), timeout=30.0)
                
                if hasattr(response, "anonymous_profile"):
                    raise ValueError("Authentication failed: received anonymous profile")
                    
                if hasattr(response, "service_account_profile"):
                    profile = response.service_account_profile
                    logger.info(f"Authenticated as service account: {profile.info.metadata.id}")
                else:
                    logger.warning("Unexpected response from whoami")
                    
            except asyncio.TimeoutError:
                raise TimeoutError("Authentication verification timed out")
                
            except RequestError as e:
                raise ValueError(f"Authentication request failed: {e}")
                
            logger.info("Nebius authentication initialized successfully")
            
        except Exception as e:
            # Clean up any resources if initialization fails
            await self.cleanup()
            raise ValueError(f"Failed to initialize Nebius authentication: {e}") from e
            
    def _convert_credentials_format(self, current_creds: Dict[str, Any]) -> Dict[str, Any]:
        """Convert old credentials format to new format with subject-credentials."""
        # Get required fields from original format
        service_account_id = current_creds.get("service_account_id")
        public_key_id = current_creds.get("publickey_id")
        private_key = current_creds.get("private_key")
        
        if not service_account_id or not public_key_id or not private_key:
            raise ValueError("Missing required fields in credentials file")
            
        # Create new format
        return {
            "id": service_account_id,
            "subject-credentials": {
                "id": service_account_id,
                "service_account_id": service_account_id,
                "public_key_id": public_key_id,
                "private_key": private_key,
                "created_at": "2024-01-01T00:00:00.000000Z",
                "key_algorithm": "RSA_2048"
            }
        }
            
    async def _create_private_key_file(self, private_key: str) -> str:
        """Create a properly formatted private key file.
        
        Returns:
            Path to the temporary private key file
        """
        # Create a temporary file for the private key
        fd, path = tempfile.mkstemp(suffix=".pem")
        os.close(fd)
        
        # Format the private key properly with correct line breaks
        if not private_key.startswith("-----BEGIN PRIVATE KEY-----"):
            private_key = "-----BEGIN PRIVATE KEY-----\n" + private_key
            
        if not private_key.endswith("-----END PRIVATE KEY-----"):
            private_key = private_key + "\n-----END PRIVATE KEY-----"
            
        # Add proper line breaks (every 64 characters)
        content = private_key.replace("-----BEGIN PRIVATE KEY-----\n", "").replace("\n-----END PRIVATE KEY-----", "")
        formatted_content = ""
        for i in range(0, len(content), 64):
            formatted_content += content[i:i+64] + "\n"
            
        formatted_key = f"-----BEGIN PRIVATE KEY-----\n{formatted_content}-----END PRIVATE KEY-----\n"
        
        # Write the formatted key to the file
        with open(path, "w") as f:
            f.write(formatted_key)
            
        # Add to cleanup tasks
        self._cleanup_tasks.append(lambda: os.unlink(path))
        
        return path
        
    @property
    def sdk(self) -> Optional[SDK]:
        """Get the initialized SDK instance."""
        if getattr(self, '_s3_only_mode', False):
            raise ValueError("SDK not available in S3-only mode. Use get_boto3_client for S3 operations.")
        return self._sdk
        
    async def cleanup(self) -> None:
        """Clean up resources."""
        # Execute all cleanup tasks
        for task in self._cleanup_tasks:
            try:
                task()
            except Exception as e:
                logger.warning(f"Cleanup task failed: {e}")
                
        self._cleanup_tasks = []
        
        # Clean up SDK if needed
        if self._sdk is not None:
            self._sdk = None
            
        # Remove private key file if it exists
        if self._private_key_path and os.path.exists(self._private_key_path):
            try:
                os.unlink(self._private_key_path)
                self._private_key_path = None
            except Exception as e:
                logger.warning(f"Failed to remove private key file: {e}")

    def get_boto3_client(self, service: str, region: Optional[str] = None, **kwargs) -> boto3.client:
        """Get a boto3 client with Nebius credentials"""
        if not self.credentials:
            raise ValueError("Nebius credentials not loaded")

        # Check for required S3 credentials
        if "access_key_id" not in self.credentials or "secret_access_key" not in self.credentials:
            raise ValueError("S3 credentials (access_key_id/secret_access_key) required for this operation")

        session = boto3.Session(
            aws_access_key_id=self.credentials["access_key_id"],
            aws_secret_access_key=self.credentials["secret_access_key"],
            region_name=region,
        )
        return session.client(service, **kwargs)

    def get_service_client(self, client_class):
        """Get a service client with proper authentication"""
        if not self._sdk:
            raise RuntimeError("SDK not initialized. Call initialize() first")
            
        # Create client with SDK instance
        return client_class(self._sdk)

    @staticmethod
    def get_region_config() -> List[str]:
        """Return list of available Nebius regions"""
        return [
            "eu-north1",
            "eu-west1"
        ] 