"""Authentication handler for Nebius S3 operations."""

import os
import configparser
import logging
import boto3
from typing import Optional

from skyplane.utils import logger

logger = logging.getLogger(__name__)

class NebiusS3Authentication:
    """Authentication handler for Nebius S3 operations using AWS-style credentials."""
    
    def __init__(self, credentials=None):
        """Initialize Nebius S3 authentication.
        
        Args:
            credentials (dict): Credentials dictionary with:
                - access_key_id: AWS-style access key
                - secret_access_key: AWS-style secret key
                - region: Optional region name
        """
        self.credentials = credentials or {}
        
        # Validate credentials
        if not self.credentials.get("access_key_id") or not self.credentials.get("secret_access_key"):
            raise ValueError("access_key_id and secret_access_key must be provided for S3 operations")
            
        logger.info("Initialized Nebius S3 authentication")
        
    @classmethod
    def from_config_file(cls, config_path=None):
        """Create authentication from a config file.
        
        Args:
            config_path (str): Path to config file (default: ~/.skyplane/config)
            
        Returns:
            NebiusS3Authentication: Authentication instance
        """
        # Use default config path if not provided
        if not config_path:
            config_path = os.path.expanduser("~/.skyplane/config")
            
        if not os.path.exists(config_path):
            raise ValueError(f"Config file not found: {config_path}")
            
        # Read config file
        config = configparser.ConfigParser()
        config.read(config_path)
        
        if 'nebius' not in config:
            raise ValueError(f"No [nebius] section found in config file: {config_path}")
            
        nebius_config = config['nebius']
        credentials = {
            'access_key_id': nebius_config.get('aws_access_key_id'),
            'secret_access_key': nebius_config.get('aws_secret_access_key'),
            'region': nebius_config.get('aws_default_region')
        }
        
        if not credentials['access_key_id'] or not credentials['secret_access_key']:
            raise ValueError(f"Missing required credentials in config file: {config_path}")
            
        logger.info(f"Loaded Nebius S3 credentials from {config_path}")
        return cls(credentials)
        
    def get_boto3_client(self, service: str, region: Optional[str] = None, **kwargs) -> boto3.client:
        """Get a boto3 client with Nebius credentials.
        
        Args:
            service (str): AWS service name (e.g., 's3')
            region (str): Region name (overrides credentials region)
            **kwargs: Additional arguments for boto3.client
            
        Returns:
            boto3.client: Configured boto3 client
        """
        # Use region from credentials if not provided
        if not region and self.credentials.get('region'):
            region = self.credentials['region']
            
        session = boto3.Session(
            aws_access_key_id=self.credentials["access_key_id"],
            aws_secret_access_key=self.credentials["secret_access_key"],
            region_name=region,
        )
        return session.client(service, **kwargs) 