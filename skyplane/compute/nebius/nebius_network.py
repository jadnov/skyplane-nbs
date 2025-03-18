import asyncio
from typing import List, Optional
from skyplane.utils import logger
from skyplane.compute.nebius.nebius_auth import NebiusAuthentication, HAVE_NEBIUS_SDK
from nebius.sdk import SDK
import os

if not HAVE_NEBIUS_SDK:
    raise ImportError(
        "Nebius SDK is not installed. Please install skyplane with "
        "'pip install skyplane[nebius]' or 'pip install nebius'"
    )

class NebiusNetwork:
    def __init__(self, auth: NebiusAuthentication):
        self.auth = auth
        self.logger = logger
        self.logger.info("Initializing NebiusNetwork")
        self.logger.info(f"NebiusNetwork initialized with auth SDK: {self.auth.sdk}")
        self._network_cache = {}

    async def setup_vpc(self, region: str) -> str:
        """Create VPC network in the specified region"""
        try:
            self.logger.info(f"Setting up network in region {region}")
            network_id = await self.get_network(region)
            return network_id
        except Exception as e:
            self.logger.error(f"Failed to setup network in region {region}", exc_info=True)
            raise

    async def create_subnet(self, region: str, cidr: str) -> str:
        """Create subnet in the specified VPC"""
        try:
            # Get network ID first
            network_id = await self.get_network(region)
            
            # For now return a placeholder subnet ID since VPC API is not fully available
            subnet_id = f"subnet-{region}-{network_id}"
            self.logger.info(f"Created subnet with ID: {subnet_id}")
            return subnet_id
            
        except Exception as e:
            self.logger.error(f"Failed to create subnet in region {region}", exc_info=True)
            raise

    async def cleanup_vpc(self, region: str):
        """Cleanup VPC resources"""
        try:
            self.logger.info(f"Cleaning up network in region {region}")
            if region in self._network_cache:
                del self._network_cache[region]
        except Exception as e:
            self.logger.error(f"Failed to cleanup network in region {region}", exc_info=True)
            raise

    async def setup_network(self, region: str) -> str:
        """Setup network in the specified region"""
        try:
            self.logger.info(f"Setting up network in region {region}")
            self.logger.debug(f"Using SDK: {self.auth.sdk}")
            sdk = self.auth.sdk

            # For now, just return a placeholder since Nebius SDK doesn't support VPC yet
            network_id = f"network-{region}"
            self.logger.info(f"Created network with ID: {network_id}")
            self.logger.debug(f"Network details: region={region}, id={network_id}")
            return network_id

        except Exception as e:
            self.logger.error(f"Failed to setup network in region {region}", exc_info=True)
            self.logger.debug(f"Error details: {str(e)}")
            raise

    async def get_network(self, region: str) -> Optional[str]:
        """Get network ID for region"""
        try:
            self.logger.info(f"Getting network for region {region}")
            
            # For now return cached network ID if exists
            if region in self._network_cache:
                return self._network_cache[region]
            
            # Create new network ID and cache it
            network_id = f"network-{region}"
            self._network_cache[region] = network_id
            
            self.logger.info(f"Created network with ID: {network_id}")
            return network_id

        except Exception as e:
            self.logger.error(f"Failed to get network for region {region}", exc_info=True)
            raise

    async def get_subnet(self, region: str) -> str:
        """Get subnet in network"""
        try:
            self.logger.info(f"Getting subnet for region {region}")
            
            # Get subnet ID from environment variable
            subnet_id = os.getenv("NEBIUS_SUBNET_ID")
            if not subnet_id:
                raise ValueError("NEBIUS_SUBNET_ID environment variable not set")
            
            self.logger.info(f"Using subnet with ID: {subnet_id}")
            return subnet_id

        except Exception as e:
            self.logger.error(f"Failed to get subnet in region {region}", exc_info=True)
            raise

    async def cleanup_network(self, region: str):
        """Cleanup network resources"""
        try:
            self.logger.info(f"Cleaning up network in region {region}")
            self.logger.debug(f"Using SDK: {self.auth.sdk}")
            # Nothing to clean up yet since we're not creating actual resources
            self.logger.info("No actual resources to clean up")
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup network in region {region}", exc_info=True)
            self.logger.debug(f"Error details: {str(e)}")

    def add_firewall_rules(self, vpc_id: str, ip_ranges: List[str], port: int = 22):
        """Add firewall rules for SSH access"""
        # TODO: Implement firewall rules when supported by Nebius SDK
        self.logger.warning("Firewall rules not yet supported in Nebius SDK")
        pass 