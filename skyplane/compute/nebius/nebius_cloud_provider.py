from typing import List, Optional
from threading import BoundedSemaphore
import os
import asyncio

from skyplane.compute.cloud_provider import CloudProvider
from skyplane.compute.nebius.nebius_auth import NebiusAuthentication, HAVE_NEBIUS_SDK
from skyplane.compute.nebius.nebius_network import NebiusNetwork
from skyplane.compute.nebius.nebius_server import NebiusServer
from skyplane.utils import logger
from nebius.sdk import SDK
from nebius.api.nebius.compute.v1 import (
    InstanceServiceClient,
    DiskServiceClient,
    CreateInstanceRequest,
    CreateDiskRequest,
    DeleteDiskRequest,
    GetDiskRequest,
    ListDisksRequest,
    InstanceSpec,
    ResourcesSpec,
    NetworkInterfaceSpec,
    IPAddress,
    PublicIPAddress,
    AttachedDiskSpec,
    ExistingDisk,
    DiskSpec,
    SourceImageFamily
)
from nebius.api.nebius.common.v1 import ResourceMetadata
from skyplane.compute.server import Server, ServerState

if not HAVE_NEBIUS_SDK:
    raise ImportError(
        "Nebius SDK is not installed. Please install skyplane with "
        "'pip install skyplane[nebius]' or 'pip install nebius'"
    )

class NebiusCloudProvider(CloudProvider):
    def __init__(
        self,
        key_prefix: str = "skyplane",
        auth: Optional[NebiusAuthentication] = None,
        network: Optional[NebiusNetwork] = None,
    ):
        """Initialize Nebius cloud provider
        
        Args:
            key_prefix: Prefix for resource names
            auth: Optional authentication instance
            network: Optional network instance
        """
        super().__init__()
        self.key_prefix = key_prefix
        self.auth = auth if auth else NebiusAuthentication()
        self.network = network if network else NebiusNetwork(self.auth)
        self._sdk = self.auth.sdk
        self.provisioning_semaphore = BoundedSemaphore(16)
    
    @property
    def name(self) -> str:
        return "nebius"
    
    @staticmethod
    def region_list() -> List[str]:
        """Get list of available regions"""
        return [
            "eu-north1",
            "eu-west1"
        ]
    
    @classmethod
    def get_transfer_cost(cls, src_key, dst_key, premium_tier=True):
        """Calculate transfer costs between regions"""
        assert src_key.startswith("nebius:")
        dst_provider, dst_region = dst_key.split(":")
        # Implement cost calculation logic
        return 0.08  # Example cost per GB
    
    def setup_global(self):
        """Global provider setup"""
        logger.info("Setting up global Nebius resources")
        # Implement global setup (IAM, etc)
        pass
    
    def setup_region(self, region: str):
        """Region-specific setup"""
        logger.info(f"Setting up Nebius region {region}")
        try:
            self.network.setup_vpc(region)
        except Exception as e:
            logger.error(f"Failed to setup region {region}: {e}")
            raise
    
    async def provision_instance(self, region: str, instance_class: str, name: str) -> NebiusServer:
        """Provision a new instance"""
        try:
            project_id = os.getenv("NEBIUS_PROJECT_ID")
            if not project_id:
                raise ValueError("NEBIUS_PROJECT_ID environment variable not set")

            # Create disk service
            disk_service = DiskServiceClient(self.auth.sdk)

            # Cleanup any existing disks with the same name
            try:
                disk_list = await disk_service.list(ListDisksRequest(parent_id=project_id))
                for disk in disk_list.items:
                    if disk.metadata.name == f"{name}-disk":
                        delete_request = DeleteDiskRequest(id=disk.metadata.id)
                        delete_operation = await disk_service.delete(delete_request)
                        await delete_operation.wait()
                        logger.debug(f"Deleted existing disk: {disk.metadata.id}")
            except Exception as e:
                logger.error("Error during disk cleanup:", exc_info=True)
                pass

            # Add a delay before creating new disk
            await asyncio.sleep(5)

            # Create disk
            disk_operation = await disk_service.create(
                request=CreateDiskRequest(
                    metadata=ResourceMetadata(
                        parent_id=project_id,
                        name=f"{name}-disk"
                    ),
                    spec=DiskSpec(
                        size_gibibytes=20,
                        type=DiskSpec.DiskType.NETWORK_HDD,
                        source_image_family=SourceImageFamily(
                            image_family="ubuntu22.04-driverless"
                        )
                    )
                )
            )
            await disk_operation.wait()
            disk_id = disk_operation.resource_id

            # Create compute service
            compute = InstanceServiceClient(self.auth.sdk)

            # Create instance request
            request = CreateInstanceRequest(
                metadata=ResourceMetadata(
                    parent_id=project_id,
                    name=name
                ),
                spec=InstanceSpec(
                    resources=ResourcesSpec(
                        platform="cpu-d3",
                        preset="4vcpu-16gb"
                    ),
                    boot_disk=AttachedDiskSpec(
                        attach_mode=AttachedDiskSpec.AttachMode.READ_WRITE,
                        existing_disk=ExistingDisk(
                            id=disk_id
                        )
                    ),
                    network_interfaces=[
                        NetworkInterfaceSpec(
                            name="eth0",
                            subnet_id=os.getenv("NEBIUS_SUBNET_ID"),
                            ip_address=IPAddress(),
                            public_ip_address=PublicIPAddress(static=True)
                        )
                    ]
                )
            )

            # Create instance
            operation = await compute.create(request)
            await operation.wait()
            
            # Get instance ID from operation
            instance_id = operation.resource_id
            logger.info(f"Created instance: {instance_id}")

            return NebiusServer(
                instance_id=instance_id,
                name=name,
                region=region,
                cloud_provider=self
            )

        except Exception as e:
            logger.error(f"Failed to provision instance: {e}")
            raise 