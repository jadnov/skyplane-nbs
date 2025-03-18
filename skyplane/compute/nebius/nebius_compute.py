from skyplane.utils import logger
from nebius.api.nebius.common.v1 import ResourceMetadata
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
    SourceImageFamily,
    GetInstanceRequest,
    DeleteInstanceRequest,
)

class NebiusCompute:
    def __init__(self, auth):
        self.auth = auth
        self.disk_service = DiskServiceClient(auth.sdk)
        self.compute_service = InstanceServiceClient(auth.sdk)
        
    async def create_disk(self, project_id, name, size_gb, image_family):
        """Create a new disk"""
        logger.info(f"Creating disk {name} in project {project_id}")
        
        request = CreateDiskRequest(
            metadata=ResourceMetadata(
                parent_id=project_id,
                name=name
            ),
            spec=DiskSpec(
                size_gibibytes=size_gb,
                type=DiskSpec.DiskType.NETWORK_HDD,
                source_image_family=SourceImageFamily(
                    image_family=image_family
                )
            )
        )
        
        operation = await self.disk_service.create(request)
        await operation.wait()
        return operation.resource_id
        
    async def create_instance(self, project_id, name, disk_id, subnet_id):
        """Create a new instance"""
        logger.info(f"Creating instance {name} in project {project_id}")
        
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
                        subnet_id=subnet_id,
                        ip_address=IPAddress(),
                        public_ip_address=PublicIPAddress(static=True)
                    )
                ]
            )
        )
        
        operation = await self.compute_service.create(request)
        await operation.wait()
        return operation.resource_id
        
    async def get_instance_status(self, instance_id):
        """Get instance status"""
        request = GetInstanceRequest(id=instance_id)
        instance = await self.compute_service.get(request)
        return instance.status.state
        
    async def delete_instance(self, instance_id):
        """Delete an instance"""
        logger.info(f"Deleting instance {instance_id}")
        request = DeleteInstanceRequest(id=instance_id)
        operation = await self.compute_service.delete(request)
        await operation.wait()
        
    async def delete_disk(self, disk_id):
        """Delete a disk"""
        logger.info(f"Deleting disk {disk_id}")
        request = DeleteDiskRequest(id=disk_id)
        operation = await self.disk_service.delete(request)
        await operation.wait() 