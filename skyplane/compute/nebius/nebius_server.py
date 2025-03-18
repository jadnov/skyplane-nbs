from typing import Optional, Dict
from skyplane.compute.server import Server, ServerState
from skyplane.compute.nebius.nebius_auth import NebiusAuthentication, HAVE_NEBIUS_SDK
from skyplane.utils import logger
from nebius.api.nebius.compute.v1 import InstanceServiceClient
from skyplane.compute import CloudProvider

if not HAVE_NEBIUS_SDK:
    raise ImportError(
        "Nebius SDK is not installed. Please install skyplane with "
        "'pip install skyplane[nebius]' or 'pip install nebius'"
    )

class NebiusServer(Server):
    def __init__(self, instance_id: str, name: str, region: str, cloud_provider: Optional[CloudProvider] = None):
        """Initialize NebiusServer instance"""
        super().__init__(name=name, region=region)
        self.instance_id = instance_id
        self.cloud_provider = cloud_provider
        self.auth = None
        self._instance = None

    async def _get_instance(self):
        """Get instance details from Nebius"""
        if not self._instance:
            sdk = self.auth.sdk
            compute_service = InstanceServiceClient(sdk)
            self._instance = await compute_service.get(self.instance_id)
        return self._instance

    def __await__(self):
        """Make the server awaitable"""
        async def _init():
            await self._get_instance()
            return self
        return _init().__await__()

    async def get_state(self) -> ServerState:
        """Get current instance state"""
        try:
            instance = await self._get_instance()
            # Map Nebius states to ServerState
            state_map = {
                "RUNNING": ServerState.RUNNING,
                "STOPPED": ServerState.STOPPED,
                "STARTING": ServerState.PENDING,
                "STOPPING": ServerState.STOPPING,
                "TERMINATED": ServerState.TERMINATED,
                "ERROR": ServerState.ERROR
            }
            return state_map.get(instance.status, ServerState.UNKNOWN)
        except Exception as e:
            logger.error(f"Failed to get instance state: {e}")
            raise

    async def get_private_ip(self) -> str:
        """Get private IP address"""
        try:
            instance = await self._get_instance()
            return instance.network_interfaces[0].primary_v4_address.address
        except Exception as e:
            logger.error(f"Failed to get private IP: {e}")
            raise

    async def get_public_ip(self) -> Optional[str]:
        """Get instance public IP"""
        try:
            instance = await self._get_instance()
            for interface in instance.network_interfaces:
                if interface.primary_v4_address and interface.primary_v4_address.one_to_one_nat:
                    return interface.primary_v4_address.one_to_one_nat.address
            return None
        except Exception as e:
            logger.error(f"Failed to get public IP: {e}")
            raise

    async def terminate(self):
        """Terminate the instance"""
        try:
            instance = await self._get_instance()
            operation = await instance.delete()
            await operation.result()
        except Exception as e:
            logger.error(f"Failed to terminate instance: {e}")
            raise

    async def get_tags(self) -> Dict[str, str]:
        """Get instance tags"""
        try:
            instance = await self._get_instance()
            return instance.labels or {}
        except Exception as e:
            logger.error(f"Failed to get tags: {e}")
            return {}

    async def wait_for_state(self, target_state: ServerState, timeout: int = 300) -> ServerState:
        """Wait for instance to reach target state"""
        import asyncio
        start_time = asyncio.get_event_loop().time()
        
        while True:
            current_state = await self.get_state()
            if current_state == target_state:
                return current_state
                
            if asyncio.get_event_loop().time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for state {target_state}")
                
            await asyncio.sleep(5)  # Poll every 5 seconds 