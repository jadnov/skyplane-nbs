from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.aws.aws_cloud_provider import AWSCloudProvider
from skyplane.compute.aws.aws_server import AWSServer
from skyplane.compute.ibmcloud.ibmcloud_server import IBMCloudServer
from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.azure.azure_cloud_provider import AzureCloudProvider
from skyplane.compute.azure.azure_server import AzureServer
from skyplane.compute.cloud_provider import CloudProvider
from skyplane.compute.gcp.gcp_auth import GCPAuthentication
from skyplane.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skyplane.compute.gcp.gcp_server import GCPServer
from skyplane.compute.ibmcloud.ibmcloud_auth import IBMCloudAuthentication
from skyplane.compute.ibmcloud.ibmcloud_provider import IBMCloudProvider
from skyplane.compute.server import Server, ServerState
from skyplane.compute.scp.scp_auth import SCPAuthentication
from skyplane.compute.scp.scp_cloud_provider import SCPCloudProvider
from skyplane.compute.scp.scp_server import SCPServer

# Add Nebius imports
try:
    print("Attempting to import Nebius modules...")  # Debug line
    from skyplane.compute.nebius.nebius_auth import NebiusAuthentication
    from skyplane.compute.nebius.nebius_cloud_provider import NebiusCloudProvider
    from skyplane.compute.nebius.nebius_server import NebiusServer
    print("Successfully imported Nebius modules")  # Debug line
    HAVE_NEBIUS = True
except ImportError as e:
    print(f"Failed to import Nebius modules: {e}")  # Debug line
    HAVE_NEBIUS = False
    NebiusCloudProvider = None
    NebiusServer = None

__all__ = [
    "CloudProvider",
    "Server",
    "ServerState",
    "AWSAuthentication",
    "AWSCloudProvider",
    "AWSServer",
    "IBMCloudServer",
    "AzureAuthentication",
    "AzureCloudProvider",
    "AzureServer",
    "GCPAuthentication",
    "GCPCloudProvider",
    "IBMCloudAuthentication",
    "IBMCloudProvider",
    "GCPServer",
    "SCPAuthentication",
    "SCPCloudProvider",
    "SCPServer",
]

if HAVE_NEBIUS:
    __all__ += ["NebiusAuthentication", "NebiusCloudProvider", "NebiusServer"]
