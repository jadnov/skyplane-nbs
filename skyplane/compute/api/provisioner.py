from skyplane.compute import HAVE_NEBIUS

def get_cloud_provider(provider_name: str, *args, **kwargs):
    """Get cloud provider instance"""
    if provider_name == "nebius":
        if not HAVE_NEBIUS:
            raise ImportError(
                "Nebius support not installed. Please install skyplane with "
                "'pip install skyplane[nebius]' or 'pip install nebius'"
            )
        from skyplane.compute.nebius.nebius_cloud_provider import NebiusCloudProvider
        return NebiusCloudProvider(*args, **kwargs)
    # ... rest of the providers ... 