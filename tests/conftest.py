import pytest
import warnings
import os


def pytest_configure(config):
    """Configure pytest - add custom markers and handle warnings"""
    # Register custom markers
    config.addinivalue_line(
        "markers", "requires_nebius: mark test as requiring Nebius credentials"
    )
    
    # Filter warnings
    warnings.filterwarnings(
        "ignore", 
        message="datetime.datetime.utcnow\\(\\) is deprecated.*",
        module="botocore.*"
    )


@pytest.fixture(autouse=True)
def ignore_botocore_warnings():
    """Fixture to ignore specific botocore warnings during tests"""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            module="botocore"
        )
        yield 


@pytest.fixture(scope="session")
def real_credentials():
    """Get real Nebius credentials from environment variable or file"""
    # First, try to get from environment variable
    creds_file = os.getenv("NEBIUS_CREDENTIALS_FILE")
    
    if not creds_file:
        # Try default locations
        default_locations = [
            os.path.expanduser("~/.nebius/credentials.json"),
            os.path.expanduser("~/.config/nebius/credentials.json"),
            "/etc/nebius/credentials.json",
        ]
        
        for location in default_locations:
            if os.path.exists(location):
                creds_file = location
                break
    
    if not creds_file or not os.path.exists(creds_file):
        pytest.skip("No Nebius credentials found")
    
    return {
        "credentials_file": creds_file
    }


@pytest.fixture
async def real_auth(real_credentials):
    """Create a real authentication object with cleanup"""
    auth = NebiusAuthentication(real_credentials)
    yield auth
    await auth.cleanup() 