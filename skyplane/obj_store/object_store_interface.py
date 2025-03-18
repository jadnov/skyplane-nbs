from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Iterator
from skyplane.utils import logger
from datetime import datetime, UTC


@dataclass
class ObjectStoreObject:
    """Defines object in object store."""

    key: str
    provider: Optional[str] = None
    bucket: Optional[str] = None
    size: Optional[int] = None
    last_modified: Optional[datetime] = None
    mime_type: Optional[str] = None

    def __init__(self, key: str, provider: str, bucket: str, size: int, last_modified: datetime, mime_type: Optional[str] = None):
        self.key = key
        self.provider = provider
        self.bucket = bucket
        self.size = size
        # Ensure last_modified is timezone-aware
        if last_modified.tzinfo is None:
            last_modified = datetime.fromtimestamp(last_modified.timestamp(), UTC)
        self.last_modified = last_modified
        self.mime_type = mime_type

    def __repr__(self):
        return f"{self.provider}://{self.bucket}/{self.key}"

    @property
    def exists(self):
        return self.size is not None and self.last_modified is not None

    def full_path(self):
        raise NotImplementedError()


class ObjectStoreInterface:  # Changed from inheriting StorageInterface
    def set_requester_bool(self, requester: bool):
        return

    def get_obj_size(self, obj_name) -> int:
        raise NotImplementedError()

    def get_obj_last_modified(self, obj_name):
        raise NotImplementedError()

    def get_obj_mime_type(self, obj_name):
        raise NotImplementedError()

    def download_object(
        self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None, write_at_offset=False, generate_md5: bool = False
    ) -> Tuple[Optional[str], Optional[bytes]]:
        """
        Downloads an object from the bucket to a local file.

        :param src_object_name: The object key in the source bucket.
        :param dst_file_path: The path to the file to write the object to
        :param offset_bytes: The offset in bytes from the start of the object to begin the download.
        If None, the download starts from the beginning of the object.
        :param size_bytes: The number of bytes to download. If None, the download will download the entire object.
        :param write_at_offset: If True, the file will be written at the offset specified by
        offset_bytes. If False, the file will be overwritten.
        :param generate_md5: If True, the MD5 hash of downloaded data will be returned.
        """
        raise NotImplementedError()

    def upload_object(
        self,
        src_file_path,
        dst_object_name,
        part_number=None,
        upload_id=None,
        check_md5: Optional[bytes] = None,
        mime_type: Optional[str] = None,
    ):
        """
        Uploads a file to the specified object

        :param src_file_path: The path to the file you want to upload,
        :param dst_object_name: The destination key of the object to be uploaded.
        :param part_number: For multipart uploads, the part number to upload.
        :param upload_id: For multipart uploads, the upload ID for the whole file to upload to.
        :param check_md5: The MD5 checksum of the file. If this is provided, the server will check the
        MD5 checksum of the file and raise ObjectStoreChecksumMismatchException if it doesn't match.
        """
        raise NotImplementedError()

    def delete_objects(self, keys: List[str]):
        raise NotImplementedError()

    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        raise ValueError("Multipart uploads not supported")

    def complete_multipart_upload(self, dst_object_name: str, upload_id: str, metadata: Optional[Any] = None) -> None:
        raise ValueError("Multipart uploads not supported")

    # Add methods from StorageInterface
    def bucket(self) -> str:
        return self.bucket_name

    @property
    def provider(self) -> str:
        raise NotImplementedError()

    def region_tag(self) -> str:
        raise NotImplementedError()

    def path(self) -> str:
        raise NotImplementedError()

    def create_bucket(self, region_tag: str):
        raise NotImplementedError()

    def delete_bucket(self):
        raise NotImplementedError()

    def bucket_exists(self) -> bool:
        raise NotImplementedError()

    def exists(self, obj_name: str) -> bool:
        raise NotImplementedError()

    def list_objects(self, prefix="") -> Iterator[Any]:
        raise NotImplementedError()

    @staticmethod
    def create(region_tag: str, bucket: str):
        # Moved imports inside method to avoid circular imports
        if region_tag.startswith("aws"):
            from skyplane.obj_store.s3_interface import S3Interface
            return S3Interface(bucket)
        elif region_tag.startswith("gcp"):
            from skyplane.obj_store.gcs_interface import GCSInterface
            return GCSInterface(bucket)
        elif region_tag.startswith("azure"):
            from skyplane.obj_store.azure_blob_interface import AzureBlobInterface
            storage_account, container = bucket.split("/", 1)
            return AzureBlobInterface(storage_account, container)
        elif region_tag.startswith("ibmcloud"):
            from skyplane.obj_store.cos_interface import COSInterface
            return COSInterface(bucket, region_tag)
        elif region_tag.startswith("hdfs"):
            from skyplane.obj_store.hdfs_interface import HDFSInterface
            logger.fs.debug(f"attempting to create hdfs bucket {bucket}")
            return HDFSInterface(host=bucket)
        elif region_tag.startswith("scp"):
            from skyplane.obj_store.scp_interface import SCPInterface
            return SCPInterface(bucket)
        elif region_tag.startswith("local"):
            from skyplane.obj_store.posix_file_interface import POSIXInterface
            return POSIXInterface(bucket)
        elif region_tag.startswith("cloudflare"):
            from skyplane.obj_store.r2_interface import R2Interface
            account, bucket = bucket.split("/", 1)
            return R2Interface(account, bucket)
        elif region_tag.startswith("nebius"):
            from skyplane.obj_store.nebius_interface import NebiusInterface
            return NebiusInterface(bucket)
        else:
            raise ValueError(f"Invalid region_tag {region_tag} - could not create interface")

# Re-export ObjectStoreInterface
__all__ = ['ObjectStoreInterface']
