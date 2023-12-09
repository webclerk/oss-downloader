import os

import oss2
from dotenv import load_dotenv
import uuid_utils as uuid
from loguru import logger
from oss2 import Bucket
from oss2.models import ListObjectsResult

from db_helper import DBHelper


def prepare_oss_bucket() -> oss2.Bucket:
    """prepare bucket via env"""
    endpoint = os.environ.get('ENDPOINT')
    access_key_id = os.environ.get('ACCESS_KEY_ID')
    access_key_secret = os.environ.get('ACCESS_KEY_SECRET')
    bucket_name = os.environ.get('BUCKET_NAME')
    oss_auth = oss2.Auth(access_key_id=access_key_id,
                         access_key_secret=access_key_secret)
    return oss2.Bucket(oss_auth, endpoint, bucket_name)


def prepare_bucket_file_info(process_bucket: Bucket, db_helper: DBHelper) -> None:
    """save bucket file info to db"""
    truncated = True
    max_keys = 1000
    next_marker = ""
    delimiter = "/"
    obj_count = 0
    it_count = 0
    while truncated:
        it_count += 1
        logger.debug(f"fetch {it_count} times")
        fetch_result: ListObjectsResult = process_bucket.list_objects(
            delimiter=delimiter, marker=next_marker, max_keys=max_keys)
        logger.debug(fetch_result.object_list)
        for fetch_object in fetch_result.object_list:
            db_helper.insert_file_info(original_name=fetch_object.key,
                                       shorter_name=str(uuid.uuid7()))
            obj_count += 1
        next_marker = fetch_result.next_marker
        truncated = fetch_result.is_truncated
        # truncated = False
    logger.debug(f"total {obj_count} files")


if __name__ == "__main__":
    load_dotenv()
    bucket = prepare_oss_bucket()
    db = DBHelper()
    db.clear_all_data()
    prepare_bucket_file_info(process_bucket=bucket, db_helper=db)
