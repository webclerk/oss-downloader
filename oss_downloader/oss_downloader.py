import os

import click
import oss2
from dotenv import load_dotenv
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


def load_bucket_file_info(process_bucket: Bucket, db_helper: DBHelper) -> None:
    """save bucket file info to db"""
    truncated = True
    max_keys = 1000
    next_marker = ""
    delimiter = "/"
    obj_count = 0
    it_count = 0
    while truncated:
        it_count += 1
        fetch_result: ListObjectsResult = process_bucket.list_objects(
            delimiter=delimiter, marker=next_marker, max_keys=max_keys)
        # logger.debug(fetch_result.object_list)
        db_helper.insert_file_info_list(
            original_name_list=[fetch_object.key for fetch_object in fetch_result.object_list])
        obj_count += len(fetch_result.object_list)
        logger.debug(f"fetch {it_count} times, got {obj_count} files")
        next_marker = fetch_result.next_marker
        truncated = fetch_result.is_truncated
        # truncated = False
    logger.debug(f"total {obj_count} files")


def save_file_to_local(process_bucket: Bucket, db_helper: DBHelper) -> None:
    """save file to local"""
    pass


@click.group()
def cli():
    pass


@cli.command()
def init_db():
    click.echo("Init database and load file info")
    db.clear_all_data()
    load_bucket_file_info(process_bucket=bucket, db_helper=db)


@cli.command()
def save_file():
    click.echo("Start save files to local")


if __name__ == "__main__":
    load_dotenv()
    bucket = prepare_oss_bucket()
    db = DBHelper()
    cli()
