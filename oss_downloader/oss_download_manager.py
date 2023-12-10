import hashlib
import os
from concurrent.futures import ProcessPoolExecutor
from os.path import abspath, join
from typing import List

import oss2
from dotenv import load_dotenv
from loguru import logger

from db_helper import DBHelper


class OssDownloadManager:
    """
    Oss Download Manager is used to manage oss download process
    """

    @staticmethod
    def _prepare_oss_bucket() -> oss2.Bucket:
        """prepare bucket via env"""
        endpoint = os.environ.get('ENDPOINT')
        access_key_id = os.environ.get('ACCESS_KEY_ID')
        access_key_secret = os.environ.get('ACCESS_KEY_SECRET')
        bucket_name = os.environ.get('BUCKET_NAME')
        oss_auth = oss2.Auth(access_key_id=access_key_id,
                             access_key_secret=access_key_secret)
        return oss2.Bucket(oss_auth, endpoint, bucket_name)

    def __init__(self):
        """init db helper and bucket"""
        load_dotenv()
        self.db_helper = DBHelper()
        self.bucket = OssDownloadManager._prepare_oss_bucket()

    def _load_bucket_file_info(self) -> None:
        """save bucket file info to db"""
        truncated = True
        max_keys = 1000
        next_marker = ""
        delimiter = "/"
        obj_count = 0
        it_count = 0
        while truncated:
            it_count += 1
            fetch_result: oss2.models.ListObjectsResult = self.bucket.list_objects(
                delimiter=delimiter, marker=next_marker, max_keys=max_keys)
            # logger.debug(fetch_result.object_list)
            self.db_helper.insert_file_info_list(
                original_name_list=[fetch_object.key for fetch_object in fetch_result.object_list])
            obj_count += len(fetch_result.object_list)
            logger.debug(f"fetch {it_count} times, got {obj_count} files")
            next_marker = fetch_result.next_marker
            truncated = fetch_result.is_truncated
            # truncated = False
        logger.debug(f"total {obj_count} files")

    def __init_file_save_path(self):
        """init file save path"""
        file_save_path = os.environ.get('FILE_SAVE_PATH', "./oss-data")
        file_save_dir = abspath(file_save_path)
        if not os.path.exists(file_save_dir):
            os.makedirs(file_save_dir)
        self.file_save_dir = file_save_dir

    def init_db(self):
        """init db and load bucket file info"""
        self.db_helper.clear_all_data()
        self._load_bucket_file_info()
        self.__init_file_save_path()

    @staticmethod
    def __calculate_file_md5(file_path: str) -> str:
        """Calculate the MD5 hash value of a file"""
        with open(file_path, 'rb') as file:
            md5_hash = hashlib.md5()
            for chunk in iter(lambda: file.read(4096), b''):
                md5_hash.update(chunk)
        return str(md5_hash.hexdigest()).lower()

    @staticmethod
    def __split_work_list(lst: List, size: int) -> List:
        """split list to sub list"""
        return [lst[i:i + size] for i in range(0, len(lst), size)]

    def process_download(self) -> int:
        """process downloaded file"""
        process_max_count = 1000
        processed_count = 0
        need_process_file_list = self.db_helper.get_unprocessed_file_info_list(limit=process_max_count)
        while len(need_process_file_list) > 0:
            sub_list = OssDownloadManager.__split_work_list(need_process_file_list,
                                                            size=int(process_max_count / 20))
            with ProcessPoolExecutor() as executor:
                # using mapp to process
                map_result_list = executor.map(self.__download_file, sub_list)
                for map_result in map_result_list:
                    self.db_helper.batch_update_processed_result(
                        original_name_list=map_result["original_name_list"],
                        file_md5_list=map_result["file_md5_list"])
                    processed_count = processed_count + len(need_process_file_list)
                    logger.debug(f"processed {processed_count} files")
            need_process_file_list = self.db_helper.get_unprocessed_file_info_list()
        return processed_count

    def __download_file(self, need_process_file_list: List):
        processed_original_name_list = []
        processed_file_md5 = []
        for need_process_file in need_process_file_list:
            saved_file_name = join(self.file_save_dir, need_process_file['shorter_name'])
            self.bucket.get_object_to_file(
                key=need_process_file['original_name'],
                filename=saved_file_name)
            file_md5 = OssDownloadManager.__calculate_file_md5(file_path=str(saved_file_name))
            processed_original_name_list.append(need_process_file['original_name'])
            processed_file_md5.append(file_md5)
            logger.debug(f"downloaded file {need_process_file['original_name']} with md5 {file_md5}")
        return {"original_name_list": processed_original_name_list,
                "file_md5_list": processed_file_md5}
