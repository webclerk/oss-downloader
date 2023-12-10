import os
from datetime import datetime
from os.path import abspath
from typing import List, Dict, Any

import sqlalchemy as db
import uuid_utils as uuid
from loguru import logger
from sqlalchemy import CursorResult


class DBHelper:
    """DB Helper is used to manage database"""

    def __init__(self) -> None:
        # create database
        db_file_name = os.environ.get('DB_FILE_NAME', 'file_info.db')
        db_file_path = abspath(db_file_name)
        logger.debug(f"database path: {db_file_path}")
        self.engine = db.create_engine(f'sqlite:///{db_file_path}')

        # create metadata
        self.metadata = db.MetaData()

        # create table
        self.file_info = db.Table('file_info', self.metadata,
                                  db.Column('original_name',
                                            db.String(length=4096), nullable=False, index=True),
                                  db.Column('shorter_name', db.String(length=255), nullable=False),
                                  db.Column('created_on', db.DateTime, nullable=False),
                                  db.Column('processed_on', db.DateTime),
                                  db.Column('file_md5', db.String(length=32), nullable=True)
                                  )

        # connect to database
        with self.engine.connect() as connection:
            # create table
            self.metadata.create_all(connection)

    def insert_file_info_list(self, original_name_list: List[str]) -> None:
        """insert file info list to database"""

        def __get_file_extension(file_name: str) -> str:
            if '.' in file_name:
                return "." + file_name.split('.')[-1]
            else:
                return ""

        with self.engine.connect() as connection:
            # insert file info one by one
            for original_name in original_name_list:
                shorter_name = str(uuid.uuid7()) + __get_file_extension(file_name=original_name)
                insert_statement = self.file_info.insert().values(
                    original_name=original_name, shorter_name=shorter_name, created_on=datetime.now())
                connection.execute(insert_statement)
            connection.commit()

    @staticmethod
    def __result_proxy_to_list(execute_result: CursorResult) -> List[Dict[str, Any]]:
        """
        transfer SqlAlchemy RowResultProxy to list of dict
        :param execute_result:
        :return:
        """
        result = []
        if execute_result.returns_rows:
            cols = execute_result.cursor.description
            for row in execute_result:
                row_dict = {}
                for index, column in enumerate(cols):
                    if hasattr(column, "name"):
                        row_dict[column.name] = row[index]
                    else:
                        row_dict[column[0]] = row[index]
                result.append(row_dict)
        return result

    def get_unprocessed_file_info_list(self, limit: int = 1000) -> List[dict]:
        """get unprocessed file info list"""
        with self.engine.connect() as connection:
            # get unprocessed file info list
            select_statement = self.file_info.select().where(
                self.file_info.c.processed_on == None).order_by(
                self.file_info.c.created_on).limit(limit)
            result_proxy = connection.execute(select_statement)
            return DBHelper.__result_proxy_to_list(result_proxy)

    def batch_update_processed_result(self, original_name_list: List[str], file_md5_list: List[str]) -> None:
        """batch update processed on"""
        if len(original_name_list) != len(file_md5_list):
            raise ValueError("original_name_list and file_md5_list should have same length")
        with self.engine.connect() as connection:
            # update processed on and md5
            for original_name, file_md5 in zip(original_name_list, file_md5_list):
                update_statement = self.file_info.update().where(
                    self.file_info.c.original_name == original_name).values(
                    processed_on=datetime.now(), file_md5=file_md5)
                connection.execute(update_statement)
            connection.commit()

    # def update_processed_on(self, original_name: str, file_md5: str) -> None:
    #     with self.engine.connect() as connection:
    #         # 更新数据
    #         update_statement = self.file_info.update().where(
    #             self.file_info.c.original_name == original_name).values(
    #             processed_on=datetime.now(), file_md5=file_md5)
    #         connection.execute(update_statement)
    #         connection.commit()

    def clear_all_data(self) -> None:
        """clear all data"""
        with self.engine.connect() as connection:
            # 删除数据
            delete_statement = self.file_info.delete()
            connection.execute(delete_statement)
            connection.commit()
