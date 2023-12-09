import sqlalchemy as db
from datetime import datetime


class DBHelper:
    def __init__(self) -> None:
        # 创建数据库引擎
        self.engine = db.create_engine('sqlite:///file_info.db')

        # 创建元数据对象
        self.metadata = db.MetaData()

        # 创建file_info表
        self.file_info = db.Table('file_info', self.metadata,
                                  db.Column('original_name', db.String(length=4096), nullable=False),
                                  db.Column('shorter_name', db.String(length=255), nullable=False),
                                  db.Column('created_on', db.DateTime, nullable=False),
                                  db.Column('processed_on', db.DateTime)
                                  )

        # 创建数据库连接
        with self.engine.connect() as connection:
            # 创建表
            self.metadata.create_all(connection)

    def insert_file_info(self, original_name: str, shorter_name: str) -> None:
        with self.engine.connect() as connection:
            # 插入数据
            insert_statement = self.file_info.insert().values(original_name=original_name,
                                                              shorter_name=shorter_name,
                                                              created_on=datetime.now())
            connection.execute(insert_statement)
            connection.commit()

    def update_processed_on(self, original_name: str) -> None:
        with self.engine.connect() as connection:
            # 更新数据
            update_statement = self.file_info.update().where(
                self.file_info.c.original_name == original_name).values(
                processed_on=datetime.now())
            connection.execute(update_statement)
            connection.commit()

    def clear_all_data(self) -> None:
        with self.engine.connect() as connection:
            # 删除数据
            delete_statement = self.file_info.delete()
            connection.execute(delete_statement)
            connection.commit()
