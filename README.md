# oss-downloader

As well know that Ali OSS have no tool to download the whole bucket files, so I write this tool to download the files from oss to local.

This tool has the following advantages:

1. it will save the file info to database, so you can use it to check the file info.
2. it will check the file md5, so you can use it to check the file integrity.
3. it can handle the network breakdown issue, you can continue to download the file from the break point.
4. it can using ProcessPoolExecutor to speed up download.


## usage command

### init-db

It will init database and create table, and pull oss file info list to database
```python
python oss_downloader\main.py init-db
```


### save-file

It will use the oss file info in database to download file to local, after download, it will update the database
```python
python oss_downloader\main.py save_file
```

## config

You need using `.env` to config oss info, database info, and local path info

```dotenv
ENDPOINT=""
ACCESS_KEY_ID=""
ACCESS_KEY_SECRET=""
BUCKET_NAME=""
DB_FILE_NAME=""
FILE_SAVE_PATH=""
```
