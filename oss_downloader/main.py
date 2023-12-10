import click

from oss_download_manager import OssDownloadManager

oss_download_manager = OssDownloadManager()


@click.group()
def cli():
    pass


@cli.command()
def init_db():
    click.echo("Init database and load file info")
    oss_download_manager.init_db()


@cli.command()
def save_file():
    click.echo("Start save files to local")
    oss_download_manager.process_download()


if __name__ == "__main__":
    cli()
