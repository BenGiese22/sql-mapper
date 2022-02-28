import click
from file_search import FileSearch
from pathlib import Path

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
def search_file(file_path):
    """Simple program that searches file for insert into and froms."""
    click.echo(f"Searching File: {click.format_filename(file_path)}")
    file_search = FileSearch()
    search_results = file_search.search_file_for_insert_into_from(Path(file_path))
    click.echo(search_results)

# TODO run over directory
    # TODO handle subdirectories
# TODO run over file



if __name__ == '__main__':
    search_file()
