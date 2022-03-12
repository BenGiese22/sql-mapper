import click
from sql_searcher import SearchPattern, SqlSearcher
from pathlib import Path
from tree_structure import TreeStructure

class InvalidPathError(RuntimeError):
    pass

@click.command()
@click.argument('search_type', type=click.Choice(['create', 'insert_into_from']))
@click.argument('input_path', type=click.Path(exists=True, file_okay=True, dir_okay=True, readable=True))
def search(search_type, input_path):
    """
        Simple program that searches file for insert into and froms.

        SEARCH_TYPE specifies how you want to search the desired file or directory.
        SEARCH_TYPE choices consist of: 'create' and 'insert_into_from'

        INPUT_PATH is the raw path to a file or a directory you want to search. (Must be readable)
    """
    search_pattern = SearchPattern.INSERT_INTO_FROM
    if search_type == 'create':
        search_pattern = SearchPattern.CREATED_TABLE

    sql_searcher = SqlSearcher()
    tree_structure = TreeStructure(search_pattern)
    click.echo(input_path)
    click.echo(f"Searching File: {click.format_filename(input_path)}")

    input_path = Path(input_path)
    if not input_path.exists():
        raise InvalidPathError(f"The path entered is does not exist: {str(input_path)}.")
    if input_path.is_dir():
        search_results = sql_searcher.search_directory(input_path, search_pattern)
    elif input_path.is_file():
        if search_type == 'insert_into_from':
            search_results = sql_searcher.search_file_for_insert_into_from(input_path)
        else:
            search_results = sql_searcher.search_file_for_create_table(input_path)
    click.echo(search_results)

    trees = tree_structure.create_trees(search_results)
    for tree in trees:
        tree.show()

@click.group()
def cli():
    pass

cli.add_command(search)