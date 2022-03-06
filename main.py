import click
from sql_searcher import SearchPattern, SqlSearcher
from pathlib import Path
from tree_structure import TreeStructure

class InvalidPathError(RuntimeError):
    pass

@click.command()
@click.argument('input_path', type=click.Path(exists=True, file_okay=True, dir_okay=True, readable=True))
def search(input_path):
    """Simple program that searches file for insert into and froms."""
    sql_searcher = SqlSearcher()
    tree_structure = TreeStructure()
    click.echo(input_path)
    click.echo(f"Searching File: {click.format_filename(input_path)}")

    input_path = Path(input_path)
    if not input_path.exists():
        raise InvalidPathError(f"The path entered is does not exist: {str(input_path)}.")
    if input_path.is_dir():
        search_results = sql_searcher.search_directory(input_path, SearchPattern.INSERT_INTO_FROM)
    elif input_path.is_file():
        search_results = sql_searcher.search_file_for_insert_into_from(input_path)
    click.echo(search_results)
    trees = tree_structure.create_trees(search_results)
    for tree in trees:
        tree.show()


if __name__ == '__main__':
    search()
