from re import M


import re
from re import Pattern
from pathlib import Path

def search_file(file_path: Path, search_pattern: Pattern):
    search_results = []
    with file_path.open() as f:
        for line in f:
            # print(str(line))
            match = search_pattern.search(str(line))
            if match:
                from_table = match.group(2)
                to_table = match.group(1)
                search_results.append(f"From: {from_table} -> To: {to_table}")
    return search_results
            

def main():
    # create_table_pattern = re.compile("create\stable\s(\S+)", re.IGNORECASE)
    select_from_pattern = re.compile("insert\sinto\s(\S+).+from\s([A-Za-z_]+)", re.IGNORECASE)
    file_path = Path(__file__).parent.joinpath("./test_file.py")
    search_results = search_file(file_path, select_from_pattern)
    print(search_results)
        

if __name__ == "__main__":
    main()
