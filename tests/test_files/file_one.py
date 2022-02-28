
def main():
    sql = """
    CREATE TABLE test_table (like source_table);
    """
    sql2 = """
    INSERT INTO test_table SELECT * FROM source_table;
    """

if __name__ == "__main__":
    main()
