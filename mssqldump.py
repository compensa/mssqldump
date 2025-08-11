import argparse
import datetime
import sys

import pyodbc


def mssqldump(database: str,
              user: str = "SA",
              password: str = "",
              host: str = "localhost",
              path: str = "",
              tables: list[str] | None = None,
              no_data: bool = False,
              no_create_info: bool = False,
              no_indices: bool = False,
              add_drop_table: bool = False):

    path = '\\' + path if path else ''
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host}{path};DATABASE={database};UID={user};PWD={password}'

    conn = pyodbc.connect(conn_str)

    tables = tables or list_tables(conn)

    for table in tables:
        dump_table(conn, table, no_data, no_create_info, no_indices,
                   add_drop_table)

    conn.close()


def list_tables(conn):
    query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';"
    cursor = conn.cursor()
    cursor.execute(query)

    tables = cursor.fetchall()

    return [table[0] for table in tables]


def dump_indices(conn, table_name):
    cursor = conn.cursor()
    # Get the indices
    cursor.execute(f"""
        SELECT
            i.name AS IndexName,
            i.type_desc AS IndexType,
            i.is_primary_key AS IsPrimaryKey,
            c.name AS ColumnName
        FROM
            sys.indexes AS i
        JOIN
            sys.index_columns AS ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN
            sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE
            i.object_id = OBJECT_ID('{table_name}')
        ORDER BY
            i.name, ic.key_ordinal
    """)

    indices = cursor.fetchall()

    # Format the indices
    index_statements = {}
    pks = {}
    for index in indices:
        index_name = index.IndexName
        column_name = index.ColumnName
        if index.IsPrimaryKey:
            if index_name not in pks:
                pks[index_name] = []
            pks[index_name].append(column_name)
        else:
            if index_name not in index_statements:
                index_statements[index_name] = []
            index_statements[index_name].append(column_name)

    # Construct the primary key creation statements
    for index_name, columns in pks.items():
        columns_list = ', '.join(columns)
        print(
            f"ALTER TABLE {table_name} ADD CONSTRAINT {index_name} PRIMARY KEY ({columns_list});")

    # Construct the index creation statements
    for index_name, columns in index_statements.items():
        columns_list = ', '.join(columns)
        print(f"CREATE INDEX {index_name} ON {table_name} ({columns_list});")


def dump_table(conn, table_name, no_data, no_create_info, no_indices,
               add_drop_table):
    if add_drop_table:
        print(f"DROP TABLE IF EXISTS {table_name};\n\n")

    schema_query = f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    cursor = conn.cursor()
    cursor.execute(schema_query)

    create_table_query = f"CREATE TABLE {table_name} (\n"
    columns = []
    for row in cursor:
        column_name = row[0]
        columns.append(column_name)
        if no_create_info:
            continue
        data_type = row[1]
        is_nullable = row[2]
        character_max_length = row[3]
        column_definition = f"    {column_name} {data_type}"
        if data_type in ['varchar', 'char', 'nvarchar', 'nchar'] and character_max_length not in [None, -1]:
            column_definition += f"({int(character_max_length)})"
        if is_nullable == 'NO':
            column_definition += " NOT NULL"
        create_table_query += column_definition + ",\n"

    if not no_create_info:
        print(create_table_query.rstrip(',\n') + "\n);\n\n")

    if not no_indices:
        dump_indices(conn, table_name)

    if no_data:
        return
    # Dump data as a single SQL INSERT statement
    data_query = f'SELECT * FROM {table_name}'
    cursor = conn.cursor()
    cursor.execute(data_query)

    def repl(value):
        if (isinstance(value, datetime.datetime)):
            value = value.strftime("%Y-%m-%d %H:%M:%S") + \
                f'.{value.microsecond // 1000:03}'
        else:
            value = str(value)
        return value.replace('\'', '\'\'')

    i = 0
    insert_statement = ""
    for row in cursor:
        if i % 1000 == 0:
            insert_statement = f"; INSERT INTO {table_name} ({', '.join(columns)}) VALUES \n"  # noqa
        else:
            insert_statement = ", "
        insert_statement += "("
        for value in row:
            insert_statement += \
                (f"'{repl(value)}'" if value is not None else 'NULL') + ", "
        insert_statement = insert_statement[:-2] + ")\n"
        print(insert_statement)
        i += 1

    cursor.close()

    print(";\n")


def main():
    parser = argparse.ArgumentParser(
        description='A Python mirror of mysqldump arguments.',
        add_help=False)

    # General options
    parser.add_argument('--help', action='help',
                        help='Display this help message and exit.')
    parser.add_argument('-B', '--database', type=str,
                        required=True, help='The database to dump.')
    parser.add_argument('-u', '--user', default='SA',
                        help='The MySQL user to connect as.')
    parser.add_argument('-p', '--password', default='',
                        help='The password for the MySQL user.')
    parser.add_argument('-h', '--host', default='localhost',
                        help='The host to connect to (default: localhost).')
    parser.add_argument('--path', type=str, default="",
                        help='The path (named instance) to use for the connection.')
    parser.add_argument('-t', '--tables', nargs='+',
                        help='Dump several tables from the database.')
    parser.add_argument('-d', '--no-data', action='store_true',
                        help='No row information. Dump only the table structure.')
    parser.add_argument('--no-create-info', action='store_true',
                        help='No CREATE TABLE statements.')
    parser.add_argument('--no-indices', action='store_true',
                        help='No adding of PRIMARY KEYS and no CREATE INDEX statements.')
    parser.add_argument('--add-drop-table', action='store_true',
                        help='Add a DROP TABLE statement before each CREATE TABLE statement.')
    parser.add_argument('--default-character-set',
                        help='Set the default character set.')

    args = parser.parse_args()

    mssqldump(database=args.database,
              user=args.user,
              password=args.password,
              host=args.host,
              path=args.path,
              tables=args.tables,
              no_data=args.no_data,
              no_create_info=args.no_create_info,
              no_indices=args.no_indices,
              add_drop_table=args.add_drop_table)


if __name__ == '__main__':
    main()
