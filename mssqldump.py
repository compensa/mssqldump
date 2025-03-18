import pyodbc
import pandas as pd
import argparse

def mssqldump(database: str,
              user: str = "SA",
              password: str = "",
              host: str = "localhost",
              port: int = 1433,
              tables: list[str] | None = None,
              no_data: bool = False,
              no_create_info: bool = False,
              add_drop_table: bool = False):

    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password}'

    conn = pyodbc.connect(conn_str)

    tables = tables or list_tables(conn)

    for table in tables:
        dump_table(conn, table, no_data, no_create_info, add_drop_table)

    conn.close()


def list_tables(conn):
    query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';"
    cursor = conn.cursor()
    cursor.execute(query)

    tables = cursor.fetchall()

    return [table[0] for table in tables]


def dump_table(conn, table_name, no_data, no_create_info, add_drop_table, ):
    schema_query = f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    schema_df = pd.read_sql(schema_query, conn)

    if add_drop_table:
        print(f"DROP TABLE IF EXISTS {table_name};\n\n")

    if not no_create_info:
        create_table_query = f"CREATE TABLE {table_name} (\n"
        for _, row in schema_df.iterrows():
            column_definition = f"    {row['COLUMN_NAME']} {row['DATA_TYPE']}"
            if row['DATA_TYPE'] in ['varchar', 'char', 'nvarchar', 'nchar'] and row['CHARACTER_MAXIMUM_LENGTH'] is not None:
                column_definition += f"({int(row['CHARACTER_MAXIMUM_LENGTH'])})"
            if row['IS_NULLABLE'] == 'NO':
                column_definition += " NOT NULL"
            create_table_query += column_definition + ",\n"

        print(create_table_query.rstrip(',\n') + "\n);\n\n")

    if not no_data:
        # Dump data as a single SQL INSERT statement
        data_query = f'SELECT * FROM {table_name}'
        cursor = conn.cursor()
        cursor.execute(data_query)

        def repl(value):
            return str(value).replace('\'', '\'\'')

        i = 0
        insert_statement = ""
        for row in cursor:
            if i % 1000 == 0:
                insert_statement = f"; INSERT INTO {table_name} ({', '.join(schema_df['COLUMN_NAME'])}) VALUES \n" # noqa
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
    parser.add_argument('--help', action='help', help='Display this help message and exit.')
    parser.add_argument('-B', '--database', type=str, required=True, help='The database to dump.')
    parser.add_argument('-u', '--user', default='SA', help='The MySQL user to connect as.')
    parser.add_argument('-p', '--password', default='', help='The password for the MySQL user.')
    parser.add_argument('-h', '--host', default='localhost', help='The host to connect to (default: localhost).')
    parser.add_argument('-P', '--port', type=int, default=1433, help='The port number to use for the connection.')
    parser.add_argument('-t', '--tables', nargs='+', help='Dump several tables from the database.')
    parser.add_argument('-d', '--no-data', action='store_true', help='No row information. Dump only the table structure.')
    parser.add_argument('--no-create-info', action='store_true', help='No CREATE TABLE statements.')
    parser.add_argument('--add-drop-table', action='store_true', help='Add a DROP TABLE statement before each CREATE TABLE statement.')
    parser.add_argument('--default-character-set', help='Set the default character set.')

    args = parser.parse_args()

    mssqldump(database=args.database,
              user=args.user,
              password=args.password,
              host=args.host,
              port=args.port,
              tables=args.tables,
              no_data=args.no_data,
              no_create_info=args.no_create_info,
              add_drop_table=args.add_drop_table)


if __name__ == '__main__':
    main()
