from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData
import csv
import requests

# TODO: get this list from configuration file
csv_files = [
    # CSV URLs here
]


def main():
    # TODO: move engine creation to another function
    # TODO: generalize this
    engine = create_engine('postgresql://user:password@host:port/database')
    meta = MetaData(engine, schema='src')

    with engine.connect() as connection:
        for url in csv_files:
            response = requests.get(url)
            # get table name from file name
            table_name = response.headers['Content-disposition'].split('=')[1].split('.')[0]
            table = None
            column_names = None
            values_to_insert = []
            text = response.iter_lines(decode_unicode=True)
            reader = csv.reader(text, delimiter=',')
            row_counter = 0
            for row in reader:
                if reader.line_num == 1:
                    print("Creating table " + table_name)
                    columns = (Column(column, String) for column in row)
                    column_names = [column_name for column_name in row]
                    table = Table(table_name, meta, *columns)
                    table.create(checkfirst=True)
                else:
                    # prepare insert statement
                    row_values = [value for value in row]
                    values_to_insert.append(dict(zip(column_names, row_values)))
                    row_counter += 1
                # insert rows once 1000 are ready
                # TODO: parameterize this value
                if row_counter % 1000 == 0 and row_counter != 0:
                    connection.execute(table.insert().values(values_to_insert))
                    values_to_insert = []
                # log every 10k rows processed
                # TODO: parameterize this value
                if row_counter % 10000 == 0:
                    print(str(row_counter) + ' rows processed')
            # if there are less than 1000 records, insert those records
            # TODO: move this logic to the previous insert
            if values_to_insert:
                connection.execute(table.insert().values(values_to_insert))
            print(str(row_counter) + ' rows processed')


if __name__ == '__main__':
    main()
