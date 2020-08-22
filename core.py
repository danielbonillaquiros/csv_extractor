from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData
import csv
import requests

csv_files = [
    # CSV URLs here
]

def main():
    engine = create_engine('postgresql://user:password@host:port/database')

    meta = MetaData(engine)

    for url in csv_files:
        response = requests.get(url)
        table_name = response.headers['Content-disposition'].split('=')[1].split('.')[0]
        table = None
        text = response.iter_lines(decode_unicode=True)
        reader = csv.reader(text, delimiter=',')
        row_counter = 0
        for row in reader:
            with engine.connect() as connection:
                if reader.line_num == 1:
                    # create table
                    print("creating table")
                    columns_names = (Column(column_name, String) for column_name in row)
                    table = Table(table_name, meta, *columns_names)
                    table.create(checkfirst=True)
                else:
                    # insert row
                    row_values = [value for value in row]
                    insert_statement = table.insert().values(row_values)
                    connection.execute(insert_statement)
                    row_counter += 1
            if row_counter % 100 == 0:
                print(str(row_counter) + 'processed')


if __name__ == '__main__':
    main()
