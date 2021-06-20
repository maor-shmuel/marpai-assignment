"""
Author: Maor Shmuel

ETL:
====
EXTRACT: read data from the csv file (pandas)
TRANSFORM : validation/cleaning/requirements logic (pandas, pandas_schema)
LOAD: ingest data into the database (sqllite3)
"""
import json
from csv_etl import CsvEtl


def main():

    with open('config/config.json') as config_file:
        config_data = json.load(config_file)

    etl = CsvEtl(config_data)
    while not etl.last_iteration:
        etl.extract()
        etl.transform()
        etl.load()

    etl.print_db()
    etl.db_con.close()


if __name__ == '__main__':
    main()
