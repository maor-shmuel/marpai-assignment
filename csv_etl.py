import datetime
import pandas as pd
import sqlite3 as db
from dateutil.parser import ParserError
from pandas_schema import Column, Schema
from pandas_schema.validation import MatchesPatternValidation, CustomElementValidation


class CsvEtl:
    """
    class to manage the ETL
    """
    def __init__(self, config):
        """
        inits a class object with config params

        number_of_rows_per_iteration, offset, header - used to read chunks of data from the source csv file
        db_con - sqlite3 connection

        df - holds the temporary data frame (for each chunk)

        errors_file = path to validation errors file
        sample from file:
        0,{row: 1, column: ""diagnosis_code""}: ""S52515S"" does not match the pattern ""^[a-zA-Z0-9]{1,5}$

        in this example diagnosis_code failed validation because its length is 7 and the max allowed length is 5
        """
        self.input_csv_file = config['input_csv_file']
        self.number_of_rows_per_iteration = config['number_of_rows_per_iteration']
        self.offset = 0
        self.header = None
        self.last_iteration = False
        self.db_con = db.connect(config['sqlite_db_file'])
        self.df = pd.DataFrame()
        self.errors_file = 'errors_' + datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S') + '.csv'
        # init the db - create tables
        try:
            print("initializing DB")
            db_cur = self.db_con.cursor()
            with open(config['sql_script_file']) as sql_script_file:
                sql_script = sql_script_file.read()
            db_cur.executescript(sql_script)
        except Exception:
            raise

    def extract(self):
        """
        performs a chunk read from the csv file

        :return: None
        """
        print(f'reading {self.number_of_rows_per_iteration} rows from {self.input_csv_file} starting from row {self.offset}')
        self.df = pd.read_csv(self.input_csv_file,
                              nrows=self.number_of_rows_per_iteration,
                              skiprows=self.offset)
        # using the header form the first chunk to set the data frame columns
        if self.header is not None:
            self.df.columns = self.header
        # saving the header from the data frame created from the first chunk
        if self.offset == 0:
            self.header = self.df.columns
        self.offset += self.number_of_rows_per_iteration
        # setting last_iteration once the number of rows fetched is less then the chunk size
        if len(self.df) < self.number_of_rows_per_iteration:
            self.last_iteration = True

    def transform(self):
        """
        performs data validation, cleanup and transformation
        :return: None
        """
        def date_column_validator(date_str):
            """
            validate that date values can be converted into date time objects
            :param date_str: string to validate
            :return: True is validation successfull, False if not
            """
            try:
                pd.to_datetime(date_str)
                return True
            except ParserError:
                return False

        schema = Schema([
            Column('member_first_name', []),
            Column('member_last_name', []),
            Column('diagnosis_code',
                   [MatchesPatternValidation(r'^[a-zA-Z0-9]{1,5}$')],
                   allow_empty=False),
            Column('diagnosis_description', []),
            Column('procedure_code',
                   [MatchesPatternValidation(r'^[0-9]{1,5}$|^[a-zA-Z][0-9]{1,4}$|^[0-9]{1,4}[a-zA-Z]$')],
                   allow_empty=False),
            Column('procedure_description', []),
            Column('provider_id',
                   [MatchesPatternValidation(r'^[0-9]{10}$')],
                   allow_empty=False),
            Column('provider_org_name', []),
            Column('provider_last_name', []),
            Column('service_date',
                   [CustomElementValidation(lambda date_str: date_column_validator(date_str),
                                            'service_date value is not a valid date')],
                   allow_empty=False)
        ])
        print('validating schema')
        errors = schema.validate(self.df)
        # writing errors to a csv file indicating which rows were dropped and why
        print(f'writing validation errors to: {self.errors_file}')
        pd.DataFrame({'errors': errors}).to_csv(self.errors_file, mode='a')
        # dropping rows that didn't pass validation from the data frame before writing to db
        row_ids_to_filter = []
        for error in errors:
            row_ids_to_filter.append(error.row)
        self.df.drop(row_ids_to_filter, inplace=True)
        # transform date to required format - 20210301 for example
        self.df['date_formatted'] = self.df['service_date'].apply(
            lambda x: int(pd.to_datetime(x).strftime("%Y%m%d")))

    def load(self):
        """
        loads data into the db
        :return: None
        """
        print("loading data to DB")
        # populate DimProcedure table
        procedures_df = self.df[['procedure_code',
                                 'procedure_description']]
        procedures_df.to_sql('DimProcedure', self.db_con, if_exists='append', index=False)

        # populate DimProvider table
        providers_df = self.df[['provider_id',
                                'provider_org_name',
                                'provider_last_name']]
        providers_df.to_sql('DimProvider', self.db_con, if_exists='append', index=False)

        # populate FactDiagnosis table
        diagnosis_df = self.df[['diagnosis_code',
                                'procedure_code',
                                'provider_id',
                                'member_first_name',
                                'member_last_name',
                                'diagnosis_description',
                                'date_formatted']]
        diagnosis_df.to_sql('FactDiagnosis', self.db_con, if_exists='append', index=False)
        print("done loading data to DB")

    def print_db(self):
        """
        fetching all the data form the db and printing to stdout

        :return: None
        """
        res = self.db_con.cursor().execute("""
                SELECT FactDiagnosis.*, 
                       DimProcedure.procedure_description, 
                       DimProvider.provider_org_name, 
                       DimProvider.provider_last_name 
                FROM FactDiagnosis
                LEFT JOIN DimProcedure
                ON FactDiagnosis.procedure_code = DimProcedure.procedure_code 
                LEFT JOIN DimProvider 
                ON FactDiagnosis.provider_id = DimProvider.provider_id
            """)
        for row in res.fetchall():
            print(row)
