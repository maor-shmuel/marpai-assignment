import datetime
import pandas as pd
import sqlite3 as db
from dateutil.parser import ParserError
from pandas_schema import Column, Schema
from pandas_schema.validation import MatchesPatternValidation, CustomElementValidation


class CsvEtl:
    def __init__(self, config):
        self.input_csv_file = config['input_csv_file']
        self.number_of_rows_per_iteration = config['number_of_rows_per_iteration']
        self.offset = 0
        self.header = None
        self.last_iteration = False
        self.db_con = db.connect(config['sqlite_db_file'])
        self.df = pd.DataFrame()
        self.errors_file = 'errors_' + datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S') + '.csv'

        try:
            db_cur = self.db_con.cursor()
            with open(config['sql_script_file']) as sql_script_file:
                sql_script = sql_script_file.read()
            db_cur.executescript(sql_script)
        except Exception:
            raise

    def extract(self):
        self.df = pd.read_csv(self.input_csv_file, nrows=self.number_of_rows_per_iteration, skiprows=self.offset)
        if self.header is not None:
            self.df.columns = self.header
        if self.offset == 0:  # saving the header on the first iteration
            self.header = self.df.columns
        self.offset += self.number_of_rows_per_iteration
        if len(self.df) < self.number_of_rows_per_iteration:
            self.last_iteration = True

    def transform(self):
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

        errors = schema.validate(self.df)
        # writing errors to a csv file indicating which rows were dropped and why
        pd.DataFrame({'errors': errors}).to_csv(self.errors_file, mode='a')
        row_ids_to_filter = []
        for error in errors:
            row_ids_to_filter.append(error.row)
        self.df.drop(row_ids_to_filter, inplace=True)
        print(len(self.df))

        self.df['date_formatted'] = self.df['service_date'].apply(
            lambda x: int(pd.to_datetime(x).strftime("%Y%m%d")))

    def load(self):
        procedures_df = self.df[['procedure_code',
                                 'procedure_description']]
        procedures_df.to_sql('DimProcedure', self.db_con, if_exists='append', index=False)

        providers_df = self.df[['provider_id',
                                'provider_org_name',
                                'provider_last_name']]
        providers_df.to_sql('DimProvider', self.db_con, if_exists='append', index=False)

        diagnosis_df = self.df[['diagnosis_code',
                                'procedure_code',
                                'provider_id',
                                'member_first_name',
                                'member_last_name',
                                'diagnosis_description',
                                'date_formatted']]
        diagnosis_df.to_sql('FactDiagnosis', self.db_con, if_exists='append', index=False)

    def print_db(self):
        # fetching data from db
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
