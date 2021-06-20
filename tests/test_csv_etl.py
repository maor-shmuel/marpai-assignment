import json
import os

from ..csv_etl import CsvEtl


def test_csv_etl():
    with open('tests/config/config.json') as config_file:
        config_data = json.load(config_file)

    etl = CsvEtl(config_data)
    while not etl.last_iteration:
        etl.extract()
        etl.transform()
        etl.load()

    # extracting data from db
    res = etl.db_con.cursor().execute("""
                    SELECT FactDiagnosis.diagnosis_id,
                           FactDiagnosis.member_first_name,
                           FactDiagnosis.member_last_name, 
                           DimProcedure.procedure_description, 
                           DimProvider.provider_org_name, 
                           DimProvider.provider_last_name 
                    FROM FactDiagnosis
                    LEFT JOIN DimProcedure
                    ON FactDiagnosis.procedure_code = DimProcedure.procedure_code 
                    LEFT JOIN DimProvider 
                    ON FactDiagnosis.provider_id = DimProvider.provider_id
                """)
    db_data = res.fetchall()
    # testing expected count of rows and data in db
    assert len(db_data) == 6

    assert str(db_data[0]) == "(1, 'Good', 'Date', 'Routine ophthalmological exa', None, 'TROTCHIE')"
    assert str(db_data[1]) == "(2, 'Good', 'DiagnosisCode_5', 'BIOPSY ABDOMINAL MASS', None, 'HARDEBECK')"
    assert str(db_data[2]) == "(3, 'Good', 'DiagnosisCode_4', 'BIOPSY ABDOMINAL MASS', None, 'HARDEBECK')"
    assert str(db_data[3]) == "(4, 'Good', 'DiagnosisCode_3', 'BIOPSY ABDOMINAL MASS', None, 'HARDEBECK')"
    assert str(db_data[4]) == "(5, 'Good', 'DiagnosisCode_2', 'BIOPSY ABDOMINAL MASS', None, 'HARDEBECK')"
    assert str(db_data[5]) == "(6, 'Good', 'DiagnosisCode_1', 'BIOPSY ABDOMINAL MASS', None, 'HARDEBECK')"

    # testing expected count of rows and data in errors file
    for f in os.listdir('tests/'):
        if f.startswith('errors'):
            with open('tests/' + f) as errors_file:
                errors_data = errors_file.readlines()
            break
    assert len(errors_data) == 5
    assert str(errors_data[1]).strip() == '0,"{row: 0, column: ""service_date""}: ""23 MAOR 20"" service_date value is not a valid date"'
    assert str(errors_data[2]).strip() == '1,"{row: 2, column: ""diagnosis_code""}: ""S52515S"" does not match the pattern ""^[a-zA-Z0-9]{1,5}$"""'
    assert str(errors_data[3]).strip() == '2,"{row: 8, column: ""diagnosis_code""}: ""!@#$@"" does not match the pattern ""^[a-zA-Z0-9]{1,5}$"""'
    assert str(errors_data[4]).strip() == '3,"{row: 9, column: ""procedure_code""}: ""491801234"" does not match the pattern ""^[0-9]{1,5}$|^[a-zA-Z][0-9]{1,4}$|^[0-9]{1,4}[a-zA-Z]$"""'

    etl.db_con.close()