MARPAI HOME ASSIGNMENT

Author: Maor Shmuel

### Clarifications:

1. requirement states that Valid diagnosis code requires: "its length won’t be longer than 5 characters",
   
   And for Valid procedure code: "is 5 characters length as well".
   
   it's a bit unclear if procedure code should always be 5 chars long or up to 5 chars long.
   
   I treated both the same allowing up to 5 chars for each
   

2. it was not stated exactly what to do with rows that did not comply to requirements
   some options i considered:
   - save them in the db with a flag stating data did not pass the validation
   - correct the data (only possible if a fix algorithm is supplied and is relibel)
     for example: take the first 5 chars of the provider id in case its longer
   - output a separate report with a description of the issues found during validation.

  I chose the third option and each execution of the module will generate a new erros_timestamp.csv
  holding data about the rows that did not pass validation and the reason it failed validation.
  this is just to demonstrate the ability to identify the relevant rows and the option to choose how
  to handle it in the code in a later phase if required.
 
3. I chose to implement the read operation in chunks - according to a config param, only a fixed number 
   of rows are fetched from the input csv and ingested into the db, until all the data is fetched
   i did this to better control the resources used during execution, since implementation is done with
   pandas (not spark, dask, or any other distributed processing cluster), an input files with too much
   rows/data will cause "out of memory" if i'll try to save all the data in a single data frame in memory

### Data model:

2 relevant models i would consider are:
1. star schema model
2. "flat" table model

for this exercise - i'll be using the star schema

#### Star Schema model:


2 dimensional tables for procedures and providers and 1 fact table to hold diagnosis data.
I didn't include a dimensional table for members since the only data available for members is
first and last name which is really thin and also not unique (2 different people might have the same
first, last names), so these are addressed as diagnosis specific.
if for example members data in the input included also: id, address, phone-number, etc..
then it was a clear dimensional table

``` sql
CREATE TABLE IF NOT EXISTS DimProcedure(
    procedure_code          TEXT PRIMARY KEY ON CONFLICT IGNORE,
    procedure_description   TEXT,
    UNIQUE (procedure_code, procedure_description) ON CONFLICT IGNORE
);


CREATE TABLE IF NOT EXISTS DimProvider(
    provider_id         TEXT PRIMARY KEY ON CONFLICT IGNORE,
    provider_org_name   TEXT,
    provider_last_name  TEXT,
    UNIQUE (provider_id, provider_org_name, provider_last_name) ON CONFLICT IGNORE

);

CREATE TABLE IF NOT EXISTS FactDiagnosis(
    diagnosis_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    diagnosis_code          TEXT,
    procedure_code          TEXT,
    provider_id             TEXT,
    member_first_name       TEXT,
    member_last_name        TEXT,
    diagnosis_description   TEXT,
    date_formatted          INTEGER,
    ingest_timestamp        TIMESTAMP DEFAULT current_timestamp,
    FOREIGN KEY (procedure_code)
      REFERENCES DimProcedure (procedure_code),
    FOREIGN KEY (provider_id)
      REFERENCES DimProvider (provider_id)
);
```

#### example query to get all the data:

``` sql
SELECT FactDiagnosis.*, 
       DimProcedure.procedure_description, 
       DimProvider.provider_org_name, 
       DimProvider.provider_last_name 
FROM FactDiagnosis
LEFT JOIN DimProcedure
ON FactDiagnosis.procedure_code = DimProcedure.procedure_code 
LEFT JOIN DimProvider 
ON FactDiagnosis.provider_id = DimProvider.provider_id
```

### "flat" table model:

assuming:

1. diagnosis is the main interest (ie there is no much value in querying data about a procedure/provider
   without the diagnosis context
2. use cases dont require the ability to answer questions about what did not happen
   (for example: what procedure was never performed)
   in the exercise scope we dont get providers/procedures data separately, but in theory its possible

``` sql
fact_diagnosis:
  diagnosis_id               (primary key)
  member_first_name
  member_last_name
  procedure_code
  procedure_description
  provider_id
  provider_org_name
  provider_last_name
  diagnosis_code
  diagnosis_description
  date_formatted
  ingest_timestamp
```
a flat table might mean some duplication of specific columns for different rows but will
make maintenance and ingest processes simpler

#### HOW TO RUN:
``` shell
virtualenv .venv
. .venv/bin/activate
pip install -r requirements.txt
python marpai_task.py

# at the end of the run - db is printed to stdout
# and available in the sqlite3 database file
```

#### Code structure:

- marpai_task.py holds the main function
- csv_etl.py holds the class CsvEtl which implements extract, transform, load
- config/config.json holds some required configuration for tunning:
    ``` json
    {
      "number_of_rows_per_iteration": 100,
      "input_csv_file": "home_excercise_data.csv",
      "sqlite_db_file": "./sqlite.db",
      "sql_script_file": "sql/create_tables.sql"
    }
    ```
- sql/create_tables.sql - holds the sql statements to create the tables
  I also place drop statements for this exercise so every run starts with a fresh db
 

#### Tests:
To execute tests:
``` shell
pip install pytest
pytest tests/test_csv_etl.py
```
 
