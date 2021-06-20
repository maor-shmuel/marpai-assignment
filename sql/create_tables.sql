-- dropping the tables at each run because each run processes the same exercise csv file
DROP TABLE IF EXISTS DimProcedure;
DROP TABLE IF EXISTS DimProvider;
DROP tABLE IF EXISTS FactDiagnosis;

/*
    DimProcedure -  procedures dimensional data
                    preventing insertion of the same  procedure_code by ignoring (not failing) duplicate
                    procedure_code (PK)
                    preventing duplications with the UNIQUE statement - ignoring duplications on INSERTS
                    this might not be the optimal way to deal with duplications but looks reasonable in
                    the exercise scope
*/
CREATE TABLE IF NOT EXISTS DimProcedure(
    procedure_code          TEXT PRIMARY KEY ON CONFLICT IGNORE,
    procedure_description   TEXT,
    UNIQUE (procedure_code, procedure_description) ON CONFLICT IGNORE
);

/*
    DimProvider -   providers dimensional data
                    preventing insertion of the same  provider_id by ignoring (not failing) duplicate
                    provider_id (PK)
                    preventing duplications with the UNIQUE statement - ignoring duplications on INSERTS
                    this might not be the optimal way to deal with duplications but looks reasonable in
                    the exercise scope
*/
CREATE TABLE IF NOT EXISTS DimProvider(
    provider_id         TEXT PRIMARY KEY ON CONFLICT IGNORE,
    provider_org_name   TEXT,
    provider_last_name  TEXT,
    UNIQUE (provider_id, provider_org_name, provider_last_name) ON CONFLICT IGNORE

);

/*
    FactDiagnosis - fact table for diagnosis (grain)
                    diagnosis_id - auto incremental PK
                    procedure_code - foreign key to DimProcedure
                    provider_id - foreign key to DimProvider
                    preventing insertion of the same  provider_id by ignoring (not failing) duplicate
                    provider_id (PK)
                    ingest_timestamp - of type timestamp with a default current_timestamp value to indicate
                    when data was inserted to the table, mainly for debugging/monitoring purposes
*/
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
