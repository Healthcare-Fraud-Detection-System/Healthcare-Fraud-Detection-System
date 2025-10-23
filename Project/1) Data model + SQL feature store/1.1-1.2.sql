-- 1.1) Load raw Medicare data (inpatient, outpatient, beneficiaries) into relational database or data warehouse
-- 1.2) Design and create schema/tables: dimension tables (provider, diagnosis, procedure), fact tables (claims).

-- 1) creating schema
CREATE SCHEMA IF NOT EXISTS stg;

-- 2) Provider label table
CREATE TABLE IF NOT EXISTS stg.train_provider (
  provider              TEXT,
  potential_fraud       TEXT,
  load_file_name        TEXT,
  loaded_at             TIMESTAMPTZ DEFAULT now()
);

-- 3) Beneficiary table
CREATE TABLE IF NOT EXISTS stg.train_beneficiarydata (
  beneid                             TEXT,
  dob                                TEXT,
  dod                                TEXT,
  gender                             TEXT,
  race                               TEXT,
  renaldiseaseindicator              TEXT,
  state                              TEXT,
  county                             TEXT,
  noofmonths_partacov                TEXT,
  noofmonths_partbcov                TEXT,
  chroniccond_alzheimer              TEXT,
  chroniccond_heartfailure           TEXT,
  chroniccond_kidneydisease          TEXT,
  chroniccond_cancer                 TEXT,
  chroniccond_obstrpulmonary         TEXT,
  chroniccond_depression             TEXT,
  chroniccond_diabetes               TEXT,
  chroniccond_ischemicheart          TEXT,
  chroniccond_osteoporasis           TEXT,
  chroniccond_rheumatoidarthritis    TEXT,
  chroniccond_stroke                 TEXT,
  ipannualreimbursementamt           TEXT,
  ipannualdeductibleamt              TEXT,
  opannualreimbursementamt           TEXT,
  opannualdeductibleamt              TEXT,
  load_file_name                     TEXT,
  loaded_at                          TIMESTAMPTZ DEFAULT now()
);

-- 4) Inpatient claims table
CREATE TABLE IF NOT EXISTS stg.train_inpatientdata (
  beneid                 TEXT,
  claimid                TEXT,
  claimstartdt           TEXT,
  claimenddt             TEXT,
  provider               TEXT,
  inscclaimamtreimbursed TEXT,
  attendingphysician     TEXT,
  operatingphysician     TEXT,
  otherphysician         TEXT,
  admissiondt            TEXT,
  clmadmitdiagnosiscode  TEXT,
  deductibleamtpaid      TEXT,
  dischargedt            TEXT,
  diagnosisgroupcode     TEXT,
  clmdiagnosiscode_1     TEXT,
  clmdiagnosiscode_2     TEXT,
  clmdiagnosiscode_3     TEXT,
  clmdiagnosiscode_4     TEXT,
  clmdiagnosiscode_5     TEXT,
  clmdiagnosiscode_6     TEXT,
  clmdiagnosiscode_7     TEXT,
  clmdiagnosiscode_8     TEXT,
  clmdiagnosiscode_9     TEXT,
  clmdiagnosiscode_10    TEXT,
  clmprocedurecode_1     TEXT,
  clmprocedurecode_2     TEXT,
  clmprocedurecode_3     TEXT,
  clmprocedurecode_4     TEXT,
  clmprocedurecode_5     TEXT,
  clmprocedurecode_6     TEXT,
  load_file_name         TEXT,
  loaded_at              TIMESTAMPTZ DEFAULT now()
);
-- 4) Inpatient claims table
CREATE TABLE IF NOT EXISTS stg.train_inpatientdata (
  beneid                 TEXT,
  claimid                TEXT,
  claimstartdt           TEXT,
  claimenddt             TEXT,
  provider               TEXT,
  inscclaimamtreimbursed TEXT,
  attendingphysician     TEXT,
  operatingphysician     TEXT,
  otherphysician         TEXT,
  admissiondt            TEXT,
  clmadmitdiagnosiscode  TEXT,
  deductibleamtpaid      TEXT,
  dischargedt            TEXT,
  diagnosisgroupcode     TEXT,
  clmdiagnosiscode_1     TEXT,
  clmdiagnosiscode_2     TEXT,
  clmdiagnosiscode_3     TEXT,
  clmdiagnosiscode_4     TEXT,
  clmdiagnosiscode_5     TEXT,
  clmdiagnosiscode_6     TEXT,
  clmdiagnosiscode_7     TEXT,
  clmdiagnosiscode_8     TEXT,
  clmdiagnosiscode_9     TEXT,
  clmdiagnosiscode_10    TEXT,
  clmprocedurecode_1     TEXT,
  clmprocedurecode_2     TEXT,
  clmprocedurecode_3     TEXT,
  clmprocedurecode_4     TEXT,
  clmprocedurecode_5     TEXT,
  clmprocedurecode_6     TEXT,
  load_file_name         TEXT,
  loaded_at              TIMESTAMPTZ DEFAULT now()
);

-- 5) Outpatient claims table
CREATE TABLE IF NOT EXISTS stg.train_outpatientdata (
  beneid                 TEXT,
  claimid                TEXT,
  claimstartdt           TEXT,
  claimenddt             TEXT,
  provider               TEXT,
  inscclaimamtreimbursed TEXT,
  attendingphysician     TEXT,
  operatingphysician     TEXT,
  otherphysician         TEXT,
  clmdiagnosiscode_1     TEXT,
  clmdiagnosiscode_2     TEXT,
  clmdiagnosiscode_3     TEXT,
  clmdiagnosiscode_4     TEXT,
  clmdiagnosiscode_5     TEXT,
  clmdiagnosiscode_6     TEXT,
  clmdiagnosiscode_7     TEXT,
  clmdiagnosiscode_8     TEXT,
  clmdiagnosiscode_9     TEXT,
  clmdiagnosiscode_10    TEXT,
  clmprocedurecode_1     TEXT,
  clmprocedurecode_2     TEXT,
  clmprocedurecode_3     TEXT,
  clmprocedurecode_4     TEXT,
  clmprocedurecode_5     TEXT,
  clmprocedurecode_6     TEXT,
  deductibleamtpaid      TEXT,
  clmadmitdiagnosiscode  TEXT,
  load_file_name         TEXT,
  loaded_at              TIMESTAMPTZ DEFAULT now()
);


-- annotate the file name
UPDATE stg.train_provider        SET load_file_name = 'Train.csv'                 WHERE load_file_name IS NULL;
UPDATE stg.train_beneficiarydata SET load_file_name = 'Train_Beneficiarydata.csv' WHERE load_file_name IS NULL;
UPDATE stg.train_inpatientdata   SET load_file_name = 'Train_Inpatientdata.csv'   WHERE load_file_name IS NULL;
UPDATE stg.train_outpatientdata  SET load_file_name = 'Train_Outpatientdata.csv'  WHERE load_file_name IS NULL;


-- Row counts
SELECT 'train_provider' tbl, count(*) FROM stg.train_provider UNION ALL
SELECT 'train_beneficiarydata', count(*) FROM stg.train_beneficiarydata UNION ALL
SELECT 'train_inpatientdata', count(*) FROM stg.train_inpatientdata UNION ALL
SELECT 'train_outpatientdata', count(*) FROM stg.train_outpatientdata;

-- Looking for duplicates across tables --
WITH
-- 1) Placeholder tokens we treat as "missing"
tokens(token) AS (
  VALUES ('NA'), ('N/A'), (''), ('NULL'), ('null')
),
-- 2) Date pattern (adjust to '^\d{2}/\d{2}/\d{4}$' if your CSV is MM/DD/YYYY)
rx(pattern) AS (SELECT '^\d{4}-\d{2}-\d{2}$'),

-- 3) Helpers to count once per test
dup_bene AS (
  SELECT COUNT(*) AS n FROM (
    SELECT beneid FROM stg.train_beneficiarydata
    GROUP BY beneid HAVING COUNT(*) > 1
  ) d
),
dup_provider AS (
  SELECT COUNT(*) AS n FROM (
    SELECT provider FROM stg.train_provider
    GROUP BY provider HAVING COUNT(*) > 1
  ) d
),
orph_bene_ip AS (
  SELECT COUNT(*) AS n
  FROM stg.train_inpatientdata ip
  LEFT JOIN stg.train_beneficiarydata b USING (beneid)
  WHERE b.beneid IS NULL
),
orph_bene_op AS (
  SELECT COUNT(*) AS n
  FROM stg.train_outpatientdata op
  LEFT JOIN stg.train_beneficiarydata b USING (beneid)
  WHERE b.beneid IS NULL
),
orph_provider_ip AS (
  SELECT COUNT(*) AS n
  FROM stg.train_inpatientdata ip
  LEFT JOIN stg.train_provider p USING (provider)
  WHERE p.provider IS NULL
),
orph_provider_op AS (
  SELECT COUNT(*) AS n
  FROM stg.train_outpatientdata op
  LEFT JOIN stg.train_provider p USING (provider)
  WHERE p.provider IS NULL
),

-- 4) Placeholders in key IDs
ph_bene_bene AS (
  SELECT COUNT(*) AS n
  FROM stg.train_beneficiarydata b, tokens
  WHERE UPPER(TRIM(b.beneid)) = tokens.token
),
ph_provider_ip AS (
  SELECT COUNT(*) AS n
  FROM stg.train_inpatientdata t, tokens
  WHERE UPPER(TRIM(t.provider)) = tokens.token
),
ph_provider_op AS (
  SELECT COUNT(*) AS n
  FROM stg.train_outpatientdata t, tokens
  WHERE UPPER(TRIM(t.provider)) = tokens.token
),
ph_claim_ip AS (
  SELECT COUNT(*) AS n
  FROM stg.train_inpatientdata t, tokens
  WHERE UPPER(TRIM(t.claimid)) = tokens.token
),
ph_claim_op AS (
  SELECT COUNT(*) AS n
  FROM stg.train_outpatientdata t, tokens
  WHERE UPPER(TRIM(t.claimid)) = tokens.token
),

-- 5) Placeholders in diagnosis / procedure arrays (IP & OP)
ph_dx_ip AS (
  SELECT COUNT(*) AS n
  FROM stg.train_inpatientdata t, tokens
  WHERE EXISTS (
    SELECT 1 FROM UNNEST(ARRAY[
      t.clmdiagnosiscode_1, t.clmdiagnosiscode_2, t.clmdiagnosiscode_3, t.clmdiagnosiscode_4,
      t.clmdiagnosiscode_5, t.clmdiagnosiscode_6, t.clmdiagnosiscode_7, t.clmdiagnosiscode_8,
      t.clmdiagnosiscode_9, t.clmdiagnosiscode_10
    ]) AS dx(val)
    WHERE UPPER(TRIM(dx.val)) = tokens.token
  )
),
ph_px_ip AS (
  SELECT COUNT(*) AS n
  FROM stg.train_inpatientdata t, tokens
  WHERE EXISTS (
    SELECT 1 FROM UNNEST(ARRAY[
      t.clmprocedurecode_1, t.clmprocedurecode_2, t.clmprocedurecode_3,
      t.clmprocedurecode_4, t.clmprocedurecode_5, t.clmprocedurecode_6
    ]) AS px(val)
    WHERE UPPER(TRIM(px.val)) = tokens.token
  )
),
ph_dx_op AS (
  SELECT COUNT(*) AS n
  FROM stg.train_outpatientdata t, tokens
  WHERE EXISTS (
    SELECT 1 FROM UNNEST(ARRAY[
      t.clmdiagnosiscode_1, t.clmdiagnosiscode_2, t.clmdiagnosiscode_3, t.clmdiagnosiscode_4,
      t.clmdiagnosiscode_5, t.clmdiagnosiscode_6, t.clmdiagnosiscode_7, t.clmdiagnosiscode_8,
      t.clmdiagnosiscode_9, t.clmdiagnosiscode_10
    ]) AS dx(val)
    WHERE UPPER(TRIM(dx.val)) = tokens.token
  )
),
ph_px_op AS (
  SELECT COUNT(*) AS n
  FROM stg.train_outpatientdata t, tokens
  WHERE EXISTS (
    SELECT 1 FROM UNNEST(ARRAY[
      t.clmprocedurecode_1, t.clmprocedurecode_2, t.clmprocedurecode_3,
      t.clmprocedurecode_4, t.clmprocedurecode_5, t.clmprocedurecode_6
    ]) AS px(val)
    WHERE UPPER(TRIM(px.val)) = tokens.token
  )
),

-- 6) Bad date formats (exclude NULL/placeholders, then regex-mismatch)
bad_dates AS (
  SELECT 'bad_claimstartdt_ip' AS test, COUNT(*) AS n
  FROM stg.train_inpatientdata t, rx
  WHERE t.claimstartdt IS NOT NULL
    AND UPPER(TRIM(t.claimstartdt)) NOT IN (SELECT token FROM tokens)
    AND t.claimstartdt !~ rx.pattern
  UNION ALL
  SELECT 'bad_claimenddt_ip', COUNT(*)
  FROM stg.train_inpatientdata t, rx
  WHERE t.claimenddt IS NOT NULL
    AND UPPER(TRIM(t.claimenddt)) NOT IN (SELECT token FROM tokens)
    AND t.claimenddt !~ rx.pattern
  UNION ALL
  SELECT 'bad_admissiondt_ip', COUNT(*)
  FROM stg.train_inpatientdata t, rx
  WHERE t.admissiondt IS NOT NULL
    AND UPPER(TRIM(t.admissiondt)) NOT IN (SELECT token FROM tokens)
    AND t.admissiondt !~ rx.pattern
  UNION ALL
  SELECT 'bad_dischargedt_ip', COUNT(*)
  FROM stg.train_inpatientdata t, rx
  WHERE t.dischargedt IS NOT NULL
    AND UPPER(TRIM(t.dischargedt)) NOT IN (SELECT token FROM tokens)
    AND t.dischargedt !~ rx.pattern
  UNION ALL
  SELECT 'bad_claimstartdt_op', COUNT(*)
  FROM stg.train_outpatientdata t, rx
  WHERE t.claimstartdt IS NOT NULL
    AND UPPER(TRIM(t.claimstartdt)) NOT IN (SELECT token FROM tokens)
    AND t.claimstartdt !~ rx.pattern
  UNION ALL
  SELECT 'bad_claimenddt_op', COUNT(*)
  FROM stg.train_outpatientdata t, rx
  WHERE t.claimenddt IS NOT NULL
    AND UPPER(TRIM(t.claimenddt)) NOT IN (SELECT token FROM tokens)
    AND t.claimenddt !~ rx.pattern
  UNION ALL
  SELECT 'bad_dob_bene', COUNT(*)
  FROM stg.train_beneficiarydata b, rx
  WHERE b.dob IS NOT NULL
    AND UPPER(TRIM(b.dob)) NOT IN (SELECT token FROM tokens)
    AND b.dob !~ rx.pattern
  UNION ALL
  SELECT 'bad_dod_bene', COUNT(*)
  FROM stg.train_beneficiarydata b, rx
  WHERE b.dod IS NOT NULL AND TRIM(b.dod) <> ''
    AND UPPER(TRIM(b.dod)) NOT IN (SELECT token FROM tokens)
    AND b.dod !~ rx.pattern
)

-- Final stacked report (one row per check)
SELECT 'dup_beneid_in_beneficiary' AS test, 'stg.train_beneficiarydata' AS table_name, 'duplicate_beneid' AS issue, n
FROM dup_bene
UNION ALL
SELECT 'dup_provider_in_train', 'stg.train_provider', 'duplicate_provider', n
FROM dup_provider
UNION ALL
SELECT 'orphan_bene_in_inpatient', 'stg.train_inpatientdata', 'beneid_not_in_beneficiary', n
FROM orph_bene_ip
UNION ALL
SELECT 'orphan_bene_in_outpatient', 'stg.train_outpatientdata', 'beneid_not_in_beneficiary', n
FROM orph_bene_op
UNION ALL
SELECT 'orphan_provider_in_inpatient', 'stg.train_inpatientdata', 'provider_not_in_train', n
FROM orph_provider_ip
UNION ALL
SELECT 'orphan_provider_in_outpatient', 'stg.train_outpatientdata', 'provider_not_in_train', n
FROM orph_provider_op
UNION ALL
SELECT 'placeholder_beneid_beneficiary', 'stg.train_beneficiarydata', 'beneid_is_placeholder', n
FROM ph_bene_bene
UNION ALL
SELECT 'placeholder_provider_ip', 'stg.train_inpatientdata', 'provider_is_placeholder', n
FROM ph_provider_ip
UNION ALL
SELECT 'placeholder_provider_op', 'stg.train_outpatientdata', 'provider_is_placeholder', n
FROM ph_provider_op
UNION ALL
SELECT 'placeholder_claimid_ip', 'stg.train_inpatientdata', 'claimid_is_placeholder', n
FROM ph_claim_ip
UNION ALL
SELECT 'placeholder_claimid_op', 'stg.train_outpatientdata', 'claimid_is_placeholder', n
FROM ph_claim_op
UNION ALL
SELECT 'placeholder_dx_ip', 'stg.train_inpatientdata', 'diagnosis_has_placeholder', n
FROM ph_dx_ip
UNION ALL
SELECT 'placeholder_px_ip', 'stg.train_inpatientdata', 'procedure_has_placeholder', n
FROM ph_px_ip
UNION ALL
SELECT 'placeholder_dx_op', 'stg.train_outpatientdata', 'diagnosis_has_placeholder', n
FROM ph_dx_op
UNION ALL
SELECT 'placeholder_px_op', 'stg.train_outpatientdata', 'procedure_has_placeholder', n
FROM ph_px_op
UNION ALL
SELECT test, 'multiple tables' AS table_name, 'bad_date_format_regex' AS issue, n
FROM bad_dates
ORDER BY 1;



