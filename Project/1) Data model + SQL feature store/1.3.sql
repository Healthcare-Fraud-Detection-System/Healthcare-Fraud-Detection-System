-- 1.3) Clean and normalize data: missing values, date parsing, code standardization

-- 1) Create curated schema & helper view
CREATE SCHEMA IF NOT EXISTS curated;

-- Centralize placeholder tokens
CREATE OR REPLACE VIEW curated._missing_tokens AS
SELECT unnest(ARRAY['NA','N/A','','NULL','null']) AS token;

-- 2) Clean provider labels (typed)

DROP TABLE IF EXISTS curated.provider_labels CASCADE;
CREATE TABLE curated.provider_labels AS
SELECT
  NULLIF(TRIM(provider), '') AS provider,
  CASE UPPER(TRIM(potential_fraud))
       WHEN 'YES' THEN TRUE
       WHEN 'NO'  THEN FALSE
       ELSE NULL
  END AS potential_fraud
FROM stg.train_provider;

-- Basic DQ: provider should not be null
ALTER TABLE curated.provider_labels
  ADD CONSTRAINT provider_labels_pk PRIMARY KEY (provider);


-- 3) Clean beneficiary table (dates, flags, amounts)

DROP TABLE IF EXISTS curated.beneficiary CASCADE;

CREATE TABLE curated.beneficiary AS
WITH t AS (
  SELECT
    CASE 
      WHEN UPPER(TRIM(beneid)) IN (SELECT token FROM curated._missing_tokens) THEN NULL
      ELSE TRIM(beneid)
    END AS beneid,

    NULLIF(TRIM(dob), '') AS dob_raw,
    NULLIF(TRIM(dod), '') AS dod_raw,

    -- Gender: “1” → 0, “2” → 1, else NULL
    CASE
      WHEN TRIM(gender) = '1' THEN 0
      WHEN TRIM(gender) = '2' THEN 1
      ELSE NULL
    END AS gender_raw,

    -- Race: 1–5 only
    CASE
      WHEN TRIM(race) ~ '^[1-5]$' THEN CAST(TRIM(race) AS INTEGER)
      ELSE NULL
    END AS race_raw,

    -- Renal indicator: “0” → 0, “Y” → 1, else NULL
    CASE
      WHEN TRIM(renaldiseaseindicator) = '0' THEN 0
      WHEN UPPER(TRIM(renaldiseaseindicator)) = 'Y' THEN 1
      ELSE NULL
    END AS renal_raw,

    NULLIF(TRIM(state), '')::INTEGER AS state,
	NULLIF(TRIM(county), '')::INTEGER AS county,


    NULLIF(TRIM(noofmonths_partacov), '') AS part_a_months_raw,
    NULLIF(TRIM(noofmonths_partbcov), '') AS part_b_months_raw,

    -- Chronic flags: “1” → 0, “2” → 1
    CASE WHEN TRIM(chroniccond_alzheimer) = '1' THEN 0
         WHEN TRIM(chroniccond_alzheimer) = '2' THEN 1
         ELSE NULL END AS chronic_alzheimer,
    CASE WHEN TRIM(chroniccond_heartfailure) = '1' THEN 0
         WHEN TRIM(chroniccond_heartfailure) = '2' THEN 1
         ELSE NULL END AS chronic_heartfailure,
    CASE WHEN TRIM(chroniccond_kidneydisease) = '1' THEN 0
         WHEN TRIM(chroniccond_kidneydisease) = '2' THEN 1
         ELSE NULL END AS chronic_kidney,
    CASE WHEN TRIM(chroniccond_cancer) = '1' THEN 0
         WHEN TRIM(chroniccond_cancer) = '2' THEN 1
         ELSE NULL END AS chronic_cancer,
    CASE WHEN TRIM(chroniccond_obstrpulmonary) = '1' THEN 0
         WHEN TRIM(chroniccond_obstrpulmonary) = '2' THEN 1
         ELSE NULL END AS chronic_copd,
    CASE WHEN TRIM(chroniccond_depression) = '1' THEN 0
         WHEN TRIM(chroniccond_depression) = '2' THEN 1
         ELSE NULL END AS chronic_depression,
    CASE WHEN TRIM(chroniccond_diabetes) = '1' THEN 0
         WHEN TRIM(chroniccond_diabetes) = '2' THEN 1
         ELSE NULL END AS chronic_diabetes,
    CASE WHEN TRIM(chroniccond_ischemicheart) = '1' THEN 0
         WHEN TRIM(chroniccond_ischemicheart) = '2' THEN 1
         ELSE NULL END AS chronic_ihd,
    CASE WHEN TRIM(chroniccond_osteoporasis) = '1' THEN 0
         WHEN TRIM(chroniccond_osteoporasis) = '2' THEN 1
         ELSE NULL END AS chronic_osteoporosis,
    CASE WHEN TRIM(chroniccond_rheumatoidarthritis) = '1' THEN 0
         WHEN TRIM(chroniccond_rheumatoidarthritis) = '2' THEN 1
         ELSE NULL END AS chronic_ra,
    CASE WHEN TRIM(chroniccond_stroke) = '1' THEN 0
         WHEN TRIM(chroniccond_stroke) = '2' THEN 1
         ELSE NULL END AS chronic_stroke,

    NULLIF(TRIM(ipannualreimbursementamt), '') AS ip_annual_reimb_raw,
    NULLIF(TRIM(ipannualdeductibleamt), '') AS ip_annual_deduct_raw,
    NULLIF(TRIM(opannualreimbursementamt), '') AS op_annual_reimb_raw,
    NULLIF(TRIM(opannualdeductibleamt), '') AS op_annual_deduct_raw

  FROM stg.train_beneficiarydata
)
SELECT
  beneid,

  -- Guarded parsing of dob
  CASE
    WHEN dob_raw IS NULL OR dob_raw !~ '^\d{4}-\d{2}-\d{2}$' THEN NULL
    ELSE TO_DATE(dob_raw, 'YYYY-MM-DD')
  END AS dob,

  -- Guarded parsing of dod
  CASE
    WHEN dod_raw IS NULL OR dod_raw !~ '^\d{4}-\d{2}-\d{2}$' THEN NULL
    ELSE TO_DATE(dod_raw, 'YYYY-MM-DD')
  END AS dod,

  gender_raw AS gender,
  race_raw   AS race,

  renal_raw AS renal_disease_ind,

  state, county,

  NULLIF(part_a_months_raw, 'NA')::INTEGER AS months_part_a,
  NULLIF(part_b_months_raw, 'NA')::INTEGER AS months_part_b,

  chronic_alzheimer,
  chronic_heartfailure,
  chronic_kidney,
  chronic_cancer,
  chronic_copd,
  chronic_depression,
  chronic_diabetes,
  chronic_ihd,
  chronic_osteoporosis,
  chronic_ra,
  chronic_stroke,

  ip_annual_reimb_raw::NUMERIC(12,2)    AS ip_annual_reimb,
  ip_annual_deduct_raw::NUMERIC(12,2)   AS ip_annual_deduct,
  op_annual_reimb_raw::NUMERIC(12,2)    AS op_annual_reimb,
  op_annual_deduct_raw::NUMERIC(12,2)   AS op_annual_deduct

FROM t;

ALTER TABLE curated.beneficiary
  ADD CONSTRAINT beneficiary_pk PRIMARY KEY (beneid);

-- Removes periods and uppercases code strings (works for ICD-9/10)
CREATE OR REPLACE FUNCTION curated.clean_code(txt TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
SELECT CASE
   WHEN txt IS NULL OR UPPER(TRIM(txt)) IN (SELECT token FROM curated._missing_tokens)
   THEN NULL
   ELSE replace(UPPER(TRIM(txt)), '.', '')
END
$$;

-- 4) Clean Inpatient table (dates, flags, amounts)
DROP TABLE IF EXISTS curated.claims_inpatient CASCADE;
CREATE TABLE curated.claims_inpatient AS
SELECT
  NULLIF(TRIM(beneid), '')                              AS beneid,
  NULLIF(TRIM(claimid), '')                             AS claimid,
  NULLIF(TRIM(provider), '')                            AS provider,
  TO_DATE(NULLIF(TRIM(claimstartdt), ''), 'YYYY-MM-DD') AS claim_start,
  TO_DATE(NULLIF(TRIM(claimenddt),   ''), 'YYYY-MM-DD') AS claim_end,
  TO_DATE(NULLIF(TRIM(admissiondt),  ''), 'YYYY-MM-DD') AS admit_dt,
  TO_DATE(NULLIF(TRIM(dischargedt),  ''), 'YYYY-MM-DD') AS discharge_dt,
  NULLIF(NULLIF(TRIM(inscclaimamtreimbursed), ''), 'NA')::NUMERIC(12,2) AS reimb_amt,
  NULLIF(NULLIF(TRIM(deductibleamtpaid), ''), 'NA')::NUMERIC(12,2) AS deductible_paid,
  curated.clean_code(clmadmitdiagnosiscode)     AS admit_dx,
  curated.clean_code(diagnosisgroupcode)                AS drg,
  curated.clean_code(clmdiagnosiscode_1)  AS dx1,
  curated.clean_code(clmdiagnosiscode_2)  AS dx2,
  curated.clean_code(clmdiagnosiscode_3)  AS dx3,
  curated.clean_code(clmdiagnosiscode_4)  AS dx4,
  curated.clean_code(clmdiagnosiscode_5)  AS dx5,
  curated.clean_code(clmdiagnosiscode_6)  AS dx6,
  curated.clean_code(clmdiagnosiscode_7)  AS dx7,
  curated.clean_code(clmdiagnosiscode_8)  AS dx8,
  curated.clean_code(clmdiagnosiscode_9)  AS dx9,
  curated.clean_code(clmdiagnosiscode_10) AS dx10,
  curated.clean_code(clmprocedurecode_1)  AS px1,
  curated.clean_code(clmprocedurecode_2)  AS px2,
  curated.clean_code(clmprocedurecode_3)  AS px3,
  curated.clean_code(clmprocedurecode_4)  AS px4,
  curated.clean_code(clmprocedurecode_5)  AS px5,
  curated.clean_code(clmprocedurecode_6)  AS px6,
  -- Determine ICD version by start date (ICD-10 from 2015-10-01)
  CASE WHEN TO_DATE(NULLIF(TRIM(claimstartdt), ''), 'YYYY-MM-DD') >= DATE '2015-10-01'
       THEN 'ICD10' ELSE 'ICD9' END AS icd_version
FROM stg.train_inpatientdata;

ALTER TABLE curated.claims_inpatient
  ADD CONSTRAINT claims_inpatient_pk PRIMARY KEY (claimid);

-- 5) Clean Outpatient Table (dates, flags, amounts)
DROP TABLE IF EXISTS curated.claims_outpatient CASCADE;
CREATE TABLE curated.claims_outpatient AS
SELECT
  NULLIF(TRIM(beneid), '')                              AS beneid,
  NULLIF(TRIM(claimid), '')                             AS claimid,
  NULLIF(TRIM(provider), '')                            AS provider,
  TO_DATE(NULLIF(TRIM(claimstartdt), ''), 'YYYY-MM-DD') AS claim_start,
  TO_DATE(NULLIF(TRIM(claimenddt),   ''), 'YYYY-MM-DD') AS claim_end,
  NULLIF(NULLIF(TRIM(inscclaimamtreimbursed), ''), 'NA')::NUMERIC(12,2) AS reimb_amt,
  NULLIF(NULLIF(TRIM(deductibleamtpaid), ''), 'NA')::NUMERIC(12,2) AS deductible_paid,
  curated.clean_code(clmadmitdiagnosiscode)             AS admit_dx,
  curated.clean_code(clmdiagnosiscode_1)  AS dx1,
  curated.clean_code(clmdiagnosiscode_2)  AS dx2,
  curated.clean_code(clmdiagnosiscode_3)  AS dx3,
  curated.clean_code(clmdiagnosiscode_4)  AS dx4,
  curated.clean_code(clmdiagnosiscode_5)  AS dx5,
  curated.clean_code(clmdiagnosiscode_6)  AS dx6,
  curated.clean_code(clmdiagnosiscode_7)  AS dx7,
  curated.clean_code(clmdiagnosiscode_8)  AS dx8,
  curated.clean_code(clmdiagnosiscode_9)  AS dx9,
  curated.clean_code(clmdiagnosiscode_10) AS dx10,
  curated.clean_code(clmprocedurecode_1)  AS px1,
  curated.clean_code(clmprocedurecode_2)  AS px2,
  curated.clean_code(clmprocedurecode_3)  AS px3,
  curated.clean_code(clmprocedurecode_4)  AS px4,
  curated.clean_code(clmprocedurecode_5)  AS px5,
  curated.clean_code(clmprocedurecode_6)  AS px6,
  CASE WHEN TO_DATE(NULLIF(TRIM(claimstartdt), ''), 'YYYY-MM-DD') >= DATE '2015-10-01'
       THEN 'ICD10' ELSE 'ICD9' END AS icd_version
FROM stg.train_outpatientdata;

ALTER TABLE curated.claims_outpatient
  ADD CONSTRAINT claims_outpatient_pk PRIMARY KEY (claimid);

