--
-- PostgreSQL database dump
--

\restrict MWAoaLu1GdoZo9mrvRwH59emXkLIYlYfARxVo5qcz8P9hPnmi5XO5xYPm93h8a6

-- Dumped from database version 18.0
-- Dumped by pg_dump version 18.0

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: curated; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA curated;


ALTER SCHEMA curated OWNER TO postgres;

--
-- Name: mart; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA mart;


ALTER SCHEMA mart OWNER TO postgres;

--
-- Name: stg; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA stg;


ALTER SCHEMA stg OWNER TO postgres;

--
-- Name: clean_code(text); Type: FUNCTION; Schema: curated; Owner: postgres
--

CREATE FUNCTION curated.clean_code(txt text) RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$
SELECT CASE
   WHEN txt IS NULL OR UPPER(TRIM(txt)) IN (SELECT token FROM curated._missing_tokens)
   THEN NULL
   ELSE replace(UPPER(TRIM(txt)), '.', '')
END
$$;


ALTER FUNCTION curated.clean_code(txt text) OWNER TO postgres;

--
-- Name: _nonnull_count(text[]); Type: FUNCTION; Schema: mart; Owner: postgres
--

CREATE FUNCTION mart._nonnull_count(VARIADIC arr text[]) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
  SELECT count(*) FROM unnest(arr) AS t(x) WHERE x IS NOT NULL
$$;


ALTER FUNCTION mart._nonnull_count(VARIADIC arr text[]) OWNER TO postgres;

--
-- Name: _missing_tokens; Type: VIEW; Schema: curated; Owner: postgres
--

CREATE VIEW curated._missing_tokens AS
 SELECT unnest(ARRAY['NA'::text, 'N/A'::text, ''::text, 'NULL'::text, 'null'::text]) AS token;


ALTER VIEW curated._missing_tokens OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: beneficiary; Type: TABLE; Schema: curated; Owner: postgres
--

CREATE TABLE curated.beneficiary (
    beneid text NOT NULL,
    dob date,
    dod date,
    gender integer,
    race integer,
    renal_disease_ind integer,
    state integer,
    county integer,
    months_part_a integer,
    months_part_b integer,
    chronic_alzheimer integer,
    chronic_heartfailure integer,
    chronic_kidney integer,
    chronic_cancer integer,
    chronic_copd integer,
    chronic_depression integer,
    chronic_diabetes integer,
    chronic_ihd integer,
    chronic_osteoporosis integer,
    chronic_ra integer,
    chronic_stroke integer,
    ip_annual_reimb numeric(12,2),
    ip_annual_deduct numeric(12,2),
    op_annual_reimb numeric(12,2),
    op_annual_deduct numeric(12,2)
);


ALTER TABLE curated.beneficiary OWNER TO postgres;

--
-- Name: claims_inpatient; Type: TABLE; Schema: curated; Owner: postgres
--

CREATE TABLE curated.claims_inpatient (
    beneid text,
    claimid text NOT NULL,
    provider text,
    claim_start date,
    claim_end date,
    admit_dt date,
    discharge_dt date,
    reimb_amt numeric(12,2),
    deductible_paid numeric(12,2),
    admit_dx text,
    drg text,
    dx1 text,
    dx2 text,
    dx3 text,
    dx4 text,
    dx5 text,
    dx6 text,
    dx7 text,
    dx8 text,
    dx9 text,
    dx10 text,
    px1 text,
    px2 text,
    px3 text,
    px4 text,
    px5 text,
    px6 text,
    icd_version text
);


ALTER TABLE curated.claims_inpatient OWNER TO postgres;

--
-- Name: claims_outpatient; Type: TABLE; Schema: curated; Owner: postgres
--

CREATE TABLE curated.claims_outpatient (
    beneid text,
    claimid text NOT NULL,
    provider text,
    claim_start date,
    claim_end date,
    reimb_amt numeric(12,2),
    deductible_paid numeric(12,2),
    admit_dx text,
    dx1 text,
    dx2 text,
    dx3 text,
    dx4 text,
    dx5 text,
    dx6 text,
    dx7 text,
    dx8 text,
    dx9 text,
    dx10 text,
    px1 text,
    px2 text,
    px3 text,
    px4 text,
    px5 text,
    px6 text,
    icd_version text
);


ALTER TABLE curated.claims_outpatient OWNER TO postgres;

--
-- Name: provider_labels; Type: TABLE; Schema: curated; Owner: postgres
--

CREATE TABLE curated.provider_labels (
    provider text NOT NULL,
    potential_fraud boolean
);


ALTER TABLE curated.provider_labels OWNER TO postgres;

--
-- Name: claim_features; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.claim_features (
    claimid text,
    claim_type text,
    provider text,
    beneid text,
    claim_start date,
    claim_end date,
    reimb_amt numeric(12,2),
    deductible_paid numeric(12,2),
    dx_count integer,
    px_count integer,
    primary_dx_prefix text,
    drg text,
    dow integer,
    is_weekend boolean,
    month integer,
    quarter integer,
    los_days integer,
    short_stay_flag integer,
    amount_zero_flag integer,
    amount_high_flag integer,
    exact_duplicate_flag integer,
    near_duplicate_group_size bigint
);


ALTER TABLE mart.claim_features OWNER TO postgres;

--
-- Name: code_dx; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.code_dx (
    provider text,
    day date,
    code text
);


ALTER TABLE mart.code_dx OWNER TO postgres;

--
-- Name: code_px; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.code_px (
    provider text,
    day date,
    code text
);


ALTER TABLE mart.code_px OWNER TO postgres;

--
-- Name: fact_claim; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.fact_claim (
    claim_type text,
    claimid text,
    provider text,
    beneid text,
    claim_start date,
    claim_end date,
    admit_dt date,
    discharge_dt date,
    reimb_amt numeric(12,2),
    deductible_paid numeric(12,2),
    admit_dx text,
    drg text,
    dx1 text,
    dx2 text,
    dx3 text,
    dx4 text,
    dx5 text,
    dx6 text,
    dx7 text,
    dx8 text,
    dx9 text,
    dx10 text,
    px1 text,
    px2 text,
    px3 text,
    px4 text,
    px5 text,
    px6 text,
    icd_version text
);


ALTER TABLE mart.fact_claim OWNER TO postgres;

--
-- Name: features_claim; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.features_claim (
    claimid text,
    claim_type text,
    provider text,
    beneid text,
    claim_start date,
    claim_end date,
    reimb_amt numeric(12,2),
    deductible_paid numeric(12,2),
    dx_count integer,
    px_count integer,
    primary_dx_prefix text,
    drg text,
    dow integer,
    is_weekend boolean,
    month integer,
    quarter integer,
    los_days integer,
    short_stay_flag integer,
    amount_zero_flag integer,
    amount_high_flag integer,
    exact_duplicate_flag integer,
    near_duplicate_group_size bigint,
    dup_exact_flag integer,
    dup_near_count bigint,
    upcoding_ip_flag integer,
    upcoding_op_flag integer,
    overcharge_z_flag integer,
    overcharge_iqr_flag integer,
    label_provider_fraud_1_0 integer
);


ALTER TABLE mart.features_claim OWNER TO postgres;

--
-- Name: features_provider; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.features_provider (
    provider text,
    label_provider_fraud_1_0 integer,
    claims_30d numeric,
    claims_90d numeric,
    claims_365d numeric,
    reimb_30d numeric,
    reimb_90d numeric,
    reimb_365d numeric,
    avg_reimb_day_30d numeric,
    unique_benes_30d bigint,
    unique_benes_90d bigint,
    unique_benes_365d bigint,
    dx_distinct_30d bigint,
    px_distinct_30d bigint,
    deductible_share_30d numeric,
    ip_claims_365 numeric,
    ip_avg_z_reimb_365 double precision,
    ip_max_z_reimb_365 double precision,
    ip_avg_z_los_365 numeric,
    ip_max_z_los_365 numeric,
    op_claims_365 numeric,
    op_avg_z_reimb_365 double precision,
    op_max_z_reimb_365 double precision,
    claims_total bigint,
    dup_exact_claims bigint,
    dup_near_total numeric,
    upcoding_ip_claims bigint,
    upcoding_op_claims bigint,
    overcharge_z_claims bigint,
    overcharge_iqr_claims bigint
);


ALTER TABLE mart.features_provider OWNER TO postgres;

--
-- Name: ip_drg_caps_365; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.ip_drg_caps_365 (
    drg text,
    p99 double precision
);


ALTER TABLE mart.ip_drg_caps_365 OWNER TO postgres;

--
-- Name: ip_drg_peer_365; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.ip_drg_peer_365 (
    drg text,
    peer_reimb_mean_365 double precision,
    peer_reimb_sd_365 double precision,
    peer_los_mean_365 numeric,
    peer_los_sd_365 numeric,
    peer_claims_365 bigint
);


ALTER TABLE mart.ip_drg_peer_365 OWNER TO postgres;

--
-- Name: mv_provider_monthly; Type: MATERIALIZED VIEW; Schema: mart; Owner: postgres
--

CREATE MATERIALIZED VIEW mart.mv_provider_monthly AS
 SELECT provider,
    (date_trunc('month'::text, (claim_start)::timestamp with time zone))::date AS month_start,
    count(*) AS claims_cnt,
    sum(reimb_amt) AS reimb_sum,
    sum(deductible_paid) AS deductible_sum,
    count(DISTINCT beneid) AS unique_benes,
    sum(((claim_type = 'IP'::text))::integer) AS ip_claims,
    sum(((claim_type = 'OP'::text))::integer) AS op_claims
   FROM mart.fact_claim
  WHERE (claim_start IS NOT NULL)
  GROUP BY provider, (date_trunc('month'::text, (claim_start)::timestamp with time zone))
  WITH NO DATA;


ALTER MATERIALIZED VIEW mart.mv_provider_monthly OWNER TO postgres;

--
-- Name: op_dxprefix_caps_365; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.op_dxprefix_caps_365 (
    dx_prefix text,
    p99 double precision
);


ALTER TABLE mart.op_dxprefix_caps_365 OWNER TO postgres;

--
-- Name: op_prefix_peer_365; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.op_prefix_peer_365 (
    dx_prefix text,
    peer_reimb_mean_365 double precision,
    peer_reimb_sd_365 double precision,
    peer_claims_365 bigint
);


ALTER TABLE mart.op_prefix_peer_365 OWNER TO postgres;

--
-- Name: provider_daily; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.provider_daily (
    provider text,
    day date,
    claims_cnt bigint,
    reimb_sum numeric,
    deductible_sum numeric,
    unique_benes_daily bigint,
    dx_distinct_daily bigint,
    px_distinct_daily bigint
);


ALTER TABLE mart.provider_daily OWNER TO postgres;

--
-- Name: provider_drg_stats_365; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.provider_drg_stats_365 (
    provider text,
    drg text,
    prov_claims_365 bigint,
    prov_reimb_mean_365 double precision,
    peer_reimb_mean_365 double precision,
    peer_reimb_sd_365 double precision,
    z_reimb_365 double precision,
    prov_los_mean_365 numeric,
    peer_los_mean_365 numeric,
    peer_los_sd_365 numeric,
    z_los_365 numeric
);


ALTER TABLE mart.provider_drg_stats_365 OWNER TO postgres;

--
-- Name: provider_op_prefix_stats_365; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.provider_op_prefix_stats_365 (
    provider text,
    dx_prefix text,
    prov_claims_365 bigint,
    prov_reimb_mean_365 double precision,
    peer_reimb_mean_365 double precision,
    peer_reimb_sd_365 double precision,
    z_reimb_365 double precision
);


ALTER TABLE mart.provider_op_prefix_stats_365 OWNER TO postgres;

--
-- Name: provider_rollups; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.provider_rollups (
    provider text,
    day date,
    claims_30d numeric,
    claims_90d numeric,
    claims_365d numeric,
    reimb_30d numeric,
    reimb_90d numeric,
    reimb_365d numeric,
    avg_reimb_day_30d numeric,
    unique_benes_30d bigint,
    unique_benes_90d bigint,
    unique_benes_365d bigint,
    dx_distinct_30d bigint,
    px_distinct_30d bigint,
    deductible_share_30d numeric
);


ALTER TABLE mart.provider_rollups OWNER TO postgres;

--
-- Name: provider_rollups_latest; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.provider_rollups_latest (
    provider text,
    day date,
    claims_30d numeric,
    claims_90d numeric,
    claims_365d numeric,
    reimb_30d numeric,
    reimb_90d numeric,
    reimb_365d numeric,
    avg_reimb_day_30d numeric,
    unique_benes_30d bigint,
    unique_benes_90d bigint,
    unique_benes_365d bigint,
    dx_distinct_30d bigint,
    px_distinct_30d bigint,
    deductible_share_30d numeric
);


ALTER TABLE mart.provider_rollups_latest OWNER TO postgres;

--
-- Name: rule_claim_flags; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.rule_claim_flags (
    claimid text,
    dup_exact_flag integer,
    dup_near_count bigint,
    upcoding_ip_flag integer,
    upcoding_op_flag integer,
    overcharge_z_flag integer,
    overcharge_iqr_flag integer
);


ALTER TABLE mart.rule_claim_flags OWNER TO postgres;

--
-- Name: rule_claim_z; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.rule_claim_z (
    claimid text,
    z_ip_reimb double precision,
    z_ip_los numeric,
    z_op_reimb double precision
);


ALTER TABLE mart.rule_claim_z OWNER TO postgres;

--
-- Name: rule_dup_exact; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.rule_dup_exact (
    claimid text,
    dup_exact_flag integer
);


ALTER TABLE mart.rule_dup_exact OWNER TO postgres;

--
-- Name: rule_dup_near; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.rule_dup_near (
    claimid text,
    dup_near_count bigint
);


ALTER TABLE mart.rule_dup_near OWNER TO postgres;

--
-- Name: rule_overcharge; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.rule_overcharge (
    claimid text,
    overcharge_z_flag integer,
    overcharge_iqr_flag integer
);


ALTER TABLE mart.rule_overcharge OWNER TO postgres;

--
-- Name: rule_peer_iqr; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.rule_peer_iqr (
    claim_type text,
    peer_key text,
    q1 double precision,
    q3 double precision
);


ALTER TABLE mart.rule_peer_iqr OWNER TO postgres;

--
-- Name: rule_provider_flags; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.rule_provider_flags (
    provider text,
    claims_total bigint,
    dup_exact_claims bigint,
    dup_near_total numeric,
    upcoding_ip_claims bigint,
    upcoding_op_claims bigint,
    overcharge_z_claims bigint,
    overcharge_iqr_claims bigint
);


ALTER TABLE mart.rule_provider_flags OWNER TO postgres;

--
-- Name: rule_upcoding; Type: TABLE; Schema: mart; Owner: postgres
--

CREATE TABLE mart.rule_upcoding (
    claimid text,
    upcoding_ip_flag integer,
    upcoding_op_flag integer
);


ALTER TABLE mart.rule_upcoding OWNER TO postgres;

--
-- Name: v_claim_with_dims; Type: VIEW; Schema: mart; Owner: postgres
--

CREATE VIEW mart.v_claim_with_dims AS
 SELECT f.claim_type,
    f.claimid,
    f.provider,
    p.potential_fraud,
    f.beneid,
    b.gender,
    b.race,
    b.state,
    b.county,
    b.dob,
    b.dod,
    b.months_part_a,
    b.months_part_b,
    b.renal_disease_ind,
    f.claim_start,
    f.claim_end,
    f.admit_dt,
    f.discharge_dt,
    f.reimb_amt,
    f.deductible_paid,
    f.admit_dx,
    f.drg,
    f.dx1,
    f.dx2,
    f.dx3,
    f.dx4,
    f.dx5,
    f.dx6,
    f.dx7,
    f.dx8,
    f.dx9,
    f.dx10,
    f.px1,
    f.px2,
    f.px3,
    f.px4,
    f.px5,
    f.px6,
    f.icd_version
   FROM ((mart.fact_claim f
     LEFT JOIN curated.provider_labels p USING (provider))
     LEFT JOIN curated.beneficiary b USING (beneid));


ALTER VIEW mart.v_claim_with_dims OWNER TO postgres;

--
-- Name: v_provider_mom_growth; Type: VIEW; Schema: mart; Owner: postgres
--

CREATE VIEW mart.v_provider_mom_growth AS
 WITH m AS (
         SELECT mv_provider_monthly.provider,
            mv_provider_monthly.month_start,
            mv_provider_monthly.claims_cnt,
            mv_provider_monthly.reimb_sum,
            lag(mv_provider_monthly.claims_cnt) OVER (PARTITION BY mv_provider_monthly.provider ORDER BY mv_provider_monthly.month_start) AS prev_claims,
            lag(mv_provider_monthly.reimb_sum) OVER (PARTITION BY mv_provider_monthly.provider ORDER BY mv_provider_monthly.month_start) AS prev_reimb
           FROM mart.mv_provider_monthly
        )
 SELECT provider,
    month_start,
    claims_cnt,
    reimb_sum,
        CASE
            WHEN ((prev_claims IS NULL) OR (prev_claims = 0)) THEN NULL::numeric
            ELSE (((claims_cnt - prev_claims))::numeric / (prev_claims)::numeric)
        END AS claims_mom,
        CASE
            WHEN ((prev_reimb IS NULL) OR (prev_reimb = (0)::numeric)) THEN NULL::numeric
            ELSE ((reimb_sum - prev_reimb) / prev_reimb)
        END AS reimb_mom
   FROM m;


ALTER VIEW mart.v_provider_mom_growth OWNER TO postgres;

--
-- Name: v_provider_peer_norm_365; Type: VIEW; Schema: mart; Owner: postgres
--

CREATE VIEW mart.v_provider_peer_norm_365 AS
 SELECT p.provider,
    sum(p.prov_claims_365) AS ip_claims_365,
    avg(NULLIF(p.z_reimb_365, (0)::double precision)) AS ip_avg_z_reimb_365,
    max(p.z_reimb_365) AS ip_max_z_reimb_365,
    avg(NULLIF(p.z_los_365, (0)::numeric)) AS ip_avg_z_los_365,
    max(p.z_los_365) AS ip_max_z_los_365
   FROM mart.provider_drg_stats_365 p
  GROUP BY p.provider
UNION ALL
 SELECT o.provider,
    NULL::bigint AS ip_claims_365,
    NULL::numeric AS ip_avg_z_reimb_365,
    NULL::numeric AS ip_max_z_reimb_365,
    NULL::numeric AS ip_avg_z_los_365,
    NULL::numeric AS ip_max_z_los_365
   FROM mart.provider_op_prefix_stats_365 o
  WHERE false;


ALTER VIEW mart.v_provider_peer_norm_365 OWNER TO postgres;

--
-- Name: v_provider_peer_norm_365_full; Type: VIEW; Schema: mart; Owner: postgres
--

CREATE VIEW mart.v_provider_peer_norm_365_full AS
 SELECT COALESCE(ip.provider, op.provider) AS provider,
    ip.ip_claims_365,
    ip.ip_avg_z_reimb_365,
    ip.ip_max_z_reimb_365,
    ip.ip_avg_z_los_365,
    ip.ip_max_z_los_365,
    op.op_claims_365,
    op.op_avg_z_reimb_365,
    op.op_max_z_reimb_365
   FROM (( SELECT provider_drg_stats_365.provider,
            sum(provider_drg_stats_365.prov_claims_365) AS ip_claims_365,
            avg(provider_drg_stats_365.z_reimb_365) AS ip_avg_z_reimb_365,
            max(provider_drg_stats_365.z_reimb_365) AS ip_max_z_reimb_365,
            avg(provider_drg_stats_365.z_los_365) AS ip_avg_z_los_365,
            max(provider_drg_stats_365.z_los_365) AS ip_max_z_los_365
           FROM mart.provider_drg_stats_365
          GROUP BY provider_drg_stats_365.provider) ip
     FULL JOIN ( SELECT provider_op_prefix_stats_365.provider,
            sum(provider_op_prefix_stats_365.prov_claims_365) AS op_claims_365,
            avg(provider_op_prefix_stats_365.z_reimb_365) AS op_avg_z_reimb_365,
            max(provider_op_prefix_stats_365.z_reimb_365) AS op_max_z_reimb_365
           FROM mart.provider_op_prefix_stats_365
          GROUP BY provider_op_prefix_stats_365.provider) op USING (provider));


ALTER VIEW mart.v_provider_peer_norm_365_full OWNER TO postgres;

--
-- Name: train_beneficiarydata; Type: TABLE; Schema: stg; Owner: postgres
--

CREATE TABLE stg.train_beneficiarydata (
    beneid text,
    dob text,
    dod text,
    gender text,
    race text,
    renaldiseaseindicator text,
    state text,
    county text,
    noofmonths_partacov text,
    noofmonths_partbcov text,
    chroniccond_alzheimer text,
    chroniccond_heartfailure text,
    chroniccond_kidneydisease text,
    chroniccond_cancer text,
    chroniccond_obstrpulmonary text,
    chroniccond_depression text,
    chroniccond_diabetes text,
    chroniccond_ischemicheart text,
    chroniccond_osteoporasis text,
    chroniccond_rheumatoidarthritis text,
    chroniccond_stroke text,
    ipannualreimbursementamt text,
    ipannualdeductibleamt text,
    opannualreimbursementamt text,
    opannualdeductibleamt text,
    load_file_name text,
    loaded_at timestamp with time zone DEFAULT now()
);


ALTER TABLE stg.train_beneficiarydata OWNER TO postgres;

--
-- Name: train_inpatientdata; Type: TABLE; Schema: stg; Owner: postgres
--

CREATE TABLE stg.train_inpatientdata (
    beneid text,
    claimid text,
    claimstartdt text,
    claimenddt text,
    provider text,
    inscclaimamtreimbursed text,
    attendingphysician text,
    operatingphysician text,
    otherphysician text,
    admissiondt text,
    clmadmitdiagnosiscode text,
    deductibleamtpaid text,
    dischargedt text,
    diagnosisgroupcode text,
    clmdiagnosiscode_1 text,
    clmdiagnosiscode_2 text,
    clmdiagnosiscode_3 text,
    clmdiagnosiscode_4 text,
    clmdiagnosiscode_5 text,
    clmdiagnosiscode_6 text,
    clmdiagnosiscode_7 text,
    clmdiagnosiscode_8 text,
    clmdiagnosiscode_9 text,
    clmdiagnosiscode_10 text,
    clmprocedurecode_1 text,
    clmprocedurecode_2 text,
    clmprocedurecode_3 text,
    clmprocedurecode_4 text,
    clmprocedurecode_5 text,
    clmprocedurecode_6 text,
    load_file_name text,
    loaded_at timestamp with time zone DEFAULT now()
);


ALTER TABLE stg.train_inpatientdata OWNER TO postgres;

--
-- Name: train_outpatientdata; Type: TABLE; Schema: stg; Owner: postgres
--

CREATE TABLE stg.train_outpatientdata (
    beneid text,
    claimid text,
    claimstartdt text,
    claimenddt text,
    provider text,
    inscclaimamtreimbursed text,
    attendingphysician text,
    operatingphysician text,
    otherphysician text,
    clmdiagnosiscode_1 text,
    clmdiagnosiscode_2 text,
    clmdiagnosiscode_3 text,
    clmdiagnosiscode_4 text,
    clmdiagnosiscode_5 text,
    clmdiagnosiscode_6 text,
    clmdiagnosiscode_7 text,
    clmdiagnosiscode_8 text,
    clmdiagnosiscode_9 text,
    clmdiagnosiscode_10 text,
    clmprocedurecode_1 text,
    clmprocedurecode_2 text,
    clmprocedurecode_3 text,
    clmprocedurecode_4 text,
    clmprocedurecode_5 text,
    clmprocedurecode_6 text,
    deductibleamtpaid text,
    clmadmitdiagnosiscode text,
    load_file_name text,
    loaded_at timestamp with time zone DEFAULT now()
);


ALTER TABLE stg.train_outpatientdata OWNER TO postgres;

--
-- Name: train_provider; Type: TABLE; Schema: stg; Owner: postgres
--

CREATE TABLE stg.train_provider (
    provider text,
    potential_fraud text,
    load_file_name text,
    loaded_at timestamp with time zone DEFAULT now()
);


ALTER TABLE stg.train_provider OWNER TO postgres;

--
-- Name: beneficiary beneficiary_pk; Type: CONSTRAINT; Schema: curated; Owner: postgres
--

ALTER TABLE ONLY curated.beneficiary
    ADD CONSTRAINT beneficiary_pk PRIMARY KEY (beneid);


--
-- Name: claims_inpatient claims_inpatient_pk; Type: CONSTRAINT; Schema: curated; Owner: postgres
--

ALTER TABLE ONLY curated.claims_inpatient
    ADD CONSTRAINT claims_inpatient_pk PRIMARY KEY (claimid);


--
-- Name: claims_outpatient claims_outpatient_pk; Type: CONSTRAINT; Schema: curated; Owner: postgres
--

ALTER TABLE ONLY curated.claims_outpatient
    ADD CONSTRAINT claims_outpatient_pk PRIMARY KEY (claimid);


--
-- Name: provider_labels provider_labels_pk; Type: CONSTRAINT; Schema: curated; Owner: postgres
--

ALTER TABLE ONLY curated.provider_labels
    ADD CONSTRAINT provider_labels_pk PRIMARY KEY (provider);


--
-- Name: idx_ip_beneid; Type: INDEX; Schema: curated; Owner: postgres
--

CREATE INDEX idx_ip_beneid ON curated.claims_inpatient USING btree (beneid);


--
-- Name: idx_ip_provider; Type: INDEX; Schema: curated; Owner: postgres
--

CREATE INDEX idx_ip_provider ON curated.claims_inpatient USING btree (provider);


--
-- Name: idx_op_beneid; Type: INDEX; Schema: curated; Owner: postgres
--

CREATE INDEX idx_op_beneid ON curated.claims_outpatient USING btree (beneid);


--
-- Name: idx_op_provider; Type: INDEX; Schema: curated; Owner: postgres
--

CREATE INDEX idx_op_provider ON curated.claims_outpatient USING btree (provider);


--
-- Name: cf_beneid_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX cf_beneid_idx ON mart.claim_features USING btree (beneid);


--
-- Name: cf_provider_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX cf_provider_idx ON mart.claim_features USING btree (provider);


--
-- Name: cf_start_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX cf_start_idx ON mart.claim_features USING btree (claim_start);


--
-- Name: code_dx_prov_day_code_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX code_dx_prov_day_code_idx ON mart.code_dx USING btree (provider, day, code);


--
-- Name: code_px_prov_day_code_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX code_px_prov_day_code_idx ON mart.code_px USING btree (provider, day, code);


--
-- Name: fact_claim_beneid_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX fact_claim_beneid_idx ON mart.fact_claim USING btree (beneid);


--
-- Name: fact_claim_claim_start_brin; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX fact_claim_claim_start_brin ON mart.fact_claim USING brin (claim_start);


--
-- Name: fact_claim_provider_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX fact_claim_provider_idx ON mart.fact_claim USING btree (provider);


--
-- Name: fact_claim_provider_start_bene_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX fact_claim_provider_start_bene_idx ON mart.fact_claim USING btree (provider, claim_start, beneid);


--
-- Name: fact_claim_start_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX fact_claim_start_idx ON mart.fact_claim USING btree (claim_start);


--
-- Name: features_claim_claimid_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX features_claim_claimid_idx ON mart.features_claim USING btree (claimid);


--
-- Name: features_claim_provider_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX features_claim_provider_idx ON mart.features_claim USING btree (provider);


--
-- Name: features_provider_provider_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX features_provider_provider_idx ON mart.features_provider USING btree (provider);


--
-- Name: ip_drg_peer_365_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX ip_drg_peer_365_idx ON mart.ip_drg_peer_365 USING btree (drg);


--
-- Name: mv_provider_monthly_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX mv_provider_monthly_idx ON mart.mv_provider_monthly USING btree (provider, month_start);


--
-- Name: op_prefix_peer_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX op_prefix_peer_idx ON mart.op_prefix_peer_365 USING btree (dx_prefix);


--
-- Name: pd_provider_day_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX pd_provider_day_idx ON mart.provider_daily USING btree (provider, day);


--
-- Name: pr_latest_provider_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX pr_latest_provider_idx ON mart.provider_rollups_latest USING btree (provider);


--
-- Name: pr_provider_day_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX pr_provider_day_idx ON mart.provider_rollups USING btree (provider, day);


--
-- Name: provider_drg_stats_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX provider_drg_stats_idx ON mart.provider_drg_stats_365 USING btree (provider, drg);


--
-- Name: provider_op_prefix_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX provider_op_prefix_idx ON mart.provider_op_prefix_stats_365 USING btree (provider, dx_prefix);


--
-- Name: rule_claim_flags_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX rule_claim_flags_idx ON mart.rule_claim_flags USING btree (dup_exact_flag, upcoding_ip_flag, upcoding_op_flag, overcharge_z_flag);


--
-- Name: rule_dup_near_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX rule_dup_near_idx ON mart.rule_dup_near USING btree (dup_near_count);


--
-- Name: rule_overcharge_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX rule_overcharge_idx ON mart.rule_overcharge USING btree (overcharge_z_flag, overcharge_iqr_flag);


--
-- Name: rule_provider_flags_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX rule_provider_flags_idx ON mart.rule_provider_flags USING btree (provider);


--
-- Name: rule_upcoding_flags_idx; Type: INDEX; Schema: mart; Owner: postgres
--

CREATE INDEX rule_upcoding_flags_idx ON mart.rule_upcoding USING btree (upcoding_ip_flag, upcoding_op_flag);


--
-- Name: claims_inpatient claims_inpatient_beneid_fkey; Type: FK CONSTRAINT; Schema: curated; Owner: postgres
--

ALTER TABLE ONLY curated.claims_inpatient
    ADD CONSTRAINT claims_inpatient_beneid_fkey FOREIGN KEY (beneid) REFERENCES curated.beneficiary(beneid);


--
-- Name: claims_inpatient claims_inpatient_provider_fkey; Type: FK CONSTRAINT; Schema: curated; Owner: postgres
--

ALTER TABLE ONLY curated.claims_inpatient
    ADD CONSTRAINT claims_inpatient_provider_fkey FOREIGN KEY (provider) REFERENCES curated.provider_labels(provider);


--
-- Name: claims_outpatient claims_outpatient_beneid_fkey; Type: FK CONSTRAINT; Schema: curated; Owner: postgres
--

ALTER TABLE ONLY curated.claims_outpatient
    ADD CONSTRAINT claims_outpatient_beneid_fkey FOREIGN KEY (beneid) REFERENCES curated.beneficiary(beneid);


--
-- Name: claims_outpatient claims_outpatient_provider_fkey; Type: FK CONSTRAINT; Schema: curated; Owner: postgres
--

ALTER TABLE ONLY curated.claims_outpatient
    ADD CONSTRAINT claims_outpatient_provider_fkey FOREIGN KEY (provider) REFERENCES curated.provider_labels(provider);


--
-- Name: SCHEMA curated; Type: ACL; Schema: -; Owner: postgres
--

GRANT ALL ON SCHEMA curated TO zuddin00;


--
-- Name: SCHEMA mart; Type: ACL; Schema: -; Owner: postgres
--

GRANT ALL ON SCHEMA mart TO zuddin00;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT USAGE ON SCHEMA public TO zuddin00;


--
-- Name: SCHEMA stg; Type: ACL; Schema: -; Owner: postgres
--

GRANT USAGE ON SCHEMA stg TO zuddin00;


--
-- Name: TABLE _missing_tokens; Type: ACL; Schema: curated; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE curated._missing_tokens TO zuddin00;


--
-- Name: TABLE beneficiary; Type: ACL; Schema: curated; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE curated.beneficiary TO zuddin00;


--
-- Name: TABLE claims_inpatient; Type: ACL; Schema: curated; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE curated.claims_inpatient TO zuddin00;


--
-- Name: TABLE claims_outpatient; Type: ACL; Schema: curated; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE curated.claims_outpatient TO zuddin00;


--
-- Name: TABLE provider_labels; Type: ACL; Schema: curated; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE curated.provider_labels TO zuddin00;


--
-- Name: TABLE claim_features; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.claim_features TO zuddin00;


--
-- Name: TABLE code_dx; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.code_dx TO zuddin00;


--
-- Name: TABLE code_px; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.code_px TO zuddin00;


--
-- Name: TABLE fact_claim; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.fact_claim TO zuddin00;


--
-- Name: TABLE features_claim; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.features_claim TO zuddin00;


--
-- Name: TABLE features_provider; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.features_provider TO zuddin00;


--
-- Name: TABLE ip_drg_caps_365; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.ip_drg_caps_365 TO zuddin00;


--
-- Name: TABLE ip_drg_peer_365; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.ip_drg_peer_365 TO zuddin00;


--
-- Name: TABLE mv_provider_monthly; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.mv_provider_monthly TO zuddin00;


--
-- Name: TABLE op_dxprefix_caps_365; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.op_dxprefix_caps_365 TO zuddin00;


--
-- Name: TABLE op_prefix_peer_365; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.op_prefix_peer_365 TO zuddin00;


--
-- Name: TABLE provider_daily; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.provider_daily TO zuddin00;


--
-- Name: TABLE provider_drg_stats_365; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.provider_drg_stats_365 TO zuddin00;


--
-- Name: TABLE provider_op_prefix_stats_365; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.provider_op_prefix_stats_365 TO zuddin00;


--
-- Name: TABLE provider_rollups; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.provider_rollups TO zuddin00;


--
-- Name: TABLE provider_rollups_latest; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.provider_rollups_latest TO zuddin00;


--
-- Name: TABLE rule_claim_flags; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.rule_claim_flags TO zuddin00;


--
-- Name: TABLE rule_claim_z; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.rule_claim_z TO zuddin00;


--
-- Name: TABLE rule_dup_exact; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.rule_dup_exact TO zuddin00;


--
-- Name: TABLE rule_dup_near; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.rule_dup_near TO zuddin00;


--
-- Name: TABLE rule_overcharge; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.rule_overcharge TO zuddin00;


--
-- Name: TABLE rule_peer_iqr; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.rule_peer_iqr TO zuddin00;


--
-- Name: TABLE rule_provider_flags; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.rule_provider_flags TO zuddin00;


--
-- Name: TABLE rule_upcoding; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.rule_upcoding TO zuddin00;


--
-- Name: TABLE v_claim_with_dims; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.v_claim_with_dims TO zuddin00;


--
-- Name: TABLE v_provider_mom_growth; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.v_provider_mom_growth TO zuddin00;


--
-- Name: TABLE v_provider_peer_norm_365; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.v_provider_peer_norm_365 TO zuddin00;


--
-- Name: TABLE v_provider_peer_norm_365_full; Type: ACL; Schema: mart; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE mart.v_provider_peer_norm_365_full TO zuddin00;


--
-- PostgreSQL database dump complete
--

\unrestrict MWAoaLu1GdoZo9mrvRwH59emXkLIYlYfARxVo5qcz8P9hPnmi5XO5xYPm93h8a6

