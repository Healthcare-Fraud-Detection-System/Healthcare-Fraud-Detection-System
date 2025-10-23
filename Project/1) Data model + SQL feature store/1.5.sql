-- -- 1.5) Build baseline SQL views/materialized tables that join relevant tables (claim + provider + beneficiary). 

-- -- 1) Merging InPatient ∪ OutPatient into One Fact Table
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.fact_claim CASCADE;
CREATE TABLE mart.fact_claim AS
SELECT
  'IP'::text AS claim_type,
  claimid,
  provider,
  beneid,
  claim_start,
  claim_end,
  admit_dt,
  discharge_dt,
  reimb_amt,
  deductible_paid,
  admit_dx,
  drg,
  dx1,dx2,dx3,dx4,dx5,dx6,dx7,dx8,dx9,dx10,
  px1,px2,px3,px4,px5,px6,
  icd_version
FROM curated.claims_inpatient
UNION ALL
SELECT
  'OP'::text AS claim_type,
  claimid,
  provider,
  beneid,
  claim_start,
  claim_end,
  NULL::date AS admit_dt,
  NULL::date AS discharge_dt,
  reimb_amt,
  deductible_paid,
  admit_dx,
  NULL::text AS drg,
  dx1,dx2,dx3,dx4,dx5,dx6,dx7,dx8,dx9,dx10,
  px1,px2,px3,px4,px5,px6,
  icd_version
FROM curated.claims_outpatient;

-- -- Helpful indexes for joins & time grouping
CREATE INDEX IF NOT EXISTS fact_claim_provider_idx ON mart.fact_claim(provider);
CREATE INDEX IF NOT EXISTS fact_claim_beneid_idx   ON mart.fact_claim(beneid);
CREATE INDEX IF NOT EXISTS fact_claim_start_idx    ON mart.fact_claim(claim_start);

-- 2) ready-to-use joined view (adds provider & beneficiary context)
DROP VIEW IF EXISTS mart.v_claim_with_dims;
CREATE VIEW mart.v_claim_with_dims AS
SELECT
  f.claim_type,
  f.claimid,
  f.provider,
  p.potential_fraud, -- training label (NULL in scoring/holdout)
  f.beneid,
  b.gender, b.race, b.state, b.county,
  b.dob, b.dod, b.months_part_a, b.months_part_b,
  b.renal_disease_ind,
  f.claim_start, f.claim_end, f.admit_dt, f.discharge_dt,
  f.reimb_amt, f.deductible_paid,
  f.admit_dx, f.drg,
  f.dx1, f.dx2, f.dx3, f.dx4, f.dx5, f.dx6, f.dx7, f.dx8, f.dx9, f.dx10,
  f.px1, f.px2, f.px3, f.px4, f.px5, f.px6,
  f.icd_version
FROM mart.fact_claim f
LEFT JOIN curated.provider_labels p USING (provider)
LEFT JOIN curated.beneficiary b USING (beneid);

-- 3) (Optional) Fast monthly rollups for providers
DROP MATERIALIZED VIEW IF EXISTS mart.mv_provider_monthly;
CREATE MATERIALIZED VIEW mart.mv_provider_monthly AS
SELECT
  provider,
  date_trunc('month', claim_start)::date AS month_start,
  COUNT(*) AS claims_cnt,
  SUM(reimb_amt) AS reimb_sum,
  SUM(deductible_paid) AS deductible_sum,
  COUNT(DISTINCT beneid) AS unique_benes,
  SUM( (claim_type='IP')::int ) AS ip_claims,
  SUM( (claim_type='OP')::int ) AS op_claims
FROM mart.fact_claim
WHERE claim_start IS NOT NULL
GROUP BY provider, date_trunc('month', claim_start);

-- Index for fast refresh/filters
CREATE INDEX IF NOT EXISTS mv_provider_monthly_idx
  ON mart.mv_provider_monthly(provider, month_start);

-- When data changes, refresh:
-- REFRESH MATERIALIZED VIEW mart.mv_provider_monthly;
-- (If you later want REFRESH ... CONCURRENTLY, create a UNIQUE index first.)

-- Tiny sanity checks (Should equal IP rows + OP rows)
SELECT 'fact_claim_count' AS check, COUNT(*) FROM mart.fact_claim
UNION ALL
SELECT 'ip_plus_op' AS check,
  (SELECT COUNT(*) FROM curated.claims_inpatient) +
  (SELECT COUNT(*) FROM curated.claims_outpatient);

-- Quick join works
SELECT COUNT(*) FROM mart.v_claim_with_dims;

-- Monthly rollup sample (top 5)
SELECT * FROM mart.mv_provider_monthly ORDER BY provider, month_start LIMIT 5;

