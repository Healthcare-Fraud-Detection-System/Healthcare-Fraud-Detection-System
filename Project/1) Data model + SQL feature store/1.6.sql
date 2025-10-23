-- 1.6) Audit basic data distribution: counts, nulls, value ranges, outliers in amounts, lengths of stay.

-- 1) Row counts & keys

-- table sizes + distinct IDs
SELECT 'ip_rows' AS k, COUNT(*) FROM curated.claims_inpatient UNION ALL
SELECT 'op_rows', COUNT(*) FROM curated.claims_outpatient UNION ALL
SELECT 'bene_rows', COUNT(*) FROM curated.beneficiary UNION ALL
SELECT 'prov_rows', COUNT(*) FROM curated.provider_labels UNION ALL
SELECT 'ip_distinct_claimid', COUNT(DISTINCT claimid) FROM curated.claims_inpatient UNION ALL
SELECT 'op_distinct_claimid', COUNT(DISTINCT claimid) FROM curated.claims_outpatient UNION ALL
SELECT 'distinct_beneid_in_claims', COUNT(DISTINCT beneid) FROM mart.fact_claim UNION ALL
SELECT 'distinct_provider_in_claims', COUNT(DISTINCT provider) FROM mart.fact_claim;


-- 2) Missingness (null/placeholder already normalized → just NULL rates)

-- NULL rates for key analytics columns
SELECT 'reimb_amt_null_rate' AS metric,
       ROUND(100.0*SUM(CASE WHEN reimb_amt IS NULL THEN 1 ELSE 0 END)/COUNT(*),2) AS pct
FROM mart.fact_claim
UNION ALL
SELECT 'deductible_null_rate',
       ROUND(100.0*SUM(CASE WHEN deductible_paid IS NULL THEN 1 ELSE 0 END)/COUNT(*),2)
FROM mart.fact_claim
UNION ALL
SELECT 'dx1_null_rate',
       ROUND(100.0*SUM(CASE WHEN dx1 IS NULL THEN 1 ELSE 0 END)/COUNT(*),2)
FROM mart.fact_claim;

-- 3) Value ranges & percentiles (for amounts)
-- Postgres ordered-set aggregates: percentiles per claim_type
SELECT claim_type,
       MIN(reimb_amt) AS min_amt,
       PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY reimb_amt) AS p25,
       PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY reimb_amt) AS p50,
       PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY reimb_amt) AS p75,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY reimb_amt) AS p95,
       PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY reimb_amt) AS p99,
       MAX(reimb_amt) AS max_amt
FROM mart.fact_claim
WHERE reimb_amt IS NOT NULL
GROUP BY claim_type
ORDER BY claim_type;

-- 4) Outliers — IQR “boxplot rule” (robust, simple)
-- flag high/low outliers on reimb_amt by claim_type
WITH pct AS (
  SELECT claim_type,
         PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY reimb_amt) AS q1,
         PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY reimb_amt) AS q3
  FROM mart.fact_claim
  WHERE reimb_amt IS NOT NULL
  GROUP BY claim_type
),
fences AS (
  SELECT claim_type,
         q1, q3, (q3 - q1) AS iqr,
         q1 - 1.5*(q3 - q1) AS low_fence,
         q3 + 1.5*(q3 - q1) AS high_fence
  FROM pct
)
SELECT f.claim_type,
       COUNT(*) FILTER (WHERE c.reimb_amt < f.low_fence)  AS low_outliers,
       COUNT(*) FILTER (WHERE c.reimb_amt > f.high_fence) AS high_outliers
FROM mart.fact_claim c
JOIN fences f USING (claim_type)
WHERE c.reimb_amt IS NOT NULL
GROUP BY f.claim_type;

-- 5) Outliers — MAD “robust z-score” (extra-robust option)
-- LOS in days (don’t count discharge day per Medicare guidance)
-- If discharge_dt is NULL, exclude from LOS stats.
-- Robust z = |x - median| / (1.4826 * MAD); flag where robust z > 3.5 (tweakable)
WITH s AS (
  SELECT claim_type,
         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY reimb_amt) AS med
  FROM mart.fact_claim
  WHERE reimb_amt IS NOT NULL
  GROUP BY claim_type
),
resid AS (
  SELECT c.claim_type, c.reimb_amt, s.med, ABS(c.reimb_amt - s.med) AS abs_dev
  FROM mart.fact_claim c JOIN s USING (claim_type)
  WHERE c.reimb_amt IS NOT NULL
),
mad AS (
  SELECT claim_type,
         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY abs_dev) AS mad
  FROM resid
  GROUP BY claim_type
)
SELECT r.claim_type,
       COUNT(*) FILTER (WHERE (r.abs_dev / NULLIF(1.4826*m.mad,0)) > 3.5) AS mad_outliers
FROM resid r JOIN mad m USING (claim_type)
GROUP BY r.claim_type;


-- 6) Length of stay (LOS) checks — IP only
WITH ip AS (
  SELECT claimid, provider, admit_dt, discharge_dt,
         GREATEST((discharge_dt - admit_dt), 0) AS los_days
  FROM mart.fact_claim
  WHERE claim_type='IP' AND admit_dt IS NOT NULL AND discharge_dt IS NOT NULL
)
SELECT
  MIN(los_days) AS min_los,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY los_days) AS p25,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY los_days) AS median,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY los_days) AS p75,
  MAX(los_days) AS max_los
FROM ip;

-- Sanity flags for impossible or extreme LOS - IP only
WITH ip AS (
  SELECT GREATEST((discharge_dt - admit_dt), 0) AS los_days
  FROM mart.fact_claim
  WHERE claim_type='IP' AND admit_dt IS NOT NULL AND discharge_dt IS NOT NULL
)
SELECT
  COUNT(*) FILTER (WHERE los_days < 0) AS negative_los,
  COUNT(*) FILTER (WHERE los_days = 0) AS same_day_stays,
  COUNT(*) FILTER (WHERE los_days < 2) AS short_stays,
  COUNT(*) FILTER (WHERE los_days > 30) AS very_long_stays
FROM ip;