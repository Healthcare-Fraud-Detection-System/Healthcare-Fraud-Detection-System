-- 0) Train-frozen baselines from mart.features_claim (build once)

-- =============================
-- BASELINES (from TRAIN = mart)
-- =============================

-- 0.1 Inpatient peer stats by DRG (mean, std, IQR)
DROP MATERIALIZED VIEW IF EXISTS mart.baseline_ip_drg_stats;
CREATE MATERIALIZED VIEW mart.baseline_ip_drg_stats AS
SELECT
  drg,
  AVG(reimb_amt)::numeric(14,6) AS mean_reimb,
  NULLIF(stddev_pop(reimb_amt),0)::numeric(14,6) AS std_reimb,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY reimb_amt)::numeric(14,6) AS p25,
  percentile_cont(0.75) WITHIN GROUP (ORDER BY reimb_amt)::numeric(14,6) AS p75
FROM mart.features_claim
WHERE claim_type = 'IP' AND reimb_amt IS NOT NULL
GROUP BY drg;

CREATE INDEX IF NOT EXISTS baseline_ip_drg_stats_drg_idx
  ON mart.baseline_ip_drg_stats(drg);

-- 0.2 Outpatient peer stats by primary_dx_prefix
DROP MATERIALIZED VIEW IF EXISTS mart.baseline_op_dxprefix_stats;
CREATE MATERIALIZED VIEW mart.baseline_op_dxprefix_stats AS
SELECT
  primary_dx_prefix,
  AVG(reimb_amt)::numeric(14,6)          AS mean_reimb,
  NULLIF(stddev_pop(reimb_amt),0)::numeric(14,6) AS std_reimb,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY reimb_amt)::numeric(14,6) AS p25,
  percentile_cont(0.75) WITHIN GROUP (ORDER BY reimb_amt)::numeric(14,6) AS p75
FROM mart.features_claim
WHERE claim_type = 'OP' AND reimb_amt IS NOT NULL
GROUP BY primary_dx_prefix;

CREATE INDEX IF NOT EXISTS baseline_op_dxprefix_stats_idx
  ON mart.baseline_op_dxprefix_stats(primary_dx_prefix);

-- 0.3 High-amount threshold by claim_type (use train p99)
DROP MATERIALIZED VIEW IF EXISTS mart.baseline_claimtype_p99;
CREATE MATERIALIZED VIEW mart.baseline_claimtype_p99 AS
SELECT
  claim_type,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY reimb_amt)::numeric(14,6) AS p99_reimb
FROM mart.features_claim
WHERE reimb_amt IS NOT NULL
GROUP BY claim_type;

CREATE INDEX IF NOT EXISTS baseline_claimtype_p99_idx
  ON mart.baseline_claimtype_p99(claim_type);
