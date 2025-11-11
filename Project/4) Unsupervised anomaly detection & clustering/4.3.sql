-- 4.3) turn model scores into a clean, audit-ready ranking
-- B) Materialize Top-lists in Postgres (budget-aware)

-- Helpful index on the ranked table
CREATE INDEX IF NOT EXISTS idx_anom_ranked_type_consensus
ON mart.anomaly_claim_scores_ranked (claim_type, consensus_rank);

-- Option 1: Percentile cutoff (e.g., top 0.5% by consensus within each cohort)
DROP MATERIALIZED VIEW IF EXISTS mart.anomaly_claim_top_pctl;
CREATE MATERIALIZED VIEW mart.anomaly_claim_top_pctl AS
WITH bounds AS (
  SELECT claim_type,
         percentile_disc(0.995) WITHIN GROUP (ORDER BY consensus_rank) AS thr
  FROM mart.anomaly_claim_scores_ranked
  GROUP BY claim_type
)
SELECT r.*
FROM mart.anomaly_claim_scores_ranked r
JOIN bounds b USING (claim_type)
WHERE r.consensus_rank >= b.thr;

CREATE INDEX IF NOT EXISTS idx_anom_top_pctl
ON mart.anomaly_claim_top_pctl (claim_type, consensus_rank DESC);

-- Option 2: Top-K per cohort (replace K_IP/K_OP with your audit capacity)
-- Example: 1,000 per cohort
DROP MATERIALIZED VIEW IF EXISTS mart.anomaly_claim_topk;
CREATE MATERIALIZED VIEW mart.anomaly_claim_topk AS
SELECT *
FROM (
  SELECT r.*,
         ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY consensus_rank DESC) AS rn
  FROM mart.anomaly_claim_scores_ranked r
) z
WHERE (z.claim_type='IP' AND z.rn <= 1000)
   OR (z.claim_type='OP' AND z.rn <= 1000);

CREATE INDEX IF NOT EXISTS idx_anom_topk
ON mart.anomaly_claim_topk (claim_type, rn);


-- C) Optional: put detector-specific top tails too
DROP MATERIALIZED VIEW IF EXISTS mart.anomaly_claim_top_by_model;
CREATE MATERIALIZED VIEW mart.anomaly_claim_top_by_model AS
WITH ranked AS (
  SELECT claimid, claim_type, model, anomaly_score,
         PERCENT_RANK() OVER (PARTITION BY claim_type, model ORDER BY anomaly_score) AS pr
  FROM mart.anomaly_claim_scores
)
SELECT * FROM ranked WHERE pr >= 0.995;  -- top 0.5% within each model/cohort

-- D) Quick QA & summary views
-- Count by cohort for your chosen list (swap to anomaly_claim_topk if you use Top-K)
SELECT claim_type, COUNT(*) AS n_top
FROM mart.anomaly_claim_top_pctl
GROUP BY 1;

-- Dollars & provider counts (join minimal context)
SELECT t.claim_type,
       COUNT(*) AS n_claims,
       SUM(c.reimb_amt) AS total_paid,
       COUNT(DISTINCT c.provider) AS n_providers
FROM mart.anomaly_claim_top_pctl t
JOIN mart.features_claim c USING (claimid, claim_type)
GROUP BY 1
ORDER BY 1;
