DROP TABLE IF EXISTS mart.provider_rollups_latest CASCADE;
CREATE TABLE mart.provider_rollups_latest AS
WITH last_day AS (
  SELECT provider, MAX(day) AS last_day
  FROM mart.provider_rollups
  GROUP BY provider
)
SELECT pr.*
FROM mart.provider_rollups pr
JOIN last_day ld
  ON pr.provider = ld.provider AND pr.day = ld.last_day;
CREATE INDEX IF NOT EXISTS pr_latest_provider_idx
  ON mart.provider_rollups_latest(provider);

DROP TABLE IF EXISTS mart.features_provider CASCADE;
CREATE TABLE mart.features_provider AS
SELECT
  COALESCE(p.provider, r.provider, z.provider) AS provider,
  pl.potential_fraud::int AS label_provider_fraud_1_0,
  pr.claims_30d, pr.claims_90d, pr.claims_365d,
  pr.reimb_30d,  pr.reimb_90d,  pr.reimb_365d,
  pr.avg_reimb_day_30d,
  pr.unique_benes_30d, pr.unique_benes_90d, pr.unique_benes_365d,
  pr.dx_distinct_30d,  pr.px_distinct_30d,
  pr.deductible_share_30d,
  z.ip_claims_365,
  z.ip_avg_z_reimb_365, z.ip_max_z_reimb_365,
  z.ip_avg_z_los_365,   z.ip_max_z_los_365,
  z.op_claims_365,
  z.op_avg_z_reimb_365, z.op_max_z_reimb_365,
  r.claims_total,
  r.dup_exact_claims,
  r.dup_near_total,
  r.upcoding_ip_claims,
  r.upcoding_op_claims,
  r.overcharge_z_claims,
  r.overcharge_iqr_claims
FROM (SELECT DISTINCT provider FROM mart.fact_claim) p
LEFT JOIN mart.provider_rollups_latest     pr USING (provider)
LEFT JOIN mart.v_provider_peer_norm_365_full z USING (provider)
LEFT JOIN mart.rule_provider_flags         r USING (provider)
LEFT JOIN curated.provider_labels          pl USING (provider);
CREATE INDEX IF NOT EXISTS features_provider_provider_idx
  ON mart.features_provider(provider);

DROP TABLE IF EXISTS mart.features_claim CASCADE;
CREATE TABLE mart.features_claim AS
SELECT
  cf.*,
  rcf.dup_exact_flag,
  rcf.dup_near_count,
  rcf.upcoding_ip_flag,
  rcf.upcoding_op_flag,
  rcf.overcharge_z_flag,
  rcf.overcharge_iqr_flag,
  pl.potential_fraud::int AS label_provider_fraud_1_0
FROM mart.claim_features cf
LEFT JOIN mart.rule_claim_flags rcf USING (claimid)
LEFT JOIN curated.provider_labels pl ON pl.provider = cf.provider;
CREATE INDEX IF NOT EXISTS features_claim_provider_idx ON mart.features_claim(provider);
CREATE INDEX IF NOT EXISTS features_claim_claimid_idx  ON mart.features_claim(claimid);


SELECT
	'providers',
	COUNT(*)
FROM
	MART.FEATURES_PROVIDER
UNION ALL
SELECT
	'claims',
	COUNT(*)
FROM
	MART.FEATURES_CLAIM
UNION ALL
SELECT
	'fact_claim',
	COUNT(*)
FROM
	MART.FACT_CLAIM;

SELECT
	COUNT(*) FILTER (
		WHERE
			LABEL_PROVIDER_FRAUD_1_0 IS NOT NULL
	) AS LABELED_PROVIDERS
FROM
	MART.FEATURES_PROVIDER;