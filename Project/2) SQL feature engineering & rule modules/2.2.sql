CREATE INDEX IF NOT EXISTS fact_claim_provider_start_bene_idx
  ON mart.fact_claim(provider, claim_start, beneid);

CREATE INDEX IF NOT EXISTS fact_claim_claim_start_brin
  ON mart.fact_claim USING BRIN (claim_start);


DROP TABLE IF EXISTS mart.code_dx CASCADE;
CREATE TABLE mart.code_dx AS
SELECT provider,
       claim_start::date AS day,
       dx_code AS code
FROM (
  SELECT provider, claim_start,
         UNNEST(ARRAY[dx1,dx2,dx3,dx4,dx5,dx6,dx7,dx8,dx9,dx10]) AS dx_code
  FROM mart.fact_claim
  WHERE claim_start IS NOT NULL
) u
WHERE dx_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS code_dx_prov_day_code_idx
  ON mart.code_dx(provider, day, code);
DROP TABLE IF EXISTS mart.code_px CASCADE;
CREATE TABLE mart.code_px AS
SELECT provider,
       claim_start::date AS day,
       px_code AS code
FROM (
  SELECT provider, claim_start,
         UNNEST(ARRAY[px1,px2,px3,px4,px5,px6]) AS px_code
  FROM mart.fact_claim
  WHERE claim_start IS NOT NULL
) u
WHERE px_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS code_px_prov_day_code_idx
  ON mart.code_px(provider, day, code);

DROP TABLE IF EXISTS mart.provider_daily CASCADE;
CREATE TABLE mart.provider_daily AS
WITH base AS (
  SELECT
    provider,
    claim_start::date AS day,
    COUNT(*)               AS claims_cnt,
    SUM(reimb_amt)         AS reimb_sum,
    SUM(deductible_paid)   AS deductible_sum,
    COUNT(DISTINCT beneid) AS unique_benes_daily
  FROM mart.fact_claim
  WHERE claim_start IS NOT NULL
  GROUP BY provider, claim_start::date
),
dx AS (
  SELECT provider, day, COUNT(DISTINCT code) AS dx_distinct_daily
  FROM mart.code_dx
  GROUP BY provider, day
),
px AS (
  SELECT provider, day, COUNT(DISTINCT code) AS px_distinct_daily
  FROM mart.code_px
  GROUP BY provider, day
)
SELECT
  b.provider, b.day,
  b.claims_cnt,
  b.reimb_sum,
  b.deductible_sum,
  b.unique_benes_daily,
  COALESCE(d.dx_distinct_daily,0) AS dx_distinct_daily,
  COALESCE(p.px_distinct_daily,0) AS px_distinct_daily
FROM base b
LEFT JOIN dx d USING (provider, day)
LEFT JOIN px p USING (provider, day);
CREATE INDEX IF NOT EXISTS pd_provider_day_idx ON mart.provider_daily(provider, day);

DROP TABLE IF EXISTS mart.provider_rollups CASCADE;
CREATE TABLE mart.provider_rollups AS
WITH days AS (
  SELECT
    provider, day,
    claims_cnt, reimb_sum, deductible_sum,
    unique_benes_daily, dx_distinct_daily, px_distinct_daily,
    SUM(claims_cnt) OVER (PARTITION BY provider ORDER BY day
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS claims_30d,
    SUM(claims_cnt) OVER (PARTITION BY provider ORDER BY day
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS claims_90d,
    SUM(claims_cnt) OVER (PARTITION BY provider ORDER BY day
      ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS claims_365d,
    SUM(reimb_sum)  OVER (PARTITION BY provider ORDER BY day
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS reimb_30d,
    SUM(reimb_sum)  OVER (PARTITION BY provider ORDER BY day
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS reimb_90d,
    SUM(reimb_sum)  OVER (PARTITION BY provider ORDER BY day
      ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS reimb_365d,
    AVG(NULLIF(reimb_sum,0)) OVER (PARTITION BY provider ORDER BY day
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_reimb_day_30d,
    SUM(deductible_sum) OVER (PARTITION BY provider ORDER BY day
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS deductible_30d
  FROM mart.provider_daily
),
benes AS (
  SELECT d.provider, d.day,
    (SELECT COUNT(DISTINCT f.beneid)
     FROM mart.fact_claim f
     WHERE f.provider = d.provider
       AND f.claim_start::date BETWEEN d.day - INTERVAL '29 days' AND d.day
    ) AS unique_benes_30d,
    (SELECT COUNT(DISTINCT f.beneid)
     FROM mart.fact_claim f
     WHERE f.provider = d.provider
       AND f.claim_start::date BETWEEN d.day - INTERVAL '89 days' AND d.day
    ) AS unique_benes_90d,
    (SELECT COUNT(DISTINCT f.beneid)
     FROM mart.fact_claim f
     WHERE f.provider = d.provider
       AND f.claim_start::date BETWEEN d.day - INTERVAL '364 days' AND d.day
    ) AS unique_benes_365d
  FROM mart.provider_daily d
),
codes AS (
  SELECT d.provider, d.day,
    (SELECT COUNT(DISTINCT code)
     FROM mart.code_dx x
     WHERE x.provider = d.provider
       AND x.day BETWEEN d.day - INTERVAL '29 days' AND d.day
    ) AS dx_distinct_30d,
    (SELECT COUNT(DISTINCT code)
     FROM mart.code_px x
     WHERE x.provider = d.provider
       AND x.day BETWEEN d.day - INTERVAL '29 days' AND d.day
    ) AS px_distinct_30d
  FROM mart.provider_daily d
)
SELECT
  d.provider, d.day,
  d.claims_30d, d.claims_90d, d.claims_365d,
  d.reimb_30d, d.reimb_90d, d.reimb_365d,
  d.avg_reimb_day_30d,
  b.unique_benes_30d, b.unique_benes_90d, b.unique_benes_365d,
  c.dx_distinct_30d, c.px_distinct_30d,
  CASE WHEN d.reimb_30d > 0 THEN d.deductible_30d / d.reimb_30d ELSE NULL END AS deductible_share_30d
FROM days d
JOIN benes b USING (provider, day)
JOIN codes c USING (provider, day);
CREATE INDEX IF NOT EXISTS pr_provider_day_idx ON mart.provider_rollups(provider, day);

DROP VIEW IF EXISTS mart.v_provider_mom_growth;
CREATE VIEW mart.v_provider_mom_growth AS
WITH m AS (
  SELECT provider, month_start,
         claims_cnt, reimb_sum,
         LAG(claims_cnt) OVER (PARTITION BY provider ORDER BY month_start) AS prev_claims,
         LAG(reimb_sum)  OVER (PARTITION BY provider ORDER BY month_start) AS prev_reimb
  FROM mart.mv_provider_monthly
)
SELECT provider, month_start,
       claims_cnt, reimb_sum,
       CASE WHEN prev_claims IS NULL OR prev_claims=0 THEN NULL
            ELSE (claims_cnt - prev_claims)::numeric / prev_claims END AS claims_mom,
       CASE WHEN prev_reimb IS NULL OR prev_reimb=0 THEN NULL
            ELSE (reimb_sum - prev_reimb)::numeric / prev_reimb END AS reimb_mom
FROM m;

ANALYZE MART.FACT_CLAIM;

ANALYZE MART.CODE_DX;

ANALYZE MART.CODE_PX;

ANALYZE MART.PROVIDER_DAILY;

ANALYZE MART.PROVIDER_ROLLUPS;

EXPLAIN
SELECT
	*
FROM
	MART.PROVIDER_ROLLUPS
WHERE
	PROVIDER = (
		SELECT
			PROVIDER
		FROM
			MART.PROVIDER_ROLLUPS
		LIMIT
			1
	)
	AND DAY BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE;