-- build_test_features_0

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
CREATE INDEX IF NOT EXISTS baseline_op_dxprefix_stats_idx
  ON mart.baseline_op_dxprefix_stats(primary_dx_prefix);

DROP MATERIALIZED VIEW IF EXISTS mart.baseline_claimtype_p99;
SELECT
  claim_type,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY reimb_amt)::numeric(14,6) AS p99_reimb
FROM mart.features_claim
WHERE reimb_amt IS NOT NULL
GROUP BY claim_type;

CREATE INDEX IF NOT EXISTS baseline_claimtype_p99_idx
  ON mart.baseline_claimtype_p99(claim_type);



--------------------------------- 
---------------------------------

-- build_test_features_1

DROP TABLE IF EXISTS test.features_claim;

CREATE TABLE test.features_claim AS
WITH
ip_base AS (
  SELECT
    i.claimid,
    'IP'::text AS claim_type,
    i.provider,
    i.beneid,
    i.claim_start,
    i.claim_end,
    COALESCE(i.reimb_amt,0)::numeric(12,2)       AS reimb_amt,
    COALESCE(i.deductible_paid,0)::numeric(12,2) AS deductible_paid,

    -- counts of dx/px present
    ((i.dx1 IS NOT NULL)::int + (i.dx2 IS NOT NULL)::int + (i.dx3 IS NOT NULL)::int +
     (i.dx4 IS NOT NULL)::int + (i.dx5 IS NOT NULL)::int + (i.dx6 IS NOT NULL)::int +
     (i.dx7 IS NOT NULL)::int + (i.dx8 IS NOT NULL)::int + (i.dx9 IS NOT NULL)::int +
     (i.dx10 IS NOT NULL)::int) AS dx_count,

    ((i.px1 IS NOT NULL)::int + (i.px2 IS NOT NULL)::int + (i.px3 IS NOT NULL)::int +
     (i.px4 IS NOT NULL)::int + (i.px5 IS NOT NULL)::int + (i.px6 IS NOT NULL)::int) AS px_count,

    SUBSTRING(i.dx1 FROM 1 FOR 3) AS primary_dx_prefix,
    COALESCE(i.drg,'')            AS drg,

    EXTRACT(DOW     FROM i.claim_start)::int AS dow,
    (EXTRACT(DOW    FROM i.claim_start)::int IN (0,6))::int AS is_weekend,
    EXTRACT(MONTH   FROM i.claim_start)::int AS month,
    EXTRACT(QUARTER FROM i.claim_start)::int AS quarter,

    GREATEST(0, (i.claim_end - i.claim_start))::int AS los_days
  FROM test.claims_inpatient i
),

ip_stats AS (
  SELECT
    drg,
    AVG(reimb_amt)        AS mean_reimb,
    STDDEV_POP(reimb_amt) AS std_reimb,
    AVG(los_days)         AS mean_los,
    STDDEV_POP(los_days)  AS std_los
  FROM mart.features_claim
  WHERE claim_type = 'IP'
  GROUP BY drg
),

ip_z AS (
  SELECT
    b.*,
    ((b.reimb_amt - s.mean_reimb) / NULLIF(s.std_reimb,0))::double precision AS z_ip_reimb,
    ((b.los_days  - s.mean_los)   / NULLIF(s.std_los,0))::double precision   AS z_ip_los
  FROM ip_base b
  LEFT JOIN ip_stats s
  LEFT JOIN ip_stats s
    ON b.drg = s.drg
),

op_base AS (
  SELECTaimid,
    'OP'::text AS claim_type,
    o.provider,
    o.beneid,
    o.claim_start,
    o.claim_end,
    COALESCE(o.reimb_amt,0)::numeric(12,2)       AS reimb_amt,
    COALESCE(o.deductible_paid,0)::numeric(12,2) AS deductible_paid,

    ((o.dx1 IS NOT NULL)::int + (o.dx2 IS NOT NULL)::int + (o.dx3 IS NOT NULL)::int +
     (o.dx4 IS NOT NULL)::int + (o.dx5 IS NOT NULL)::int + (o.dx6 IS NOT NULL)::int +
     (o.dx7 IS NOT NULL)::int + (o.dx8 IS NOT NULL)::int + (o.dx9 IS NOT NULL)::int +
     (o.dx10 IS NOT NULL)::int) AS dx_count,

    ((o.px1 IS NOT NULL)::int + (o.px2 IS NOT NULL)::int + (o.px3 IS NOT NULL)::int +
     (o.px4 IS NOT NULL)::int + (o.px5 IS NOT NULL)::int + (o.px6 IS NOT NULL)::int) AS px_count,

    SUBSTRING(o.dx1 FROM 1 FOR 3) AS primary_dx_prefix,
    ''::text                      AS drg,

    EXTRACT(DOW     FROM o.claim_start)::int AS dow,
    (EXTRACT(DOW    FROM o.claim_start)::int IN (0,6))::int AS is_weekend,
    EXTRACT(MONTH   FROM o.claim_start)::int AS month,
    EXTRACT(QUARTER FROM o.claim_start)::int AS quarter,

    0::int AS los_days
  FROM test.claims_outpatient o
),
    0::int AS los_days
  FROM test.claims_outpatient o
),

op_z AS (
  SELECT.reimb_amt
       - AVG(b.reimb_amt) OVER (PARTITION BY b.primary_dx_prefix)
      ) / NULLIF(
            STDDEV_POP(b.reimb_amt) OVER (PARTITION BY b.primary_dx_prefix),
            0
          )
    )::double precision AS z_op_reimb
  FROM op_base b
),

    )::double precision AS z_op_reimb
  FROM op_base b
),

u AS (
  SELECT is_weekend, month, quarter, los_days,
    (CASE WHEN claim_type='IP' AND los_days < 2 THEN 1 ELSE 0 END)::int AS short_stay_flag,
    (CASE WHEN reimb_amt <= 0 THEN 1 ELSE 0 END)::int                  AS amount_zero_flag,
    COALESCE(z_ip_reimb,0.0)              AS z_ip_reimb,
    COALESCE(z_ip_los,0.0)                AS z_ip_los,
    0.0::double precision                 AS z_op_reimb
  FROM ip_z

  UNION ALL

  -- OP branch
  SELECT
    claimid, claim_type, provider, beneid, claim_start, claim_end,
    reimb_amt, deductible_paid, dx_count, px_count,
    primary_dx_prefix, drg,
    dow, is_weekend, month, quarter, los_days,
  UNION ALL

  SELECT
    claimid, claim_type, provider, beneid, claim_start, claim_end,
  FROM op_z
),

-- ============================
-- Duplicate / near-duplicate
    COALESCE(z_op_reimb,0.0)              AS z_op_reimb
  FROM op_z
),

dup_exact AS (
  SELECT
dup_near AS (
  SELECT
    provider,
    beneid,
    claim_start::date AS day_key,
    COUNT(*) AS cnt
  FROM u
  GROUP BY 1,2,3
),

-- ============================
-- IQR thresholds from TRAIN (computed inline)
-- ============================
iq_ip AS (
  SELECT
    1 AS k,
  GROUP BY 1,2,3
),

iq_ip AS (
  SELECT (
  SELECT
    1 AS k,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY reimb_amt) AS q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY reimb_amt) AS q3,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY reimb_amt)
    - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY reimb_amt) AS iqr
  FROM mart.features_claim
  WHERE claim_type = 'OP'
)

-- ============================
-- Final SELECT: exact column order
-- ============================
SELECT
  u.claimid,
  u.claim_type,
  u.provider,
  u.beneid,
  FROM mart.features_claim
  WHERE claim_type = 'OP'
)

SELECT
  u.claimid,.drg,'')                           AS drg,
  COALESCE(u.dow,0)::int                       AS dow,
  COALESCE(u.is_weekend,0)::int                AS is_weekend,
  COALESCE(u.month,0)::int                     AS month,
  COALESCE(u.quarter,0)::int                   AS quarter,
  COALESCE(u.los_days,0)::int                  AS los_days,
  COALESCE(u.short_stay_flag,0)::int           AS short_stay_flag,
  COALESCE(u.amount_zero_flag,0)::int          AS amount_zero_flag,

  -- high amount via z-score
  (CASE
     WHEN (u.claim_type='IP' AND u.z_ip_reimb > 3)
       OR (u.claim_type='OP' AND u.z_op_reimb > 3)
     THEN 1 ELSE 0
   END)::int AS amount_high_flag,

  -- duplicate flags
  (CASE WHEN dx.cnt > 1 THEN 1 ELSE 0 END)::int AS exact_duplicate_flag,
  COALESCE(dn.cnt,1)::int                        AS near_duplicate_group_size,
  (CASE WHEN dx.cnt > 1 THEN 1 ELSE 0 END)::int AS dup_exact_flag,
  GREATEST(COALESCE(dn.cnt,1)-1,0)::int          AS dup_near_count,

  -- upcoding proxies
  (CASE
     WHEN u.claim_type='IP' AND u.short_stay_flag=1 AND u.z_ip_reimb > 3
     THEN 1 ELSE 0
   END)::int AS upcoding_ip_flag,

  (CASE
     WHEN u.claim_type='OP' AND u.z_op_reimb > 3
     THEN 1 ELSE 0
   END)::int AS upcoding_op_flag,

  -- overcharge flags
  (CASE
     WHEN (u.claim_type='IP' AND u.z_ip_reimb > 3)
       OR (u.claim_type='OP' AND u.z_op_reimb > 3)
     THEN 1 ELSE 0
   END)::int AS overcharge_z_flag,

  (CASE
     WHEN u.claim_type='IP'
          AND EXISTS (SELECT 1 FROM iq_ip)
          AND u.reimb_amt > (SELECT q3 + 1.5*iqr FROM iq_ip)
       THEN 1
     WHEN u.claim_type='OP'
          AND EXISTS (SELECT 1 FROM iq_op)
          AND u.reimb_amt > (SELECT q3 + 1.5*iqr FROM iq_op)
       THEN 1
     ELSE 0
   END)::int AS overcharge_iqr_flag,

  COALESCE(u.z_ip_reimb,0.0)::double precision AS z_ip_reimb,
  COALESCE(u.z_ip_los,0.0)::double precision   AS z_ip_los,
  COALESCE(u.z_op_reimb,0.0)::double precision AS z_op_reimb

FROM u
LEFT JOIN dup_exact dx
  ON dx.provider    = u.provider
 AND dx.beneid      = u.beneid
 AND dx.claim_start = u.claim_start
 AND dx.claim_end   = u.claim_end
 AND dx.claim_type  = u.claim_type
 AND dx.reimb_amt   = u.reimb_amt
LEFT JOIN dup_near dn
  ON dn.provider = u.provider
 AND dn.beneid   = u.beneid
 AND dn.day_key  = u.claim_start::date;


--------------------------------- 
---------------------------------

-- build_test_features_2

DROP TABLE IF EXISTS test.features_provider;

WITH asof AS (
  SELECT GREATEST(
    COALESCE((SELECT MAX(claim_end) FROM test.claims_inpatient), DATE '1900-01-01'),
    COALESCE((SELECT MAX(claim_end) FROM test.claims_outpatient), DATE '1900-01-01')
  ) AS asof
),
fc AS (
  SELECT
    f.provider,
    f.beneid,
    f.claim_type,
    f.claim_end::date AS day,
    f.reimb_amt,
    f.deductible_paid,
    f.primary_dx_prefix,
    f.z_ip_reimb, f.z_ip_los, f.z_op_reimb,
    f.dup_exact_flag, f.dup_near_count,
    f.upcoding_ip_flag, f.upcoding_op_flag,
    f.overcharge_z_flag, f.overcharge_iqr_flag
  FROM test.features_claim f
),

w AS (
  SELECT
    p.provider,
    SUM(1) FILTER (WHERE fc.day > a.asof - INTERVAL '30 days'  AND fc.day <= a.asof) AS claims_30d,
    SUM(1) FILTER (WHERE fc.day > a.asof - INTERVAL '90 days'  AND fc.day <= a.asof) AS claims_90d,
    SUM(1) FILTER (WHERE fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS claims_365d,

    SUM(fc.reimb_amt) FILTER (WHERE fc.day > a.asof - INTERVAL '30 days'  AND fc.day <= a.asof) AS reimb_30d,
    SUM(fc.reimb_amt) FILTER (WHERE fc.day > a.asof - INTERVAL '90 days'  AND fc.day <= a.asof) AS reimb_90d,
    SUM(fc.reimb_amt) FILTER (WHERE fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS reimb_365d,

    COUNT(DISTINCT fc.beneid) FILTER (WHERE fc.day > a.asof - INTERVAL '30 days'  AND fc.day <= a.asof) AS unique_benes_30d,
    COUNT(DISTINCT fc.beneid) FILTER (WHERE fc.day > a.asof - INTERVAL '90 days'  AND fc.day <= a.asof) AS unique_benes_90d,
    COUNT(DISTINCT fc.beneid) FILTER (WHERE fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS unique_benes_365d
  FROM (SELECT DISTINCT provider FROM test.provider) p
  LEFT JOIN fc ON fc.provider = p.provider
  CROSS JOIN asof a
  GROUP BY p.provider, a.asof
  CROSS JOIN asof a
  GROUP BY p.provider, a.asof
),

dx30 AS (
  SELECT provider, COUNT(DISTINCT code) AS dx_distinct_30dRAY[i.dx1,i.dx2,i.dx3,i.dx4,i.dx5,i.dx6,i.dx7,i.dx8,i.dx9,i.dx10]) AS code
    FROM test.claims_inpatient i
    UNION ALL
    SELECT o.provider, o.claim_end::date AS day, UNNEST(ARRAY[o.dx1,o.dx2,o.dx3,o.dx4,o.dx5,o.dx6,o.dx7,o.dx8,o.dx9,o.dx10]) AS code
    FROM test.claims_outpatient o
  ) t
  JOIN asof a ON TRUE
  WHERE t.day > a.asof - INTERVAL '30 days' AND t.day <= a.asof
    AND code IS NOT NULL AND code <> ''
  GROUP BY provider
),
px30 AS (
  SELECT provider, COUNT(DISTINCT code) AS px_distinct_30d
  FROM (
    SELECT i.provider, i.claim_end::date AS day, UNNEST(ARRAY[i.px1,i.px2,i.px3,i.px4,i.px5,i.px6]) AS code
    FROM test.claims_inpatient i
    UNION ALL
    SELECT o.provider, o.claim_end::date AS day, UNNEST(ARRAY[o.px1,o.px2,o.px3,o.px4,o.px5,o.px6]) AS code
    FROM test.claims_outpatient o
  ) t
  JOIN asof a ON TRUE
  WHERE t.day > a.asof - INTERVAL '30 days' AND t.day <= a.asof
    AND code IS NOT NULL AND code <> ''
    AND code IS NOT NULL AND code <> ''
  GROUP BY provider
),

z365 AS (
  SELECT
    p.provider,aim_type='IP')::int ) FILTER (WHERE fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS ip_claims_365,
    AVG(fc.z_ip_reimb) FILTER (WHERE fc.claim_type='IP' AND fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS ip_avg_z_reimb_365,
    MAX(fc.z_ip_reimb) FILTER (WHERE fc.claim_type='IP' AND fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS ip_max_z_reimb_365,
    AVG(fc.z_ip_los)    FILTER (WHERE fc.claim_type='IP' AND fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS ip_avg_z_los_365,
    MAX(fc.z_ip_los)    FILTER (WHERE fc.claim_type='IP' AND fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS ip_max_z_los_365,

    SUM( (fc.claim_type='OP')::int ) FILTER (WHERE fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS op_claims_365,
    AVG(fc.z_op_reimb) FILTER (WHERE fc.claim_type='OP' AND fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS op_avg_z_reimb_365,
    MAX(fc.z_op_reimb) FILTER (WHERE fc.claim_type='OP' AND fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS op_max_z_reimb_365
  FROM (SELECT DISTINCT provider FROM test.provider) p
  LEFT JOIN fc ON fc.provider = p.provider
  CROSS JOIN asof a
  GROUP BY p.provider, a.asof
  CROSS JOIN asof a
  GROUP BY p.provider, a.asof
),

tot AS (
  SELECT
    p.provider,exact_flag)::int               AS dup_exact_claims,
    SUM(fc.dup_near_count)::int               AS dup_near_total,
    SUM(CASE WHEN fc.claim_type='IP' THEN fc.upcoding_ip_flag ELSE 0 END)::int AS upcoding_ip_claims,
    SUM(CASE WHEN fc.claim_type='OP' THEN fc.upcoding_op_flag ELSE 0 END)::int AS upcoding_op_claims,
    SUM(fc.overcharge_z_flag)::int            AS overcharge_z_claims,
    SUM(fc.overcharge_iqr_flag)::int          AS overcharge_iqr_claims
  FROM (SELECT DISTINCT provider FROM test.provider) p
  LEFT JOIN fc ON fc.provider = p.provider
  GROUP BY p.provider
)
  GROUP BY p.provider
)

SELECT
  p.provider,

  COALESCE(w.claims_30d,0)::int  AS claims_30d,,

  COALESCE(w.reimb_30d,0)::numeric(14,2)  AS reimb_30d,
  COALESCE(w.reimb_90d,0)::numeric(14,2)  AS reimb_90d,
  COALESCE(w.reimb_365d,0)::numeric(14,2) AS reimb_365d,

  /* avg_reimb_day_30d = reimb_30d / 30 (fixed window length) */
  COALESCE(w.reimb_365d,0)::numeric(14,2) AS reimb_365d,

  (COALESCE(w.reimb_30d,0) / 30.0)::numeric(14,2) AS avg_reimb_day_30d,

  COALESCE(w.unique_benes_30d,0)::int    AS unique_benes_30d,
  COALESCE(dx30.dx_distinct_30d,0)::int  AS dx_distinct_30d,
  COALESCE(px30.px_distinct_30d,0)::int  AS px_distinct_30d,

  /* deductible_share_30d = sum(deductible) / sum(reimb) over 30d */
  COALESCE(px30.px_distinct_30d,0)::int  AS px_distinct_30d,

  (CASE WHEN COALESCE(w.reimb_30d,0) > 0
        THEN (SELECT COALESCE(SUM(fc.deductible_paid),0)::numeric(14,2)a.asof) / w.reimb_30d
        ELSE 0 END)::numeric(8,4) AS deductible_share_30d,

  COALESCE(z365.ip_claims_365,0)::int                 AS ip_claims_365,
  COALESCE(z365.ip_avg_z_reimb_365,0)::double precision AS ip_avg_z_reimb_365,
  COALESCE(z365.ip_max_z_reimb_365,0)::double precision AS ip_max_z_reimb_365,
  COALESCE(z365.ip_avg_z_los_365,0)::double precision   AS ip_avg_z_los_365,
  COALESCE(z365.ip_max_z_los_365,0)::double precision   AS ip_max_z_los_365,

  COALESCE(z365.op_claims_365,0)::int                 AS op_claims_365,
  COALESCE(z365.op_avg_z_reimb_365,0)::double precision AS op_avg_z_reimb_365,
  COALESCE(z365.op_max_z_reimb_365,0)::double precision AS op_max_z_reimb_365,

  COALESCE(tot.claims_total,0)::int AS claims_total,
  COALESCE(tot.dup_exact_claims,0)::int AS dup_exact_claims,
  COALESCE(tot.dup_near_total,0)::int   AS dup_near_total,
  COALESCE(tot.upcoding_ip_claims,0)::int AS upcoding_ip_claims,
  COALESCE(tot.upcoding_op_claims,0)::int AS upcoding_op_claims,
  COALESCE(tot.overcharge_z_claims,0)::int AS overcharge_z_claims,
  COALESCE(tot.overcharge_iqr_claims,0)::int AS overcharge_iqr_claims,

  /* avg_reimb_per_claim = reimb_30d / claims_30d */
  (CASE WHEN COALESCE(w.claims_30d,0) > 0
  COALESCE(tot.overcharge_iqr_claims,0)::int AS overcharge_iqr_claims,

  (CASE WHEN COALESCE(w.claims_30d,0) > 0
        THEN (COALESCE(w.reimb_30d,0) / w.claims_30d)
LEFT JOIN w     ON w.provider  = p.provider
LEFT JOIN dx30  ON dx30.provider = p.provider
LEFT JOIN px30  ON px30.provider = p.provider
LEFT JOIN z365  ON z365.provider = p.provider
LEFT JOIN tot   ON tot.provider  = p.provider
;