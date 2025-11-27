DROP TABLE IF EXISTS test.features_claim;

CREATE TABLE test.features_claim AS
WITH
-- ============================
-- IP base for TEST claims
-- ============================
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

-- TRAIN-based DRG stats for IP (from mart.features_claim)
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
    ON b.drg = s.drg
),

-- ============================
-- OP base for TEST claims
-- ============================
op_base AS (
  SELECT
    o.claimid,
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

-- OP z-scores computed from TEST OP claims themselves (by primary_dx_prefix)
op_z AS (
  SELECT
    b.*,
    (
      (b.reimb_amt
       - AVG(b.reimb_amt) OVER (PARTITION BY b.primary_dx_prefix)
      ) / NULLIF(
            STDDEV_POP(b.reimb_amt) OVER (PARTITION BY b.primary_dx_prefix),
            0
          )
    )::double precision AS z_op_reimb
  FROM op_base b
),

-- ============================
-- Union IP + OP with flags
-- ============================
u AS (
  -- IP branch
  SELECT
    claimid, claim_type, provider, beneid, claim_start, claim_end,
    reimb_amt, deductible_paid, dx_count, px_count,
    primary_dx_prefix, drg,
    dow, is_weekend, month, quarter, los_days,
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
    0::int                                AS short_stay_flag,
    (CASE WHEN reimb_amt <= 0 THEN 1 ELSE 0 END)::int AS amount_zero_flag,
    0.0::double precision                 AS z_ip_reimb,
    0.0::double precision                 AS z_ip_los,
    COALESCE(z_op_reimb,0.0)              AS z_op_reimb
  FROM op_z
),

-- ============================
-- Duplicate / near-duplicate
-- ============================
dup_exact AS (
  SELECT
    provider, beneid, claim_start, claim_end, claim_type, reimb_amt,
    COUNT(*) AS cnt
  FROM u
  GROUP BY 1,2,3,4,5,6
),

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
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY reimb_amt) AS q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY reimb_amt) AS q3,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY reimb_amt)
    - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY reimb_amt) AS iqr
  FROM mart.features_claim
  WHERE claim_type = 'IP'
),
iq_op AS (
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
  u.claim_start,
  u.claim_end,

  COALESCE(u.reimb_amt,0)::numeric(12,2)       AS reimb_amt,
  COALESCE(u.deductible_paid,0)::numeric(12,2) AS deductible_paid,
  COALESCE(u.dx_count,0)::int                  AS dx_count,
  COALESCE(u.px_count,0)::int                  AS px_count,
  COALESCE(u.primary_dx_prefix,'')             AS primary_dx_prefix,
  COALESCE(u.drg,'')                           AS drg,
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
