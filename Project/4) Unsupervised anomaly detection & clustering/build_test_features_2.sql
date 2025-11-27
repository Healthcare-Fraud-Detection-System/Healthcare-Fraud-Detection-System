DROP TABLE IF EXISTS test.features_provider;

WITH asof AS (
  SELECT GREATEST(
    COALESCE((SELECT MAX(claim_end) FROM test.claims_inpatient), DATE '1900-01-01'),
    COALESCE((SELECT MAX(claim_end) FROM test.claims_outpatient), DATE '1900-01-01')
  ) AS asof
),
-- roll base from features_claim for windowed metrics
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

-- 30/90/365 windowed sums and unique counts
w AS (
  SELECT
    p.provider,
    -- windows: 30/90/365 relative to as-of
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
),

-- dx/px distinct (30d) from raw claim tables by exploding columns
dx30 AS (
  SELECT provider, COUNT(DISTINCT code) AS dx_distinct_30d
  FROM (
    SELECT i.provider, i.claim_end::date AS day, UNNEST(ARRAY[i.dx1,i.dx2,i.dx3,i.dx4,i.dx5,i.dx6,i.dx7,i.dx8,i.dx9,i.dx10]) AS code
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
  GROUP BY provider
),

-- 365-day IP/OP counts and z aggregates (use features_claim z-values)
z365 AS (
  SELECT
    p.provider,
    SUM( (fc.claim_type='IP')::int ) FILTER (WHERE fc.day > a.asof - INTERVAL '365 days' AND fc.day <= a.asof) AS ip_claims_365,
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
),

-- totals across ALL time for rule counts and claims_total
tot AS (
  SELECT
    p.provider,
    COUNT(*)                                  AS claims_total,
    SUM(fc.dup_exact_flag)::int               AS dup_exact_claims,
    SUM(fc.dup_near_count)::int               AS dup_near_total,
    SUM(CASE WHEN fc.claim_type='IP' THEN fc.upcoding_ip_flag ELSE 0 END)::int AS upcoding_ip_claims,
    SUM(CASE WHEN fc.claim_type='OP' THEN fc.upcoding_op_flag ELSE 0 END)::int AS upcoding_op_claims,
    SUM(fc.overcharge_z_flag)::int            AS overcharge_z_claims,
    SUM(fc.overcharge_iqr_flag)::int          AS overcharge_iqr_claims
  FROM (SELECT DISTINCT provider FROM test.provider) p
  LEFT JOIN fc ON fc.provider = p.provider
  GROUP BY p.provider
)

SELECT
  /* === EXACT COLUMN ORDER of mart.features_provider === */
  p.provider,

  COALESCE(w.claims_30d,0)::int  AS claims_30d,
  COALESCE(w.claims_90d,0)::int  AS claims_90d,
  COALESCE(w.claims_365d,0)::int AS claims_365d,

  COALESCE(w.reimb_30d,0)::numeric(14,2)  AS reimb_30d,
  COALESCE(w.reimb_90d,0)::numeric(14,2)  AS reimb_90d,
  COALESCE(w.reimb_365d,0)::numeric(14,2) AS reimb_365d,

  /* avg_reimb_day_30d = reimb_30d / 30 (fixed window length) */
  (COALESCE(w.reimb_30d,0) / 30.0)::numeric(14,2) AS avg_reimb_day_30d,

  COALESCE(w.unique_benes_30d,0)::int    AS unique_benes_30d,
  COALESCE(w.unique_benes_90d,0)::int    AS unique_benes_90d,
  COALESCE(w.unique_benes_365d,0)::int   AS unique_benes_365d,

  COALESCE(dx30.dx_distinct_30d,0)::int  AS dx_distinct_30d,
  COALESCE(px30.px_distinct_30d,0)::int  AS px_distinct_30d,

  /* deductible_share_30d = sum(deductible) / sum(reimb) over 30d */
  (CASE WHEN COALESCE(w.reimb_30d,0) > 0
        THEN (SELECT COALESCE(SUM(fc.deductible_paid),0)::numeric(14,2)
              FROM fc, asof a
              WHERE fc.provider=p.provider
                AND fc.day > a.asof - INTERVAL '30 days' AND fc.day <= a.asof) / w.reimb_30d
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
        THEN (COALESCE(w.reimb_30d,0) / w.claims_30d)
        ELSE 0 END)::numeric(12,2) AS avg_reimb_per_claim

INTO test.features_provider
FROM (SELECT DISTINCT provider FROM test.provider) p
LEFT JOIN w     ON w.provider  = p.provider
LEFT JOIN dx30  ON dx30.provider = p.provider
LEFT JOIN px30  ON px30.provider = p.provider
LEFT JOIN z365  ON z365.provider = p.provider
LEFT JOIN tot   ON tot.provider  = p.provider
;