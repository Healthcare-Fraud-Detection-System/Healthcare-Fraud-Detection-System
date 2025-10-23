-- 2.4) Build rule-detection modules: @Duplicate detection: exact/near duplicates, @Upcoding/misrepresentation: logical rules comparing code intensity vs peers, @Overcharging: detect outlier charge amounts after normalization
-- A) Duplicate detection (exact & near)
-- A1) EXACT duplicate (bene+provider+service date+amount appear >1 time)
-- DROP TABLE IF EXISTS MART.RULE_DUP_EXACT CASCADE;
-- CREATE TABLE MART.RULE_DUP_EXACT AS
-- SELECT
-- 	CLAIMID,
-- 	CASE
-- 		WHEN COUNT(*) OVER (
-- 			PARTITION BY
-- 				BENEID,
-- 				PROVIDER,
-- 				CLAIM_START,
-- 				REIMB_AMT
-- 		) > 1 THEN 1
-- 		ELSE 0
-- 	END AS DUP_EXACT_FLAG
-- FROM
-- 	MART.FACT_CLAIM;
-- -- A2) NEAR duplicate (±1 day, ~same amount)
-- DROP TABLE IF EXISTS MART.RULE_DUP_NEAR CASCADE;
-- CREATE TABLE MART.RULE_DUP_NEAR AS
-- SELECT
-- 	F.CLAIMID,
-- 	COUNT(*) FILTER (
-- 		WHERE
-- 			F2.CLAIMID <> F.CLAIMID
-- 	) AS DUP_NEAR_COUNT
-- FROM
-- 	MART.FACT_CLAIM F
-- 	JOIN MART.FACT_CLAIM F2 ON F2.BENEID = F.BENEID
-- 	AND F2.PROVIDER = F.PROVIDER
-- 	AND F2.CLAIM_START BETWEEN F.CLAIM_START - INTERVAL '1 day' AND F.CLAIM_START  + INTERVAL '1 day'
-- 	AND F2.REIMB_AMT IS NOT NULL
-- 	AND F.REIMB_AMT IS NOT NULL
-- 	AND ABS(F2.REIMB_AMT - F.REIMB_AMT) <= 0.01 * GREATEST(F2.REIMB_AMT, F.REIMB_AMT)
-- GROUP BY
-- 	F.CLAIMID;
-- CREATE INDEX IF NOT EXISTS RULE_DUP_NEAR_IDX ON MART.RULE_DUP_NEAR (DUP_NEAR_COUNT);
-- B) Upcoding / misrepresentation
-- B1) Claim-level z-scores vs peers (IP by DRG; OP by dx-prefix)
-- DROP TABLE IF EXISTS mart.rule_claim_z CASCADE;
-- CREATE TABLE mart.rule_claim_z AS
-- WITH ip AS (
--   SELECT f.claimid,
--          CASE WHEN p.peer_reimb_sd_365 > 0
--               THEN (LEAST(f.reimb_amt, p.peer_reimb_mean_365 + 3*p.peer_reimb_sd_365) - p.peer_reimb_mean_365) / p.peer_reimb_sd_365
--               ELSE NULL END AS z_ip_reimb,
--          CASE WHEN p.peer_los_sd_365 > 0 AND f.admit_dt IS NOT NULL AND f.discharge_dt IS NOT NULL
--               THEN ((GREATEST((f.discharge_dt - f.admit_dt),0)) - p.peer_los_mean_365) / p.peer_los_sd_365
--               ELSE NULL END AS z_ip_los
--   FROM mart.fact_claim f
--   JOIN mart.ip_drg_peer_365 p ON p.drg = f.drg
--   WHERE f.claim_type='IP' AND f.reimb_amt IS NOT NULL AND f.drg IS NOT NULL
-- ),
-- op AS (
--   SELECT f.claimid,
--          CASE WHEN p.peer_reimb_sd_365 > 0
--               THEN (LEAST(f.reimb_amt, p.peer_reimb_mean_365 + 3*p.peer_reimb_sd_365) - p.peer_reimb_mean_365) / p.peer_reimb_sd_365
--               ELSE NULL END AS z_op_reimb
--   FROM mart.fact_claim f
--   JOIN mart.op_prefix_peer_365 p ON p.dx_prefix = substr(f.dx1,1,1)
--   WHERE f.claim_type='OP' AND f.reimb_amt IS NOT NULL AND f.dx1 IS NOT NULL
-- )
-- SELECT
--   f.claimid,
--   i.z_ip_reimb, i.z_ip_los,
--   o.z_op_reimb
-- FROM mart.fact_claim f
-- LEFT JOIN ip i USING (claimid)
-- LEFT JOIN op o USING (claimid);
-- -- B2) Upcoding flags (thresholds tuned to catch extreme tails)
-- DROP TABLE IF EXISTS mart.rule_upcoding CASCADE;
-- CREATE TABLE mart.rule_upcoding AS
-- SELECT
--   cf.claimid,
--   -- IP: high pay vs DRG peers + short stay by peers or Two-Midnight proxy
--   CASE WHEN cf.claim_type='IP'
--             AND rc.z_ip_reimb IS NOT NULL AND rc.z_ip_reimb > 2.5
--             AND (
--                  (rc.z_ip_los IS NOT NULL AND rc.z_ip_los < -0.5)
--                  OR (cf.short_stay_flag = 1)  -- <2 midnights proxy
--                 )
--        THEN 1 ELSE 0 END AS upcoding_ip_flag,
--   -- OP: very high pay vs dx-prefix peers
--   CASE WHEN cf.claim_type='OP'
--             AND rc.z_op_reimb IS NOT NULL AND rc.z_op_reimb > 3
--        THEN 1 ELSE 0 END AS upcoding_op_flag
-- FROM mart.claim_features cf
-- LEFT JOIN mart.rule_claim_z rc USING (claimid);
-- CREATE INDEX IF NOT EXISTS rule_upcoding_flags_idx ON mart.rule_upcoding(upcoding_ip_flag, upcoding_op_flag);
-- C) Overcharging / outlier amounts after normalization
-- C1) IQR fences by peer group (IP: DRG; OP: dx-prefix)
-- DROP TABLE IF EXISTS mart.rule_peer_iqr CASCADE;
-- CREATE TABLE mart.rule_peer_iqr AS
-- WITH ip AS (
--   SELECT 'IP'::text AS claim_type, drg AS peer_key,
--          PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY reimb_amt) AS q1,
--          PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY reimb_amt) AS q3
--   FROM mart.fact_claim
--   WHERE claim_type='IP' AND reimb_amt IS NOT NULL AND drg IS NOT NULL
--   GROUP BY drg
-- ),
-- op AS (
--   SELECT 'OP'::text AS claim_type, substr(dx1,1,1) AS peer_key,
--          PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY reimb_amt) AS q1,
--          PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY reimb_amt) AS q3
--   FROM mart.fact_claim
--   WHERE claim_type='OP' AND reimb_amt IS NOT NULL AND dx1 IS NOT NULL
--   GROUP BY substr(dx1,1,1)
-- )
-- SELECT * FROM ip
-- UNION ALL
-- SELECT * FROM op;
-- -- C2) Claim-level overcharge flags using either z or IQR
-- DROP TABLE IF EXISTS mart.rule_overcharge CASCADE;
-- CREATE TABLE mart.rule_overcharge AS
-- WITH f AS (
--   SELECT fc.claimid, fc.claim_type, fc.reimb_amt, fc.drg, fc.dx1,
--          rc.z_ip_reimb, rc.z_op_reimb
--   FROM mart.fact_claim fc
--   LEFT JOIN mart.rule_claim_z rc USING (claimid)
-- ),
-- joined AS (
--   SELECT f.*,
--          CASE WHEN f.claim_type='IP' THEN i.q1 ELSE o.q1 END AS q1,
--          CASE WHEN f.claim_type='IP' THEN i.q3 ELSE o.q3 END AS q3
--   FROM f
--   LEFT JOIN mart.rule_peer_iqr i ON (f.claim_type='IP' AND i.peer_key = f.drg)
--   LEFT JOIN mart.rule_peer_iqr o ON (f.claim_type='OP' AND o.peer_key = substr(f.dx1,1,1))
-- ),
-- fences AS (
--   SELECT *,
--          (q3 - q1) AS iqr,
--          (q3 + 1.5*(q3 - q1)) AS high_fence
--   FROM joined
-- )
-- SELECT
--   claimid,
--   -- z-based outlier
--   CASE WHEN (z_ip_reimb IS NOT NULL AND z_ip_reimb > 3)
--          OR (z_op_reimb IS NOT NULL AND z_op_reimb > 3)
--        THEN 1 ELSE 0 END AS overcharge_z_flag,
--   -- IQR-based outlier (beyond upper fence)
--   CASE WHEN reimb_amt IS NOT NULL AND iqr IS NOT NULL AND reimb_amt > high_fence
--        THEN 1 ELSE 0 END AS overcharge_iqr_flag
-- FROM fences;
-- CREATE INDEX IF NOT EXISTS rule_overcharge_idx ON mart.rule_overcharge(overcharge_z_flag, overcharge_iqr_flag);
-- D) Assemble: one claim-level rules table + provider roll-up
-- D1) Claim-level rule flags (one row per claim)
DROP TABLE IF EXISTS MART.RULE_CLAIM_FLAGS CASCADE;

CREATE TABLE MART.RULE_CLAIM_FLAGS AS
SELECT
	F.CLAIMID,
	COALESCE(DE.DUP_EXACT_FLAG, 0) AS DUP_EXACT_FLAG,
	COALESCE(DN.DUP_NEAR_COUNT, 0) AS DUP_NEAR_COUNT,
	COALESCE(U.UPCODING_IP_FLAG, 0) AS UPCODING_IP_FLAG,
	COALESCE(U.UPCODING_OP_FLAG, 0) AS UPCODING_OP_FLAG,
	COALESCE(O.OVERCHARGE_Z_FLAG, 0) AS OVERCHARGE_Z_FLAG,
	COALESCE(O.OVERCHARGE_IQR_FLAG, 0) AS OVERCHARGE_IQR_FLAG
FROM
	MART.FACT_CLAIM F
	LEFT JOIN MART.RULE_DUP_EXACT DE USING (CLAIMID)
	LEFT JOIN MART.RULE_DUP_NEAR DN USING (CLAIMID)
	LEFT JOIN MART.RULE_UPCODING U USING (CLAIMID)
	LEFT JOIN MART.RULE_OVERCHARGE O USING (CLAIMID);

CREATE INDEX IF NOT EXISTS RULE_CLAIM_FLAGS_IDX ON MART.RULE_CLAIM_FLAGS (
	DUP_EXACT_FLAG,
	UPCODING_IP_FLAG,
	UPCODING_OP_FLAG,
	OVERCHARGE_Z_FLAG
);

-- D2) Provider-level summary of rule hits (useful for 2.5 feature join)
DROP TABLE IF EXISTS MART.RULE_PROVIDER_FLAGS CASCADE;

CREATE TABLE MART.RULE_PROVIDER_FLAGS AS
SELECT
	F.PROVIDER,
	COUNT(*) AS CLAIMS_TOTAL,
	SUM(
		CASE
			WHEN DUP_EXACT_FLAG = 1 THEN 1
			ELSE 0
		END
	) AS DUP_EXACT_CLAIMS,
	SUM(DUP_NEAR_COUNT) AS DUP_NEAR_TOTAL,
	SUM(
		CASE
			WHEN UPCODING_IP_FLAG = 1 THEN 1
			ELSE 0
		END
	) AS UPCODING_IP_CLAIMS,
	SUM(
		CASE
			WHEN UPCODING_OP_FLAG = 1 THEN 1
			ELSE 0
		END
	) AS UPCODING_OP_CLAIMS,
	SUM(
		CASE
			WHEN OVERCHARGE_Z_FLAG = 1 THEN 1
			ELSE 0
		END
	) AS OVERCHARGE_Z_CLAIMS,
	SUM(
		CASE
			WHEN OVERCHARGE_IQR_FLAG = 1 THEN 1
			ELSE 0
		END
	) AS OVERCHARGE_IQR_CLAIMS
FROM
	MART.FACT_CLAIM F
	JOIN MART.RULE_CLAIM_FLAGS R USING (CLAIMID)
GROUP BY
	F.PROVIDER;

CREATE INDEX IF NOT EXISTS RULE_PROVIDER_FLAGS_IDX ON MART.RULE_PROVIDER_FLAGS (PROVIDER);