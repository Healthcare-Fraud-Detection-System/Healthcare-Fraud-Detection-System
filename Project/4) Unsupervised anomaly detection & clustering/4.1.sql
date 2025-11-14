-- 4) Unsupervised anomaly detection & clustering only at Claim Level

-- A) SQL — keep only fraudulent claims
-- 1A) Filter to fraudulent claims
DROP TABLE IF EXISTS mart.fraud_claims;
CREATE TABLE mart.fraud_claims AS
SELECT
  claimid, provider, beneid, claim_type,
  claim_start, claim_end, reimb_amt, deductible_paid,
  los_days, dx_count, px_count, drg,
  z_ip_reimb, z_ip_los, z_op_reimb,
  label_provider_fraud_1_0
FROM mart.features_claim
WHERE label_provider_fraud_1_0 = 1;

-- 1B) Helpful indexes
CREATE UNIQUE INDEX IF NOT EXISTS fraud_claims_pk ON mart.fraud_claims(claimid);
CREATE INDEX IF NOT EXISTS fraud_claims_type_idx ON mart.fraud_claims(claim_type);