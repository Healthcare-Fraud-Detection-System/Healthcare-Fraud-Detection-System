-- 4) Unsupervised anomaly detection & clustering only at Claim Level

-- A) SQL — materialize the fraud-only claim tables

-- Drop & create fraud-only tables
DROP TABLE IF EXISTS mart.fraud_claims_ip;
CREATE TABLE mart.fraud_claims_ip AS
SELECT *
FROM mart.features_claim
WHERE label_provider_fraud_1_0 = 1
  AND claim_type = 'IP';

DROP TABLE IF EXISTS mart.fraud_claims_op;
CREATE TABLE mart.fraud_claims_op AS
SELECT *
FROM mart.features_claim
WHERE label_provider_fraud_1_0 = 1
  AND claim_type = 'OP';

-- Helpful row counts
SELECT 'IP_fraud_rows' AS what, COUNT(*) FROM mart.fraud_claims_ip
UNION ALL
SELECT 'OP_fraud_rows', COUNT(*) FROM mart.fraud_claims_op;
