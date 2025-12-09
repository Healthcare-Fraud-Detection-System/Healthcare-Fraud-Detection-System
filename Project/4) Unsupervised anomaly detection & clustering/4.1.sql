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

SELECT 'IP_fraud_rows' AS what, COUNT(*) FROM mart.fraud_claims_ip
UNION ALL
SELECT 'OP_fraud_rows', COUNT(*) FROM mart.fraud_claims_op;
