DROP TABLE IF EXISTS mart.dup_claims_ip;
CREATE TABLE mart.dup_claims_ip AS
SELECT *
FROM mart.fraud_claims_ip
WHERE COALESCE(dup_exact_flag,0)=1 OR COALESCE(dup_near_count,0)>0;

DROP TABLE IF EXISTS mart.dup_claims_op;
CREATE TABLE mart.dup_claims_op AS
SELECT *
FROM mart.fraud_claims_op
WHERE COALESCE(dup_exact_flag,0)=1 OR COALESCE(dup_near_count,0)>0;
WHERE COALESCE(dup_exact_flag,0)=1 OR COALESCE(dup_near_count,0)>0;

SELECT 'dup_ip' AS what, COUNT(*) FROM mart.dup_claims_ip
SELECT 'dup_op', COUNT(*) FROM mart.dup_claims_op;
