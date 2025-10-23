-- -- 1.4) Map keys and foreign relationships (provider IDs, beneficiary IDs, diagnosis/procedure codes).

-- -- 1) Add Foreign Keys (provider & beneid)

-- -- Inpatient → Provider
ALTER TABLE curated.claims_inpatient
  ADD CONSTRAINT claims_inpatient_provider_fkey
  FOREIGN KEY (provider) REFERENCES curated.provider_labels(provider);

-- -- Inpatient → Beneficiary
ALTER TABLE curated.claims_inpatient
  ADD CONSTRAINT claims_inpatient_beneid_fkey
  FOREIGN KEY (beneid) REFERENCES curated.beneficiary(beneid);

-- -- Outpatient → Provider
ALTER TABLE curated.claims_outpatient
  ADD CONSTRAINT claims_outpatient_provider_fkey
  FOREIGN KEY (provider) REFERENCES curated.provider_labels(provider);

-- -- Outpatient → Beneficiary
ALTER TABLE curated.claims_outpatient
  ADD CONSTRAINT claims_outpatient_beneid_fkey
  FOREIGN KEY (beneid) REFERENCES curated.beneficiary(beneid);

-- -- 2) Index the referencing FK columns (joins + FK checks)
-- -- Inpatient
CREATE INDEX IF NOT EXISTS idx_ip_provider ON curated.claims_inpatient(provider);
CREATE INDEX IF NOT EXISTS idx_ip_beneid   ON curated.claims_inpatient(beneid);

-- -- Outpatient
CREATE INDEX IF NOT EXISTS idx_op_provider ON curated.claims_outpatient(provider);
CREATE INDEX IF NOT EXISTS idx_op_beneid   ON curated.claims_outpatient(beneid);

-- 3) Post-constraint verification
-- Sample: join via provider (expect index use on claims_* provider)
EXPLAIN ANALYZE
SELECT count(*)
FROM curated.claims_inpatient ci
JOIN curated.provider_labels p USING (provider)
WHERE p.provider = (SELECT provider FROM curated.provider_labels LIMIT 1);

EXPLAIN ANALYZE
SELECT count(*)
FROM curated.claims_outpatient co
JOIN curated.beneficiary b USING (beneid)
WHERE b.beneid = (SELECT beneid FROM curated.beneficiary LIMIT 1);

