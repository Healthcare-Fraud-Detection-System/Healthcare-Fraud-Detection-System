ALTER TABLE curated.claims_inpatient
  ADD CONSTRAINT claims_inpatient_provider_fkey
  FOREIGN KEY (provider) REFERENCES curated.provider_labels(provider);

ALTER TABLE curated.claims_inpatient
  ADD CONSTRAINT claims_inpatient_beneid_fkey
  FOREIGN KEY (beneid) REFERENCES curated.beneficiary(beneid);

ALTER TABLE curated.claims_outpatient
  ADD CONSTRAINT claims_outpatient_provider_fkey
  FOREIGN KEY (provider) REFERENCES curated.provider_labels(provider);

ALTER TABLE curated.claims_outpatient
  ADD CONSTRAINT claims_outpatient_beneid_fkey
  FOREIGN KEY (beneid) REFERENCES curated.beneficiary(beneid);

CREATE INDEX IF NOT EXISTS idx_ip_provider ON curated.claims_inpatient(provider);
CREATE INDEX IF NOT EXISTS idx_ip_beneid   ON curated.claims_inpatient(beneid);

CREATE INDEX IF NOT EXISTS idx_op_provider ON curated.claims_outpatient(provider);
CREATE INDEX IF NOT EXISTS idx_op_beneid   ON curated.claims_outpatient(beneid);

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

