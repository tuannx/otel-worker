-- P2: make service/tenant first-class columns on spans (was only inside `inner` JSON).
ALTER TABLE spans ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE spans ADD COLUMN service_name TEXT NOT NULL DEFAULT 'unknown';

CREATE INDEX spans_service_time ON spans (service_name, end_time);
CREATE INDEX spans_tenant_time ON spans (tenant_id, end_time);
