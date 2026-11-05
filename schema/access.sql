-- Tenant isolation. Generated per tenant by the provisioning job (t-042 shown). The dashboard
-- API connects as the requesting tenant's user.
--
-- ClickHouse fails open here by default: a user that no row policy names reads every row
-- (access_control_improvements.users_without_row_policies_can_read_rows is on by default).
-- So every table gets a permissive deny-all policy for ALL users. Permissive policies combine
-- with OR, so a tenant user sees (tenant_id = its tenant) OR 0, and a user whose tenant
-- policy is missing, or the default user, sees nothing. A RESTRICTIVE deny-all would be ANDed
-- with every policy and hide all rows from everyone, the pipeline included.

CREATE ROW POLICY IF NOT EXISTS deny_all_events ON events FOR SELECT USING 0 TO ALL;
CREATE ROW POLICY IF NOT EXISTS deny_all_page_views ON page_views_1m FOR SELECT USING 0 TO ALL;

-- The gate, the nightly repair and the deletion jobs (schema/jobs.sql) read across tenants.
-- The admin user that created page_views_1m_mv (its definer) needs the role too, or the
-- deny-all policy may apply to the view's reads. Untested: `make ddl` creates no users.
CREATE ROLE IF NOT EXISTS pipeline;
CREATE ROW POLICY IF NOT EXISTS pipeline_events ON events FOR SELECT USING 1 TO pipeline;
CREATE ROW POLICY IF NOT EXISTS pipeline_page_views ON page_views_1m FOR SELECT USING 1 TO pipeline;
GRANT SELECT, INSERT, ALTER, TRUNCATE ON events TO pipeline;
GRANT SELECT, INSERT, ALTER, TRUNCATE ON page_views_1m TO pipeline;
GRANT SELECT, INSERT, ALTER, TRUNCATE ON page_views_1m_repair TO pipeline;
GRANT SELECT, INSERT ON bot_visitors TO pipeline;
GRANT pipeline TO default;

-- One per tenant.
CREATE ROLE IF NOT EXISTS tenant_t042;
CREATE ROW POLICY IF NOT EXISTS t042_events ON events FOR SELECT USING tenant_id = 't-042' TO tenant_t042;
CREATE ROW POLICY IF NOT EXISTS t042_page_views ON page_views_1m FOR SELECT USING tenant_id = 't-042' TO tenant_t042;
GRANT SELECT ON events TO tenant_t042;
GRANT SELECT ON page_views_1m TO tenant_t042;
