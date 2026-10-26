-- The resource name of a tenant, as in distributed Chroma: none until it is set, then fixed, and never the same for two
-- tenants.
ALTER TABLE tenants ADD COLUMN resource_name TEXT;
CREATE UNIQUE INDEX tenants_resource_name ON tenants (resource_name) WHERE resource_name IS NOT NULL;
