-- Grant CREATE on the database so ElectricSQL can attempt to create the
-- publication itself without hitting "permission denied for database remote".
-- The publication already exists from 20251127000000_electric_support.sql,
-- so Electric will get "already exists" which it handles gracefully — this
-- removes the noisy ERROR log from Postgres on every Electric restart.
GRANT CREATE ON DATABASE remote TO electric_sync;
