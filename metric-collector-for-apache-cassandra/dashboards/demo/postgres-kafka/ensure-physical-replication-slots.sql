-- Idempotent: create physical slots for demo HA replicas if missing (existing primary volumes).
-- From dashboards/demo: ./postgres-kafka/apply-physical-replication-slots.sh (runs as `replicator`).
-- Requires REPLICATION attribute (or superuser); Bitnami `replicator` user satisfies this.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'pgdemo_phys_replica_1') THEN
    PERFORM pg_create_physical_replication_slot('pgdemo_phys_replica_1', true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'pgdemo_phys_replica_2') THEN
    PERFORM pg_create_physical_replication_slot('pgdemo_phys_replica_2', true);
  END IF;
END $$;
