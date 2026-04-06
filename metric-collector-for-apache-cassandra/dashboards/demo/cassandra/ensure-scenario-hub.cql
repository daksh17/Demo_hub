-- Hub Multi-DB scenario timeline + secondary index (low-cardinality demo filters).
-- Matches realtime-orders-search-hub/demo-ui/scenario.py: ensure_cassandra_scenario_schema
-- and app.py keyspace replication (SimpleStrategy RF=1).

CREATE KEYSPACE IF NOT EXISTS demo_hub
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS demo_hub.scenario_timeline (
  order_ref text,
  event_ts timestamp,
  event_type text,
  detail text,
  PRIMARY KEY (order_ref, event_ts)
) WITH CLUSTERING ORDER BY (event_ts DESC);

CREATE INDEX IF NOT EXISTS scenario_timeline_event_type
  ON demo_hub.scenario_timeline (event_type);
