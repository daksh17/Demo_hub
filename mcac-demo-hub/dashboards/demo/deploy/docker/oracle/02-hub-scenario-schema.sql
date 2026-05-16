-- Hub scenario mirror + workload tables (user DEMO).
WHENEVER SQLERROR EXIT SQL.SQLCODE

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE hub_workload_oracle';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE scenario_catalog_mirror_oracle';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE scenario_catalog_mirror_oracle (
  id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sku VARCHAR2(128) NOT NULL,
  title VARCHAR2(512) NOT NULL,
  category VARCHAR2(256),
  unit_price_cents NUMBER(12) DEFAULT 0 NOT NULL,
  stock_units NUMBER(12) DEFAULT 0 NOT NULL,
  source_mongo_id VARCHAR2(96),
  kafka_msg_key VARCHAR2(160),
  updated_at TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT scenario_catalog_mirror_ora_uk UNIQUE (sku)
);

CREATE TABLE hub_workload_oracle (
  id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id VARCHAR2(32) NOT NULL,
  seq NUMBER NOT NULL,
  name VARCHAR2(4000) NOT NULL,
  created_at TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT hub_workload_oracle_run_seq_uk UNIQUE (run_id, seq)
);

CREATE INDEX hub_workload_oracle_run_id_ix ON hub_workload_oracle (run_id);

COMMIT;
