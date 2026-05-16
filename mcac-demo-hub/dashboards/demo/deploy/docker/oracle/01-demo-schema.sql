-- Demo hub Oracle schema (FREEPDB1 / user DEMO): tables, PL/SQL, triggers, views, MVs.
WHENEVER SQLERROR EXIT SQL.SQLCODE

BEGIN
  EXECUTE IMMEDIATE 'DROP MATERIALIZED VIEW hub_demo_mv_summary';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -12003 THEN RAISE; END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP VIEW hub_demo_items_v';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP TRIGGER hub_demo_items_bu';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -4080 THEN RAISE; END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE hub_demo_items CASCADE CONSTRAINTS PURGE';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE hub_demo_seq';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP TYPE hub_item_tab';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -4043 THEN RAISE; END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP TYPE hub_item_rec FORCE';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -4043 THEN RAISE; END IF;
END;
/

CREATE TABLE hub_demo_items (
  id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sku VARCHAR2(128) NOT NULL,
  title VARCHAR2(512) NOT NULL,
  category VARCHAR2(256),
  unit_price_cents NUMBER(12) DEFAULT 0 NOT NULL,
  stock_units NUMBER(12) DEFAULT 0 NOT NULL,
  updated_at TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT hub_demo_items_sku_uk UNIQUE (sku)
);

CREATE SEQUENCE hub_demo_seq START WITH 1000 INCREMENT BY 1 NOCACHE;

CREATE OR REPLACE TYPE hub_item_rec AS OBJECT (
  sku VARCHAR2(128),
  title VARCHAR2(512),
  price_cents NUMBER
);
/

CREATE OR REPLACE TYPE hub_item_tab AS TABLE OF hub_item_rec;
/

CREATE OR REPLACE PACKAGE hub_demo_pkg AS
  FUNCTION next_sku_prefix RETURN VARCHAR2;
  PROCEDURE bump_updated(p_id IN NUMBER);
  FUNCTION catalog_count RETURN NUMBER;
END hub_demo_pkg;
/

CREATE OR REPLACE PACKAGE BODY hub_demo_pkg AS
  FUNCTION next_sku_prefix RETURN VARCHAR2 IS
  BEGIN
    RETURN 'SKU-' || hub_demo_seq.NEXTVAL;
  END next_sku_prefix;

  PROCEDURE bump_updated(p_id IN NUMBER) IS
  BEGIN
    UPDATE hub_demo_items SET updated_at = SYSTIMESTAMP WHERE id = p_id;
  END bump_updated;

  FUNCTION catalog_count RETURN NUMBER IS
    c NUMBER;
  BEGIN
    SELECT COUNT(*) INTO c FROM hub_demo_items;
    RETURN c;
  END catalog_count;
END hub_demo_pkg;
/

CREATE OR REPLACE FUNCTION hub_demo_price_dollars(p_cents IN NUMBER) RETURN NUMBER DETERMINISTIC IS
BEGIN
  RETURN ROUND(p_cents / 100, 2);
END hub_demo_price_dollars;
/

CREATE OR REPLACE PROCEDURE hub_demo_upsert_item(
  p_sku IN VARCHAR2,
  p_title IN VARCHAR2,
  p_cents IN NUMBER,
  p_category IN VARCHAR2 DEFAULT NULL
) IS
BEGIN
  MERGE INTO hub_demo_items t
  USING (
    SELECT p_sku sku, p_title title, p_cents cents, p_category category FROM dual
  ) s
  ON (t.sku = s.sku)
  WHEN MATCHED THEN
    UPDATE SET
      title = s.title,
      unit_price_cents = s.cents,
      category = NVL(s.category, t.category),
      updated_at = SYSTIMESTAMP
  WHEN NOT MATCHED THEN
    INSERT (sku, title, unit_price_cents, category)
    VALUES (s.sku, s.title, s.cents, s.category);
END hub_demo_upsert_item;
/

CREATE OR REPLACE TRIGGER hub_demo_items_bu
BEFORE UPDATE ON hub_demo_items
FOR EACH ROW
BEGIN
  :NEW.updated_at := SYSTIMESTAMP;
END;
/

CREATE OR REPLACE VIEW hub_demo_items_v AS
SELECT
  id,
  sku,
  title,
  category,
  hub_demo_price_dollars(unit_price_cents) AS price_dollars,
  stock_units,
  updated_at
FROM hub_demo_items;

CREATE MATERIALIZED VIEW hub_demo_mv_summary
BUILD IMMEDIATE
REFRESH ON DEMAND
AS
SELECT
  COUNT(*) AS item_count,
  NVL(SUM(unit_price_cents), 0) AS total_cents,
  MAX(updated_at) AS last_updated
FROM hub_demo_items;

BEGIN
  hub_demo_upsert_item('ORA-DEMO-001', 'Oracle demo widget', 1999, 'gadgets');
  hub_demo_upsert_item('ORA-DEMO-002', 'Oracle demo gadget', 2999, 'gadgets');
END;
/

COMMIT;

SELECT 'oracle demo schema ready' AS status FROM dual;
SELECT object_type, COUNT(*) AS cnt
FROM user_objects
WHERE object_name LIKE 'HUB_DEMO%' OR object_name LIKE 'HUB_ITEM%'
GROUP BY object_type
ORDER BY object_type;
