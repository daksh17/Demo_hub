-- Privileges for iamseth/oracledb_exporter (demo user). Run as SYS (see init-demo-schema.sh).
WHENEVER SQLERROR EXIT SQL.SQLCODE

BEGIN
  EXECUTE IMMEDIATE 'GRANT SELECT_CATALOG_ROLE TO demo';
EXCEPTION
  WHEN OTHERS THEN
    -- Role missing on some editions (-1924), or already granted (-1917).
    IF SQLCODE NOT IN (-1917, -1924) THEN RAISE; END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO demo';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE NOT IN (-1917, -1924) THEN RAISE; END IF;
END;
/

COMMIT;
