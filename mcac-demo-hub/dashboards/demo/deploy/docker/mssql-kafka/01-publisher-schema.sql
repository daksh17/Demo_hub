/* Publisher: demo DB, hub mirror table, CDC for Debezium. */
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'demo')
    CREATE DATABASE demo;
GO
USE demo;
GO
IF OBJECT_ID(N'dbo.scenario_catalog_mirror_mssql', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.scenario_catalog_mirror_mssql (
        id INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
        sku NVARCHAR(128) NOT NULL UNIQUE,
        title NVARCHAR(512) NOT NULL,
        category NVARCHAR(256) NULL,
        unit_price_cents INT NOT NULL DEFAULT 0,
        stock_units INT NOT NULL DEFAULT 0,
        source_mongo_id NVARCHAR(96) NULL,
        kafka_msg_key NVARCHAR(160) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO
/* Hub workload generator (not CDC — avoids noisy Debezium topics). */
IF OBJECT_ID(N'dbo.hub_workload_mssql', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.hub_workload_mssql (
        id BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
        run_id NVARCHAR(32) NOT NULL,
        seq INT NOT NULL,
        name NVARCHAR(4000) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_hub_workload_mssql_run_seq UNIQUE (run_id, seq)
    );
    CREATE INDEX IX_hub_workload_mssql_run_id ON dbo.hub_workload_mssql (run_id);
END
GO
/* CDC (required for Debezium SQL Server connector). */
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'demo' AND is_cdc_enabled = 0)
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO
USE demo;
GO
DECLARE @oid INT = OBJECT_ID(N'dbo.scenario_catalog_mirror_mssql');
IF @oid IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.databases WHERE name = N'demo' AND is_cdc_enabled = 1)
   AND (SELECT COALESCE(is_tracked_by_cdc, 0) FROM sys.tables WHERE object_id = @oid) = 0
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'scenario_catalog_mirror_mssql',
        @role_name = NULL,
        @supports_net_changes = 1;
END
GO
