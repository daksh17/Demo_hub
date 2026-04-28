/*
 * Best-effort transactional replication bootstrap (publisher = distributor).
 * Docker labs often need @@SERVERNAME / DNS aligned with compose hostnames.
 * If this fails, Debezium + JDBC sink still work; only native SQL Server replication is skipped.
 *
 * Run from sqlcmd connected to PUBLISHER as sa, then SUBSCRIBER steps separately if needed.
 * See: https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-replication-configure-transactional
 */
SET NOCOUNT ON;
USE master;
GO

DECLARE @dist sysname = CAST(SERVERPROPERTY('MachineName') AS sysname);
DECLARE @pwd NVARCHAR(128) = N'ReplDist_DemoHub_2025!';

/* Distributor + distribution database (idempotent-ish). */
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'distribution')
BEGIN
    EXEC sp_adddistributor @distributor = @dist, @password = @pwd;
    EXEC sp_adddistributiondb
        @database = N'distribution',
        @data_folder = N'/var/opt/mssql/data',
        @log_folder = N'/var/opt/mssql/data',
        @log_file_size = 2,
        @min_distretention = 0,
        @max_distretention = 72,
        @history_retention = 48;
END
GO

USE demo;
GO
IF NOT EXISTS (
    SELECT 1 FROM sys.databases WHERE name = N'demo' AND is_published = 1
)
BEGIN
    EXEC sp_replicationdboption @dbname = N'demo', @optname = N'publish', @value = N'true';
END
GO

USE demo;
GO
IF NOT EXISTS (SELECT 1 FROM syspublications WHERE name = N'demo_hub_pub')
BEGIN
    EXEC sp_addpublication
        @publication = N'demo_hub_pub',
        @description = N'demo-hub catalog mirror',
        @sync_method = N'native',
        @status = N'active',
        @allow_push = N'true',
        @allow_pull = N'true',
        @independent_agent = N'true';
END
GO

USE demo;
GO
IF NOT EXISTS (SELECT 1 FROM sysarticles a INNER JOIN syspublications p ON a.pubid = p.pubid WHERE p.name = N'demo_hub_pub' AND a.article = N'scenario_catalog_mirror_mssql')
BEGIN
    EXEC sp_addarticle
        @publication = N'demo_hub_pub',
        @article = N'scenario_catalog_mirror_mssql',
        @source_table = N'scenario_catalog_mirror_mssql',
        @source_owner = N'dbo',
        @destination_table = N'scenario_catalog_mirror_mssql',
        @type = N'logbased';
END
GO
