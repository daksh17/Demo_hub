/* Subscriber: sink table aligned with Debezium unwrap columns (upsert by id). */
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'demo')
    CREATE DATABASE demo;
GO
USE demo;
GO
IF OBJECT_ID(N'dbo.demo_items_from_kafka_mssql', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.demo_items_from_kafka_mssql (
        id INT NOT NULL PRIMARY KEY,
        sku NVARCHAR(128) NULL,
        title NVARCHAR(512) NULL,
        category NVARCHAR(256) NULL,
        unit_price_cents INT NULL,
        stock_units INT NULL,
        source_mongo_id NVARCHAR(96) NULL,
        kafka_msg_key NVARCHAR(160) NULL,
        updated_at DATETIME2 NULL
    );
END
GO
