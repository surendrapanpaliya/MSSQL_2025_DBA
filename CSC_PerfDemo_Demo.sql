/*==============================================================================
 CSC Integrated Demo Database Script
 Entities / Filings / Invoices + Skewed Data Generator (for Performance Labs)

 Target: SQL Server 2019/2022/2025 (works in Azure SQL with minor edits)
 Author: Surendra Panpaliya

 What you get:
  - Database: CSC_PerfDemo
  - Tables: Entities, Jurisdictions, FilingTypes, ComplianceFilings, Invoices, InvoicePayments
  - Skewed data distributions (to reproduce bad estimates & parameter sniffing)
  - Helpful indexes + “intentionally missing” areas for labs
  - Optional toggles: row counts, rebuild/reset sections

 Safety:
  - Drops and recreates database CSC_PerfDemo (edit name if needed)

 Suggested labs enabled by this data:
  - Scan vs Seek, sargability, covering indexes, filtered indexes
  - Parameter sniffing (highly skewed Status and Entity distribution)
  - Stale statistics → bad estimates
  - Memory grants & spills (hash joins/sorts on large sets)
  - Query Store plan regression & plan forcing (if enabled)

==============================================================================*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

-------------------------------------------------------------------------------
-- 0) Parameters (EDIT THESE)
-------------------------------------------------------------------------------
DECLARE @DBName sysname = N'CSC_PerfDemo';

-- Scale knobs (keep sane for laptops; increase for powerful servers)
DECLARE @EntityCount        int = 20000;      -- 20k entities
DECLARE @JurisdictionCount  int = 120;        -- 120 jurisdictions
DECLARE @FilingTypeCount    int = 8;          -- 8 filing types
DECLARE @FilingCount        int = 350000;     -- 350k filings
DECLARE @InvoiceCount       int = 250000;     -- 250k invoices
DECLARE @PaymentCount       int = 180000;     -- 180k payments

-- Date range knobs
DECLARE @StartDate date = '2022-01-01';
DECLARE @EndDate   date = '2026-12-31';

-------------------------------------------------------------------------------
-- 1) Drop & Create Database
-------------------------------------------------------------------------------
IF DB_ID(@DBName) IS NOT NULL
BEGIN
    PRINT 'Dropping database ' + @DBName;
    DECLARE @sql nvarchar(max) = N'ALTER DATABASE ' + QUOTENAME(@DBName) + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE;'
                              + N'DROP DATABASE ' + QUOTENAME(@DBName) + N';';
    EXEC sys.sp_executesql @sql;
END
GO

DECLARE @DBName sysname = N'CSC_PerfDemo';
DECLARE @sql nvarchar(max) = N'CREATE DATABASE ' + QUOTENAME(@DBName) + N';';
EXEC sys.sp_executesql @sql;
GO

USE CSC_PerfDemo;
GO

-------------------------------------------------------------------------------
-- 2) Basic DB Options (safe defaults for demo)
-------------------------------------------------------------------------------
ALTER DATABASE CURRENT SET RECOVERY SIMPLE;
ALTER DATABASE CURRENT SET AUTO_UPDATE_STATISTICS ON;
ALTER DATABASE CURRENT SET AUTO_UPDATE_STATISTICS_ASYNC OFF;
ALTER DATABASE CURRENT SET QUERY_STORE = ON;
ALTER DATABASE CURRENT SET QUERY_STORE (OPERATION_MODE = READ_WRITE);
GO

-------------------------------------------------------------------------------
-- 3) Helper: Numbers / Tally (fast set-based generator)
-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.Tally','U') IS NOT NULL DROP TABLE dbo.Tally;
GO

;WITH
E1(N) AS (SELECT 1 FROM (VALUES(1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) v(n)),      -- 10
E2(N) AS (SELECT 1 FROM E1 a CROSS JOIN E1 b),                                     -- 100
E4(N) AS (SELECT 1 FROM E2 a CROSS JOIN E2 b),                                     -- 10,000
E8(N) AS (SELECT 1 FROM E4 a CROSS JOIN E4 b)                                      -- 100,000,000 (logical)
SELECT TOP (1000000) IDENTITY(int,1,1) AS n
INTO dbo.Tally
FROM E8;
GO

-------------------------------------------------------------------------------
-- 4) Master Tables
-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.Entities','U') IS NOT NULL DROP TABLE dbo.Entities;
IF OBJECT_ID('dbo.Jurisdictions','U') IS NOT NULL DROP TABLE dbo.Jurisdictions;
IF OBJECT_ID('dbo.FilingTypes','U') IS NOT NULL DROP TABLE dbo.FilingTypes;
GO

CREATE TABLE dbo.Entities
(
    entity_id      int IDENTITY(1,1) NOT NULL CONSTRAINT PK_Entities PRIMARY KEY,
    client_name    nvarchar(120) NOT NULL,
    entity_name    nvarchar(200) NOT NULL,
    country_code   char(2) NOT NULL,
    risk_tier      tinyint NOT NULL,               -- 1..5
    created_at     datetime2(0) NOT NULL,
    is_active      bit NOT NULL
);
GO

CREATE TABLE dbo.Jurisdictions
(
    jurisdiction_id int IDENTITY(1,1) NOT NULL CONSTRAINT PK_Jurisdictions PRIMARY KEY,
    country_code    char(2) NOT NULL,
    jurisdiction_nm nvarchar(150) NOT NULL
);
GO

CREATE TABLE dbo.FilingTypes
(
    filing_type_id int IDENTITY(1,1) NOT NULL CONSTRAINT PK_FilingTypes PRIMARY KEY,
    filing_type    nvarchar(80) NOT NULL,
    frequency      nvarchar(30) NOT NULL           -- Monthly/Quarterly/Annual/Adhoc
);
GO

-------------------------------------------------------------------------------
-- 5) Transaction Tables
-------------------------------------------------------------------------------
IF OBJECT_ID('dbo.ComplianceFilings','U') IS NOT NULL DROP TABLE dbo.ComplianceFilings;
IF OBJECT_ID('dbo.Invoices','U') IS NOT NULL DROP TABLE dbo.Invoices;
IF OBJECT_ID('dbo.InvoicePayments','U') IS NOT NULL DROP TABLE dbo.InvoicePayments;
GO

CREATE TABLE dbo.ComplianceFilings
(
    filing_id        bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_ComplianceFilings PRIMARY KEY,
    entity_id        int NOT NULL,
    jurisdiction_id  int NOT NULL,
    filing_type_id   int NOT NULL,
    due_date         date NOT NULL,
    filed_date       date NULL,
    filing_status    varchar(20) NOT NULL,         -- Pending/Overdue/Filed/Rejected/Cancelled
    penalty_amount   decimal(12,2) NOT NULL,
    last_updated_at  datetime2(0) NOT NULL,
    CONSTRAINT FK_Filings_Entity FOREIGN KEY (entity_id) REFERENCES dbo.Entities(entity_id),
    CONSTRAINT FK_Filings_Juris  FOREIGN KEY (jurisdiction_id) REFERENCES dbo.Jurisdictions(jurisdiction_id),
    CONSTRAINT FK_Filings_Type   FOREIGN KEY (filing_type_id) REFERENCES dbo.FilingTypes(filing_type_id)
);
GO

CREATE TABLE dbo.Invoices
(
    invoice_id       bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_Invoices PRIMARY KEY,
    entity_id        int NOT NULL,
    invoice_date     date NOT NULL,
    due_date         date NOT NULL,
    currency_code    char(3) NOT NULL,
    amount           decimal(12,2) NOT NULL,
    invoice_status   varchar(20) NOT NULL,         -- Open/Paid/Partial/Disputed/WrittenOff
    created_at       datetime2(0) NOT NULL,
    CONSTRAINT FK_Invoices_Entity FOREIGN KEY (entity_id) REFERENCES dbo.Entities(entity_id)
);
GO

CREATE TABLE dbo.InvoicePayments
(
    payment_id       bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_InvoicePayments PRIMARY KEY,
    invoice_id       bigint NOT NULL,
    payment_date     date NOT NULL,
    amount_paid      decimal(12,2) NOT NULL,
    payment_method   varchar(20) NOT NULL,         -- NEFT/RTGS/Card/UPI/Cheque
    CONSTRAINT FK_Pay_Invoice FOREIGN KEY (invoice_id) REFERENCES dbo.Invoices(invoice_id)
);
GO

-------------------------------------------------------------------------------
-- 6) Seed Dimension Data
-------------------------------------------------------------------------------
PRINT 'Seeding FilingTypes...';
INSERT dbo.FilingTypes (filing_type, frequency)
VALUES (N'GST Return', N'Monthly'),
       (N'TDS Return', N'Quarterly'),
       (N'PF Return', N'Monthly'),
       (N'ESIC Return', N'Monthly'),
       (N'ROC Annual Filing', N'Annual'),
       (N'Trade License Renewal', N'Annual'),
       (N'Professional Tax', N'Quarterly'),
       (N'Adhoc Compliance', N'Adhoc');

PRINT 'Seeding Jurisdictions...';
;WITH c AS
(
    SELECT TOP (@JurisdictionCount)
           n,
           CASE
             WHEN n % 10 IN (0,1,2,3) THEN 'IN'  -- skew toward India
             WHEN n % 10 IN (4,5)     THEN 'US'
             WHEN n % 10 IN (6)       THEN 'GB'
             WHEN n % 10 IN (7)       THEN 'SG'
             WHEN n % 10 IN (8)       THEN 'AE'
             ELSE 'AU'
           END AS country_code
    FROM dbo.Tally
)
INSERT dbo.Jurisdictions(country_code, jurisdiction_nm)
SELECT country_code,
       CONCAT(country_code, N'-Juris-', FORMAT(n,'0000'))
FROM c;

PRINT 'Seeding Entities (skewed clients & countries)...';
/*
 Skew:
  - A few "mega clients" have many entities (to reproduce skewed lookups)
  - Country skew toward IN/US
*/
;WITH e AS
(
    SELECT TOP (@EntityCount)
        n,
        CASE
          WHEN n <= (@EntityCount * 0.05) THEN N'Client-MEGA-01'
          WHEN n <= (@EntityCount * 0.09) THEN N'Client-MEGA-02'
          WHEN n <= (@EntityCount * 0.12) THEN N'Client-MEGA-03'
          ELSE CONCAT(N'Client-', FORMAT((n%500)+1,'0000'))
        END AS client_name,
        CONCAT(N'Entity-', FORMAT(n,'000000')) AS entity_name,
        CASE
          WHEN n % 10 IN (0,1,2,3,4,5) THEN 'IN'  -- 60%
          WHEN n % 10 IN (6,7)         THEN 'US'  -- 20%
          WHEN n % 10 IN (8)           THEN 'GB'  -- 10%
          ELSE 'SG'                                 -- 10%
        END AS country_code,
        CAST((n % 5) + 1 AS tinyint) AS risk_tier,
        DATEADD(day, (n % 1460), CAST('2021-01-01' AS date)) AS created_at,
        CASE WHEN n % 20 = 0 THEN 0 ELSE 1 END AS is_active
    FROM dbo.Tally
)
INSERT dbo.Entities(client_name, entity_name, country_code, risk_tier, created_at, is_active)
SELECT client_name, entity_name, country_code, risk_tier, CAST(created_at AS datetime2(0)), is_active
FROM e;

-------------------------------------------------------------------------------
-- 7) Seed ComplianceFilings (skewed statuses + due dates)
-------------------------------------------------------------------------------
PRINT 'Seeding ComplianceFilings...';
/*
 Status skew:
  - Pending ~60%
  - Filed   ~30%
  - Overdue ~6%
  - Rejected~3%
  - Cancelled~1%

 Entity skew:
  - Mega clients have more filings (join skew)
*/
DECLARE @DaysRange int = DATEDIFF(day, @StartDate, @EndDate);

;WITH f AS
(
    SELECT TOP (@FilingCount)
        n,
        -- skew entity_id toward lower IDs (mega clients are earlier due to insert order)
        CASE
          WHEN n % 10 IN (0,1,2,3,4,5,6) THEN (n % (@EntityCount/5)) + 1     -- hot 70%
          ELSE (n % @EntityCount) + 1                                       -- rest 30%
        END AS entity_id,
        (n % @JurisdictionCount) + 1 AS jurisdiction_id,
        (n % @FilingTypeCount) + 1 AS filing_type_id,
        DATEADD(day, ABS(CHECKSUM(NEWID())) % @DaysRange, @StartDate) AS due_date,
        ABS(CHECKSUM(NEWID())) % 100 AS status_seed
    FROM dbo.Tally
)
INSERT dbo.ComplianceFilings
(
  entity_id, jurisdiction_id, filing_type_id,
  due_date, filed_date, filing_status, penalty_amount, last_updated_at
)
SELECT
  entity_id,
  jurisdiction_id,
  filing_type_id,
  due_date,
  CASE
    WHEN status_seed < 60 THEN NULL                               -- Pending
    WHEN status_seed < 90 THEN DATEADD(day, - (ABS(CHECKSUM(NEWID())) % 30), due_date)  -- Filed
    WHEN status_seed < 96 THEN NULL                               -- Overdue
    WHEN status_seed < 99 THEN DATEADD(day, - (ABS(CHECKSUM(NEWID())) % 10), due_date)  -- Rejected
    ELSE DATEADD(day, - (ABS(CHECKSUM(NEWID())) % 10), due_date)   -- Cancelled
  END AS filed_date,
  CASE
    WHEN status_seed < 60 THEN 'Pending'
    WHEN status_seed < 90 THEN 'Filed'
    WHEN status_seed < 96 THEN 'Overdue'
    WHEN status_seed < 99 THEN 'Rejected'
    ELSE 'Cancelled'
  END AS filing_status,
  CAST( (ABS(CHECKSUM(NEWID())) % 50000) / 10.0 AS decimal(12,2)) AS penalty_amount,
  SYSUTCDATETIME()
FROM f;
GO

-------------------------------------------------------------------------------
-- 8) Seed Invoices & Payments (AR/AP style skew)
-------------------------------------------------------------------------------
PRINT 'Seeding Invoices...';
DECLARE @DaysRange2 int = DATEDIFF(day, @StartDate, @EndDate);

;WITH i AS
(
    SELECT TOP (@InvoiceCount)
        n,
        CASE
          WHEN n % 10 IN (0,1,2,3,4,5) THEN (n % (@EntityCount/6)) + 1
          ELSE (n % @EntityCount) + 1
        END AS entity_id,
        DATEADD(day, ABS(CHECKSUM(NEWID())) % @DaysRange2, @StartDate) AS invoice_date,
        ABS(CHECKSUM(NEWID())) % 100 AS st_seed
    FROM dbo.Tally
)
INSERT dbo.Invoices(entity_id, invoice_date, due_date, currency_code, amount, invoice_status, created_at)
SELECT
  entity_id,
  invoice_date,
  DATEADD(day, 30 + (ABS(CHECKSUM(NEWID())) % 45), invoice_date) AS due_date,
  CASE WHEN entity_id % 10 IN (0,1,2,3,4,5,6) THEN 'INR'
       WHEN entity_id % 10 IN (7,8) THEN 'USD'
       ELSE 'GBP' END AS currency_code,
  CAST( (ABS(CHECKSUM(NEWID())) % 2000000) / 10.0 AS decimal(12,2)) AS amount,
  CASE
    WHEN st_seed < 55 THEN 'Open'
    WHEN st_seed < 80 THEN 'Paid'
    WHEN st_seed < 92 THEN 'Partial'
    WHEN st_seed < 97 THEN 'Disputed'
    ELSE 'WrittenOff'
  END AS invoice_status,
  SYSUTCDATETIME()
FROM i;
GO

PRINT 'Seeding InvoicePayments...';
;WITH p AS
(
    SELECT TOP (@PaymentCount)
        n,
        (ABS(CHECKSUM(NEWID())) % (SELECT MAX(invoice_id) FROM dbo.Invoices)) + 1 AS invoice_id,
        ABS(CHECKSUM(NEWID())) % 100 AS pm_seed
    FROM dbo.Tally
)
INSERT dbo.InvoicePayments(invoice_id, payment_date, amount_paid, payment_method)
SELECT
  invoice_id,
  DATEADD(day, - (ABS(CHECKSUM(NEWID())) % 60), CAST(GETDATE() AS date)) AS payment_date,
  CAST( (ABS(CHECKSUM(NEWID())) % 1500000) / 10.0 AS decimal(12,2)) AS amount_paid,
  CASE
    WHEN pm_seed < 40 THEN 'NEFT'
    WHEN pm_seed < 55 THEN 'UPI'
    WHEN pm_seed < 70 THEN 'RTGS'
    WHEN pm_seed < 85 THEN 'Card'
    ELSE 'Cheque'
  END AS payment_method
FROM p;
GO

-------------------------------------------------------------------------------
-- 9) Baseline Indexes (some are intentionally partial for labs)
-------------------------------------------------------------------------------
PRINT 'Creating baseline indexes...';

-- Entities filters
CREATE INDEX IX_Entities_Country ON dbo.Entities(country_code) INCLUDE (client_name, entity_name, risk_tier, is_active);
CREATE INDEX IX_Entities_Client  ON dbo.Entities(client_name)  INCLUDE (country_code, entity_name, risk_tier, is_active);

-- Filings: helpful, but not perfect (DBAs can tune further)
CREATE INDEX IX_Filings_Entity_DueDate ON dbo.ComplianceFilings(entity_id, due_date) INCLUDE (filing_status, jurisdiction_id, filing_type_id, penalty_amount);
CREATE INDEX IX_Filings_Status_DueDate ON dbo.ComplianceFilings(filing_status, due_date) INCLUDE (entity_id, jurisdiction_id, filing_type_id, penalty_amount);

-- Invoices: typical AR/AP access patterns
CREATE INDEX IX_Invoices_Entity_Status_Due ON dbo.Invoices(entity_id, invoice_status, due_date) INCLUDE (amount, currency_code, invoice_date);
CREATE INDEX IX_Invoices_Status_Due ON dbo.Invoices(invoice_status, due_date) INCLUDE (entity_id, amount, currency_code);

-- Payments: lookups by invoice
CREATE INDEX IX_Payments_InvoiceDate ON dbo.InvoicePayments(invoice_id, payment_date) INCLUDE (amount_paid, payment_method);
GO

-------------------------------------------------------------------------------
-- 10) Optional: Create a skewed filtered index (for labs)
-------------------------------------------------------------------------------
/*
 This filtered index helps ONLY for Overdue filings.
 Great for demonstrating:
  - filtered indexes
  - parameter sniffing changes
  - plan stability vs regressions
*/
CREATE INDEX IX_Filings_Overdue_DueDate
ON dbo.ComplianceFilings(due_date)
INCLUDE (entity_id, jurisdiction_id, filing_type_id, penalty_amount)
WHERE filing_status = 'Overdue';
GO

-------------------------------------------------------------------------------
-- 11) Warm-up: Update Statistics (baseline)
-------------------------------------------------------------------------------
PRINT 'Updating statistics (baseline)...';
EXEC sp_updatestats;
GO

-------------------------------------------------------------------------------
-- 12) Demo Queries 
-------------------------------------------------------------------------------
/*
================================================================================
 DEMO QUERY PACK (Copy/paste into new window with Include Actual Plan ON)
================================================================================

-- A) Parameter Sniffing candidate (skewed status)
CREATE OR ALTER PROCEDURE dbo.usp_FilingsByStatus
  @Status varchar(20)
AS
BEGIN
  SET NOCOUNT ON;
  SELECT TOP (5000) filing_id, entity_id, due_date, filing_status, penalty_amount
  FROM dbo.ComplianceFilings
  WHERE filing_status = @Status
  ORDER BY due_date DESC;
END;
GO

EXEC dbo.usp_FilingsByStatus 'Overdue';  -- rare
EXEC dbo.usp_FilingsByStatus 'Pending';  -- common

-- B) Non-sargable predicate (scan)
SELECT COUNT(*) 
FROM dbo.ComplianceFilings
WHERE CONVERT(date, due_date) = CONVERT(date, GETDATE());

-- Sargable rewrite (seek)
DECLARE @d date = CONVERT(date, GETDATE());
SELECT COUNT(*)
FROM dbo.ComplianceFilings
WHERE due_date >= @d AND due_date < DATEADD(day,1,@d);

-- C) Stats/regression demo: simulate skew change, then compare plan
-- (Do bulk insert to a single status to change distribution; then DON'T update stats)
INSERT dbo.ComplianceFilings(entity_id, jurisdiction_id, filing_type_id, due_date, filed_date, filing_status, penalty_amount, last_updated_at)
SELECT TOP (50000)
  1, 1, 1,
  DATEADD(day, n%365, '2026-01-01'),
  NULL,
  'Overdue',
  0.00,
  SYSUTCDATETIME()
FROM dbo.Tally;

-- Re-run the proc and observe estimates mismatch (stale stats)
EXEC dbo.usp_FilingsByStatus 'Overdue';

-- Fix: update stats and re-run
UPDATE STATISTICS dbo.ComplianceFilings WITH FULLSCAN;
EXEC dbo.usp_FilingsByStatus 'Overdue';

-- D) Memory grant / spill candidate (hash + sort)
SELECT e.country_code, f.filing_status, COUNT(*) AS cnt, SUM(f.penalty_amount) AS penalties
FROM dbo.Entities e
JOIN dbo.ComplianceFilings f
  ON f.entity_id = e.entity_id
GROUP BY e.country_code, f.filing_status
ORDER BY penalties DESC;

================================================================================
*/
GO

PRINT 'CSC_PerfDemo created successfully.';
PRINT 'Next: Run the DEMO QUERY PACK section in a new window with Actual Plans ON.';
