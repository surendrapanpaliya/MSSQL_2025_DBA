/* =========================================================
   CSC GLOBAL DBA DEMO – Module 1
   SQL Server Architecture & Execution Lifecycle + DMVs
   Scenario: Global Compliance Overdue Morning Report slows down
   ========================================================= */

------------------------------------------------------------
-- 0) Create Demo DB
------------------------------------------------------------
IF DB_ID('CSC_DBA_Demo') IS NOT NULL
BEGIN
    ALTER DATABASE CSC_DBA_Demo SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE CSC_DBA_Demo;
END
GO

CREATE DATABASE CSC_DBA_Demo;
GO
ALTER DATABASE CSC_DBA_Demo SET RECOVERY SIMPLE;
GO

USE CSC_DBA_Demo;
GO

------------------------------------------------------------
-- 1) Create CSC-like Tables
------------------------------------------------------------

-- Master list of legal entities managed globally
CREATE TABLE dbo.Entities
(
    entity_id      INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Entities PRIMARY KEY,
    entity_name    NVARCHAR(200) NOT NULL,
    client_name    NVARCHAR(200) NOT NULL,
    country_code   CHAR(2) NOT NULL,
    created_at     DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
GO

-- Jurisdictions / countries / states where filings happen
CREATE TABLE dbo.Jurisdictions
(
    jurisdiction_id INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Jurisdictions PRIMARY KEY,
    country_code    CHAR(2) NOT NULL,
    jurisdiction    NVARCHAR(100) NOT NULL
);
GO

-- Compliance events (annual returns, tax filings, etc.)
CREATE TABLE dbo.ComplianceFilings
(
    filing_id       BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_ComplianceFilings PRIMARY KEY,
    entity_id       INT NOT NULL,
    jurisdiction_id INT NOT NULL,
    filing_type     NVARCHAR(50) NOT NULL,      -- Annual Return, Tax, KYC, etc.
    due_date        DATE NOT NULL,
    filing_status   NVARCHAR(20) NOT NULL,      -- OVERDUE / DUE / FILED
    last_updated    DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
    notes           NVARCHAR(200) NULL,

    CONSTRAINT FK_ComplianceFilings_Entities
        FOREIGN KEY(entity_id) REFERENCES dbo.Entities(entity_id),

    CONSTRAINT FK_ComplianceFilings_Jurisdictions
        FOREIGN KEY(jurisdiction_id) REFERENCES dbo.Jurisdictions(jurisdiction_id)
);
GO

------------------------------------------------------------
-- 2) Seed Data (Countries + Jurisdictions)
------------------------------------------------------------
INSERT INTO dbo.Jurisdictions(country_code, jurisdiction)
VALUES
('US','Delaware'),('US','California'),('US','New York'),
('UK','England'),('UK','Scotland'),
('IN','Maharashtra'),('IN','Karnataka'),('IN','Delhi'),
('SG','Singapore'),('AE','Dubai');
GO

------------------------------------------------------------
-- 3) Generate Entities (10,000)
------------------------------------------------------------
;WITH n AS
(
    SELECT TOP (10000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.Entities(entity_name, client_name, country_code)
SELECT
    CONCAT(N'Entity-', rn),
    CONCAT(N'Client-', (rn % 500) + 1),
    CASE (rn % 5)
        WHEN 0 THEN 'US'
        WHEN 1 THEN 'UK'
        WHEN 2 THEN 'IN'
        WHEN 3 THEN 'SG'
        ELSE 'AE'
    END
FROM n;
GO

------------------------------------------------------------
-- 4) Generate Filings (~300,000)
--    Each entity gets ~30 filings across jurisdictions over time
------------------------------------------------------------
DECLARE @Today DATE = CAST(GETDATE() AS DATE);

;WITH e AS
(
    SELECT entity_id, country_code
    FROM dbo.Entities
),
j AS
(
    SELECT jurisdiction_id, country_code
    FROM dbo.Jurisdictions
),
x AS
(
    SELECT TOP (300000)
        e.entity_id,
        j.jurisdiction_id,
        ABS(CHECKSUM(NEWID())) % 5 AS filing_type_id,
        (ABS(CHECKSUM(NEWID())) % 900) AS day_offset,
        ABS(CHECKSUM(NEWID())) % 100 AS status_seed
    FROM e
    JOIN j ON j.country_code = e.country_code
    CROSS JOIN (SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 3) t
)
INSERT INTO dbo.ComplianceFilings
(
    entity_id, jurisdiction_id, filing_type, due_date, filing_status, notes
)
SELECT
    entity_id,
    jurisdiction_id,
    CASE filing_type_id
        WHEN 0 THEN N'Annual Return'
        WHEN 1 THEN N'Tax Filing'
        WHEN 2 THEN N'KYC Update'
        WHEN 3 THEN N'License Renewal'
        ELSE N'Board Resolution'
    END,
    DATEADD(DAY, -day_offset, @Today),
    CASE
        WHEN status_seed < 15 THEN N'OVERDUE'
        WHEN status_seed < 60 THEN N'DUE'
        ELSE N'FILED'
    END,
    N'System generated'
FROM x;
GO

------------------------------------------------------------
-- 5) Baseline Indexes (Reasonable)
------------------------------------------------------------
-- Helps entity filters and joins
CREATE INDEX IX_Entities_Country ON dbo.Entities(country_code) INCLUDE (client_name, entity_name);
GO

-- Helpful for due status search & group-by
CREATE INDEX IX_Filings_Status_DueDate ON dbo.ComplianceFilings(filing_status, due_date)
INCLUDE (entity_id, jurisdiction_id, filing_type);
GO

------------------------------------------------------------
-- 6) Demo Query: "Global Overdue Compliance Report"
--    This is the morning report that stakeholders complain about.
------------------------------------------------------------

/*
    Tip: Turn on Actual Execution Plan in SSMS.
*/

-- A parameterized report date (today)
DECLARE @AsOfDate DATE = CAST(GETDATE() AS DATE);

-- Report: Overdue count by country and filing type (top offenders)
SELECT
    e.country_code,
    cf.filing_type,
    COUNT_BIG(*) AS overdue_count
FROM dbo.ComplianceFilings cf
JOIN dbo.Entities e ON e.entity_id = cf.entity_id
WHERE
    cf.filing_status = N'OVERDUE'
    AND cf.due_date < @AsOfDate
GROUP BY
    e.country_code,
    cf.filing_type
ORDER BY
    overdue_count DESC;
GO

------------------------------------------------------------
-- 7) Make It "Interesting": Create a Performance Problem
--    We simulate a bad plan by dropping a useful index and
--    turning the report into a memory-heavy aggregation.
------------------------------------------------------------

-- Drop the helpful index to push scans & heavier work
DROP INDEX IX_Filings_Status_DueDate ON dbo.ComplianceFilings;
GO

-- Memory-heavy version: add ORDER BY on aggregated results + widen grouping
DECLARE @AsOfDate2 DATE = CAST(GETDATE() AS DATE);

SELECT
    e.country_code,
    e.client_name,
    cf.filing_type,
    COUNT_BIG(*) AS overdue_count
FROM dbo.ComplianceFilings cf
JOIN dbo.Entities e ON e.entity_id = cf.entity_id
WHERE
    cf.filing_status = N'OVERDUE'
    AND cf.due_date < @AsOfDate2
GROUP BY
    e.country_code,
    e.client_name,
    cf.filing_type
ORDER BY
    overdue_count DESC;
GO

------------------------------------------------------------
-- 8) Observe Query Lifecycle Using DMVs (Run WHILE query is running)
--    Open a second SSMS window and execute these while the report runs.
------------------------------------------------------------

-- 8.1 Identify running requests and waits (Lifecycle: running/waiting)
SELECT
    r.session_id,
    r.status,
    r.command,
    r.cpu_time,
    r.total_elapsed_time,
    r.reads,
    r.writes,
    r.logical_reads,
    r.wait_type,
    r.wait_time,
    r.last_wait_type,
    r.blocking_session_id
FROM sys.dm_exec_requests r
WHERE r.session_id > 50
ORDER BY r.total_elapsed_time DESC;
GO

-- 8.2 Get SQL text for the currently running request (pick a session_id from above)
-- Replace  <SESSION_ID_HERE>
DECLARE @sid INT = NULL; -- set e.g. 55
SELECT
    r.session_id,
    t.text
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE r.session_id = @sid;
GO

-- 8.3 Scheduler pressure (workers/threads)
SELECT
    scheduler_id,
    cpu_id,
    status,
    current_tasks_count,
    runnable_tasks_count,
    active_workers_count,
    load_factor
FROM sys.dm_os_schedulers
WHERE status = 'VISIBLE ONLINE'
ORDER BY runnable_tasks_count DESC;
GO

-- 8.4 Memory grants (who is waiting for memory / who consumed big grant)
SELECT
    mg.session_id,
    mg.request_time,
    mg.grant_time,
    mg.requested_memory_kb,
    mg.granted_memory_kb,
    mg.required_memory_kb,
    mg.used_memory_kb,
    mg.max_used_memory_kb,
    mg.wait_time_ms,
    mg.is_next_candidate
FROM sys.dm_exec_query_memory_grants mg
ORDER BY mg.requested_memory_kb DESC;
GO

------------------------------------------------------------
-- 9) "Fix" Step: Add Better Index + Update Stats
--    This should change optimizer decisions (Relational Engine)
------------------------------------------------------------

CREATE INDEX IX_Filings_Status_DueDate
ON dbo.ComplianceFilings(filing_status, due_date)
INCLUDE (entity_id, jurisdiction_id, filing_type);
GO

UPDATE STATISTICS dbo.ComplianceFilings WITH FULLSCAN;
GO

-- Run the heavy report again after fix
DECLARE @AsOfDate3 DATE = CAST(GETDATE() AS DATE);

SELECT
    e.country_code,
    e.client_name,
    cf.filing_type,
    COUNT_BIG(*) AS overdue_count
FROM dbo.ComplianceFilings cf
JOIN dbo.Entities e ON e.entity_id = cf.entity_id
WHERE
    cf.filing_status = N'OVERDUE'
    AND cf.due_date < @AsOfDate3
GROUP BY
    e.country_code,
    e.client_name,
    cf.filing_type
ORDER BY
    overdue_count DESC;
GO

------------------------------------------------------------
-- 10) Clean-up (optional)
------------------------------------------------------------
-- USE master;
-- DROP DATABASE CSC_DBA_Demo;