
/***************************************************************************************************
CSC Global DBA - Module 7 Instructor Demo Pack (Query Store + Plan Regression + Forcing + AG Metrics)
Database: CSC_DBA_Demo

What this script delivers (single flow):
  1) Enable Query Store + set demo-friendly settings
  2) Establish a "GOOD plan" baseline and capture it in Query Store
  3) Simulate plan regression (index drop + parameter sniffing)
  4) Detect regression in Query Store
  5) Force/unforce stable plans
  6) Always On AG performance snapshot queries (safe if AG not enabled)

How to teach (super simple):
  - Window A: run workload steps (baseline -> regression -> fix)
  - Window B: run Query Store detection queries while A runs

IMPORTANT:
  - Run on SQL Server 2016+ (Query Store). Works on 2019/2022/2025.
  - AG DMVs return empty if AG not configured (that's OK).
****************************************************************************************************/

USE CSC_DBA_Demo;
GO

/*===============================================================================================
0) Pre-check: confirm tables exist
===============================================================================================*/
IF OBJECT_ID(N'dbo.ComplianceFilings', 'U') IS NULL OR OBJECT_ID(N'dbo.Entities', 'U') IS NULL
BEGIN
    RAISERROR('CSC_DBA_Demo tables not found. Run seed script first.', 16, 1);
    RETURN;
END
GO

/*===============================================================================================
1) Enable Query Store (internals)
===============================================================================================*/
ALTER DATABASE CURRENT SET QUERY_STORE = ON;

ALTER DATABASE CURRENT SET QUERY_STORE (OPERATION_MODE = READ_WRITE);
GO

/* Demo-friendly Query Store configuration (optional but recommended in class) */
ALTER DATABASE CURRENT SET QUERY_STORE
(
    CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 7),
    DATA_FLUSH_INTERVAL_SECONDS = 30,
    INTERVAL_LENGTH_MINUTES = 10,
    MAX_STORAGE_SIZE_MB = 512,
    QUERY_CAPTURE_MODE = AUTO
);
GO

/*===============================================================================================
2) Helper: identify "small client" vs "big client" for parameter sniffing demo
===============================================================================================*/
;WITH c AS
(
    SELECT client_name, COUNT_BIG(*) AS entity_count
    FROM dbo.Entities
    GROUP BY client_name
)
SELECT TOP (1) client_name AS big_client, entity_count
FROM c
ORDER BY entity_count DESC;

;WITH c AS
(
    SELECT client_name, COUNT_BIG(*) AS entity_count
    FROM dbo.Entities
    GROUP BY client_name
)
SELECT TOP (1) client_name AS small_client, entity_count
FROM c
ORDER BY entity_count ASC;
GO

/*===============================================================================================
3) GOOD PLAN baseline capture (index + stats + run tagged query)
===============================================================================================*/
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dbo.ComplianceFilings')
      AND name = N'IX_Filings_Status_DueDate_M7'
)
BEGIN
    CREATE INDEX IX_Filings_Status_DueDate_M7
    ON dbo.ComplianceFilings (filing_status, due_date)
    INCLUDE (entity_id, filing_type);
END
GO

UPDATE STATISTICS dbo.ComplianceFilings WITH FULLSCAN;
UPDATE STATISTICS dbo.Entities WITH FULLSCAN;
GO

SET STATISTICS IO ON;
SET STATISTICS TIME ON;
GO

DECLARE @AsOfDate_Good DATE = CAST(GETDATE() AS DATE);

SELECT
    e.country_code,
    e.client_name,
    cf.filing_type,
    COUNT_BIG(*) AS overdue_count
FROM dbo.ComplianceFilings cf
JOIN dbo.Entities e ON e.entity_id = cf.entity_id
WHERE
    cf.filing_status = N'OVERDUE'
    AND cf.due_date < @AsOfDate_Good
    AND 1 = 1 /*QS_M7_GLOBAL_OVERDUE*/
GROUP BY
    e.country_code,
    e.client_name,
    cf.filing_type
ORDER BY
    overdue_count DESC;
GO

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

/*===============================================================================================
4) Find query_id and "good plan" in Query Store
===============================================================================================*/
SELECT TOP (20)
    q.query_id,
    p.plan_id,
    p.is_forced_plan,
    SUM(rs.count_executions) AS execs,
    CAST(AVG(rs.avg_duration)/1000.0 AS DECIMAL(18,2)) AS avg_duration_ms,
    CAST(AVG(rs.avg_cpu_time)/1000.0 AS DECIMAL(18,2)) AS avg_cpu_ms,
    CAST(AVG(rs.avg_logical_io_reads) AS DECIMAL(18,2)) AS avg_logical_reads,
    CAST(MAX(rs.max_dop) AS INT) AS max_dop_seen,
    LEFT(qt.query_sql_text, 220) AS query_text_snippet
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p ON q.query_id = p.query_id
JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
WHERE qt.query_sql_text LIKE N'%QS_M7_GLOBAL_OVERDUE%'
GROUP BY q.query_id, p.plan_id, p.is_forced_plan, qt.query_sql_text
ORDER BY avg_duration_ms ASC;
GO

/*===============================================================================================
5) SIMULATE REGRESSION #1: Index drop (post-deployment incident)
===============================================================================================*/
DROP INDEX IX_Filings_Status_DueDate_M7 ON dbo.ComplianceFilings;
GO

SET STATISTICS IO ON;
SET STATISTICS TIME ON;
GO

DECLARE @AsOfDate_Bad1 DATE = CAST(GETDATE() AS DATE);

SELECT
    e.country_code,
    e.client_name,
    cf.filing_type,
    COUNT_BIG(*) AS overdue_count
FROM dbo.ComplianceFilings cf
JOIN dbo.Entities e ON e.entity_id = cf.entity_id
WHERE
    cf.filing_status = N'OVERDUE'
    AND cf.due_date < @AsOfDate_Bad1
    AND 1 = 1 /*QS_M7_GLOBAL_OVERDUE*/
GROUP BY
    e.country_code,
    e.client_name,
    cf.filing_type
ORDER BY
    overdue_count DESC;
GO

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

/*===============================================================================================
6) Detect regression (same query_id, different plan_id, worse avg_duration/cpu/reads)
===============================================================================================*/
SELECT TOP (20)
    q.query_id,
    p.plan_id,
    p.is_forced_plan,
    SUM(rs.count_executions) AS execs,
    CAST(AVG(rs.avg_duration)/1000.0 AS DECIMAL(18,2)) AS avg_duration_ms,
    CAST(AVG(rs.avg_cpu_time)/1000.0 AS DECIMAL(18,2)) AS avg_cpu_ms,
    CAST(AVG(rs.avg_logical_io_reads) AS DECIMAL(18,2)) AS avg_logical_reads,
    LEFT(qt.query_sql_text, 220) AS query_text_snippet
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p ON q.query_id = p.query_id
JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
WHERE qt.query_sql_text LIKE N'%QS_M7_GLOBAL_OVERDUE%'
GROUP BY q.query_id, p.plan_id, p.is_forced_plan, qt.query_sql_text
ORDER BY avg_duration_ms DESC;
GO

/*===============================================================================================
7) Force stable plan (fill these 2 values from Query Store output above)
===============================================================================================*/
DECLARE @QueryId BIGINT = NULL;      -- set from output
DECLARE @GoodPlanId BIGINT = NULL;   -- set from output (fastest plan)

IF @QueryId IS NULL OR @GoodPlanId IS NULL
BEGIN
    PRINT 'Set @QueryId and @GoodPlanId first (see Query Store output above).';
END
ELSE
BEGIN
    EXEC sys.sp_query_store_force_plan @query_id = @QueryId, @plan_id = @GoodPlanId;
END
GO

/* Verify force status */
DECLARE @QueryIdV BIGINT = NULL;  -- set it
IF @QueryIdV IS NOT NULL
BEGIN
    SELECT
        p.query_id,
        p.plan_id,
        p.is_forced_plan,
        p.force_failure_count,
        p.last_force_failure_reason_desc
    FROM sys.query_store_plan p
    WHERE p.query_id = @QueryIdV;
END
GO

/*===============================================================================================
8) Verify improvement (run tagged query again)
===============================================================================================*/
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
GO

DECLARE @AsOfDate_Verify DATE = CAST(GETDATE() AS DATE);

SELECT
    e.country_code,
    e.client_name,
    cf.filing_type,
    COUNT_BIG(*) AS overdue_count
FROM dbo.ComplianceFilings cf
JOIN dbo.Entities e ON e.entity_id = cf.entity_id
WHERE
    cf.filing_status = N'OVERDUE'
    AND cf.due_date < @AsOfDate_Verify
    AND 1 = 1 /*QS_M7_GLOBAL_OVERDUE*/
GROUP BY
    e.country_code,
    e.client_name,
    cf.filing_type
ORDER BY
    overdue_count DESC;
GO

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

/*===============================================================================================
9) SIMULATE REGRESSION #2: Parameter sniffing (client drilldown)
===============================================================================================*/
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dbo.Entities')
      AND name = N'IX_Entities_ClientName'
)
BEGIN
    CREATE INDEX IX_Entities_ClientName
    ON dbo.Entities (client_name)
    INCLUDE (entity_id, country_code);
END
GO

CREATE OR ALTER PROCEDURE dbo.usp_ClientOverdue_M7
    @ClientName NVARCHAR(200),
    @AsOfDate   DATE
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        e.country_code,
        e.client_name,
        cf.filing_type,
        COUNT_BIG(*) AS overdue_count
    FROM dbo.ComplianceFilings cf
    JOIN dbo.Entities e ON e.entity_id = cf.entity_id
    WHERE
        e.client_name = @ClientName
        AND cf.filing_status = N'OVERDUE'
        AND cf.due_date < @AsOfDate
        AND 1 = 1 /*QS_M7_CLIENT_OVERDUE*/
    GROUP BY
        e.country_code,
        e.client_name,
        cf.filing_type
    ORDER BY
        overdue_count DESC;
END
GO

-- Replace these with the big/small client outputs from step 2
DECLARE @SmallClient NVARCHAR(200) = N'Client-1';
DECLARE @BigClient   NVARCHAR(200) = N'Client-2';

EXEC dbo.usp_ClientOverdue_M7 @ClientName = @SmallClient, @AsOfDate = CAST(GETDATE() AS DATE);
EXEC dbo.usp_ClientOverdue_M7 @ClientName = @BigClient,   @AsOfDate = CAST(GETDATE() AS DATE);
GO

/* Fix option: RECOMPILE (reliably avoids sniffing at cost of compilation) */
CREATE OR ALTER PROCEDURE dbo.usp_ClientOverdue_M7_Recompile
    @ClientName NVARCHAR(200),
    @AsOfDate   DATE
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        e.country_code,
        e.client_name,
        cf.filing_type,
        COUNT_BIG(*) AS overdue_count
    FROM dbo.ComplianceFilings cf
    JOIN dbo.Entities e ON e.entity_id = cf.entity_id
    WHERE
        e.client_name = @ClientName
        AND cf.filing_status = N'OVERDUE'
        AND cf.due_date < @AsOfDate
        AND 1 = 1 /*QS_M7_CLIENT_OVERDUE_RECOMPILE*/
    GROUP BY
        e.country_code,
        e.client_name,
        cf.filing_type
    ORDER BY
        overdue_count DESC
    OPTION (RECOMPILE);
END
GO

EXEC dbo.usp_ClientOverdue_M7_Recompile @ClientName = @BigClient, @AsOfDate = CAST(GETDATE() AS DATE);
GO

/*===============================================================================================
10) Root fix + unforce plan (best practice)
===============================================================================================*/
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dbo.ComplianceFilings')
      AND name = N'IX_Filings_Status_DueDate_M7'
)
BEGIN
    CREATE INDEX IX_Filings_Status_DueDate_M7
    ON dbo.ComplianceFilings (filing_status, due_date)
    INCLUDE (entity_id, filing_type);
END
GO

UPDATE STATISTICS dbo.ComplianceFilings WITH FULLSCAN;
GO

DECLARE @QueryIdUnforce BIGINT = NULL; -- set
DECLARE @PlanIdUnforce  BIGINT = NULL; -- set
IF @QueryIdUnforce IS NOT NULL AND @PlanIdUnforce IS NOT NULL
BEGIN
    EXEC sys.sp_query_store_unforce_plan @query_id = @QueryIdUnforce, @plan_id = @PlanIdUnforce;
END
GO

/*===============================================================================================
11) Always On AG performance snapshot queries (safe if AG not enabled)
===============================================================================================*/

-- 11A) Log send & redo queue metrics (per database/replica)
SELECT
    DB_NAME(drs.database_id) AS database_name,
    drs.is_primary_replica,
    drs.synchronization_state_desc,
    drs.synchronization_health_desc,
    drs.log_send_queue_size,      -- KB waiting to send
    drs.log_send_rate,            -- KB/sec
    drs.redo_queue_size,          -- KB waiting to redo
    drs.redo_rate,                -- KB/sec
    drs.last_commit_time,
    drs.last_hardened_time,
    drs.last_redone_time
FROM sys.dm_hadr_database_replica_states drs
ORDER BY drs.log_send_queue_size DESC, drs.redo_queue_size DESC;
GO

-- 11B) HADR waits (commit/log send signals)
SELECT TOP (20)
    ws.wait_type,
    ws.waiting_tasks_count,
    ws.wait_time_ms,
    ws.signal_wait_time_ms
FROM sys.dm_os_wait_stats ws
WHERE ws.wait_type LIKE 'HADR%'
ORDER BY ws.wait_time_ms DESC;
GO

PRINT 'Module 7 demo pack complete: regression -> detect -> force -> verify -> sniffing -> AG snapshot.';
GO
