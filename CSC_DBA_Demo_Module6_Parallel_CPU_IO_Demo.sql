
/**********************************************************************************************
CSC Global DBA - Module 6 Demo Pack (Parallelism, CPU & I/O Tuning)
Target DB: CSC_DBA_Demo

Single instructor flow:
  1) BAD parallel plan -> observe CPU + DOP + IO
  2) FIX -> measure again
  3) VERIFY in Query Store

Works for SQL Server 2016+ (Query Store).
Azure SQL Database notes included (MAXDOP tuning differs).

Instructor setup:
  - Use 2 SSMS windows:
      Window A: run the “bad query” a few times (or concurrently)
      Window B: run Query Store + IO latency queries to observe
************************************************************************************************/

USE CSC_DBA_Demo;
GO

/*==============================================================================================
0) Ensure Query Store is ON (needed for CPU + parallel analysis)
==============================================================================================*/
ALTER DATABASE CURRENT SET QUERY_STORE = ON;
ALTER DATABASE CURRENT SET QUERY_STORE (OPERATION_MODE = READ_WRITE);
GO

/*==============================================================================================
1) Baseline business query (often uses hash/sort and may go parallel)
CSC story:
  - Morning Global Overdue Compliance report grouped by country/client/type.
Teaching:
  - Turn on Actual Execution Plan (Ctrl+M).
  - Read Messages tab (CPU time, elapsed time, reads).
==============================================================================================*/
SET NOCOUNT ON;
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
GO

DECLARE @AsOfDate DATE = CAST(GETDATE() AS DATE);

SELECT
    e.country_code,
    e.client_name,
    cf.filing_type,
    COUNT_BIG(*) AS overdue_count
FROM dbo.ComplianceFilings cf
JOIN dbo.Entities e
    ON e.entity_id = cf.entity_id
WHERE
    cf.filing_status = N'OVERDUE'
    AND cf.due_date < @AsOfDate
GROUP BY
    e.country_code,
    e.client_name,
    cf.filing_type
ORDER BY
    overdue_count DESC
OPTION (MAXDOP 0);   -- allow parallelism (demo)
GO

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


/*==============================================================================================
2) Make it “bad”: remove helpful index (forces scans -> CPU-heavy parallel plan)
==============================================================================================*/
IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dbo.ComplianceFilings')
      AND name IN (N'IX_Filings_Status_DueDate', N'IX_Filings_Status_DueDate_Mem')
)
BEGIN
    BEGIN TRY
        DROP INDEX IX_Filings_Status_DueDate ON dbo.ComplianceFilings;
    END TRY BEGIN CATCH END CATCH;

    BEGIN TRY
        DROP INDEX IX_Filings_Status_DueDate_Mem ON dbo.ComplianceFilings;
    END TRY BEGIN CATCH END CATCH;
END
GO


/*==============================================================================================
3) BAD PLAN RUN (CPU spike)
Run this 3-5 times, or run from multiple windows.
==============================================================================================*/
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
GO

DECLARE @AsOfDateBad DATE = CAST(GETDATE() AS DATE);

SELECT
    e.country_code,
    e.client_name,
    cf.filing_type,
    COUNT_BIG(*) AS overdue_count
FROM dbo.ComplianceFilings cf
JOIN dbo.Entities e
    ON e.entity_id = cf.entity_id
WHERE
    cf.filing_status = N'OVERDUE'
    AND cf.due_date < @AsOfDateBad
GROUP BY
    e.country_code,
    e.client_name,
    cf.filing_type
ORDER BY
    overdue_count DESC
OPTION (MAXDOP 0);
GO

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


/*==============================================================================================
4) OBSERVE: Query Store scripts for CPU + parallel analysis
Run these in Window B after executing the report a few times.
==============================================================================================*/

-- 4A) Top CPU queries (last 24 hours) + DOP seen

SELECT TOP (15)
    q.query_id,
    p.plan_id,
    SUM(rs.count_executions) AS exec_count,
    CAST(SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS DECIMAL(18,2)) AS total_cpu_ms,
    CAST((SUM(rs.avg_cpu_time * rs.count_executions) / NULLIF(SUM(rs.count_executions),0)) / 1000.0 AS DECIMAL(18,2)) AS avg_cpu_ms,
    CAST((SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions),0)) / 1000.0 AS DECIMAL(18,2)) AS avg_duration_ms,
    CAST(MAX(rs.max_dop) AS INT) AS max_dop_seen,
    CAST(AVG(rs.avg_dop) AS DECIMAL(10,2)) AS avg_dop_seen,
    LEFT(qt.query_sql_text, 200) AS query_text_snippet
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p ON q.query_id = p.query_id
JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
JOIN sys.query_store_runtime_stats_interval rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
WHERE rsi.start_time >= DATEADD(HOUR, -24, SYSUTCDATETIME())
GROUP BY q.query_id, p.plan_id, qt.query_sql_text
ORDER BY total_cpu_ms DESC;
GO

-- 4B) Parallel offenders (high max_dop)
SELECT TOP (20)
    q.query_id,
    p.plan_id,
    SUM(rs.count_executions) AS exec_count,
    CAST(AVG(rs.avg_dop) AS DECIMAL(10,2)) AS avg_dop,
    CAST(MAX(rs.max_dop) AS INT) AS max_dop,
    CAST(AVG(rs.avg_cpu_time) / 1000.0 AS DECIMAL(18,2)) AS avg_cpu_ms,
    CAST(AVG(rs.avg_duration) / 1000.0 AS DECIMAL(18,2)) AS avg_duration_ms,
    LEFT(qt.query_sql_text, 200) AS query_text_snippet
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p ON q.query_id = p.query_id
JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
JOIN sys.query_store_runtime_stats_interval rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
WHERE rsi.start_time >= DATEADD(HOUR, -24, SYSUTCDATETIME())
GROUP BY q.query_id, p.plan_id, qt.query_sql_text
HAVING MAX(rs.max_dop) >= 4
ORDER BY max_dop DESC, avg_cpu_ms DESC;
GO


/*==============================================================================================
5) OBSERVE: File-level I/O latency (data/log/tempdb)
Tool: sys.dm_io_virtual_file_stats
==============================================================================================*/
SELECT
    DB_NAME(vfs.database_id) AS database_name,
    mf.type_desc AS file_type,
    mf.name AS logical_file_name,
    mf.physical_name,
    vfs.num_of_reads,
    CAST(vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads,0) AS DECIMAL(18,2)) AS avg_read_ms,
    vfs.num_of_writes,
    CAST(vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes,0) AS DECIMAL(18,2)) AS avg_write_ms,
    CAST((vfs.io_stall_read_ms + vfs.io_stall_write_ms) /
         NULLIF((vfs.num_of_reads + vfs.num_of_writes),0) AS DECIMAL(18,2)) AS avg_io_ms
FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
JOIN sys.master_files mf
  ON vfs.database_id = mf.database_id
 AND vfs.file_id = mf.file_id
ORDER BY avg_io_ms DESC;
GO


/*==============================================================================================
6) FIX: Create right index + update stats (reduces scan + CPU + parallel pressure)
==============================================================================================*/
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dbo.ComplianceFilings')
      AND name = N'IX_Filings_Status_DueDate'
)
BEGIN
    CREATE INDEX IX_Filings_Status_DueDate
    ON dbo.ComplianceFilings (filing_status, due_date)
    INCLUDE (entity_id, filing_type);
END
GO

UPDATE STATISTICS dbo.ComplianceFilings WITH FULLSCAN;
UPDATE STATISTICS dbo.Entities WITH FULLSCAN;
GO


/*==============================================================================================
7) FIXED RUN: controlled parallelism (use MAXDOP 2 for demo)
==============================================================================================*/
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
GO

DECLARE @AsOfDateFix DATE = CAST(GETDATE() AS DATE);

SELECT
    e.country_code,
    e.client_name,
    cf.filing_type,
    COUNT_BIG(*) AS overdue_count
FROM dbo.ComplianceFilings cf
JOIN dbo.Entities e
    ON e.entity_id = cf.entity_id
WHERE
    cf.filing_status = N'OVERDUE'
    AND cf.due_date < @AsOfDateFix
GROUP BY
    e.country_code,
    e.client_name,
    cf.filing_type
ORDER BY
    overdue_count DESC
OPTION (MAXDOP 2);
GO

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


/*==============================================================================================
8) VERIFY: Query Store after fix (run after executing fixed query a few times)
==============================================================================================*/
SELECT TOP (15)
    q.query_id,
    p.plan_id,
    SUM(rs.count_executions) AS exec_count,
    CAST(SUM(rs.avg_cpu_time * rs.count_executions) / 1000.0 AS DECIMAL(18,2)) AS total_cpu_ms,
    CAST((SUM(rs.avg_cpu_time * rs.count_executions) / NULLIF(SUM(rs.count_executions),0)) / 1000.0 AS DECIMAL(18,2)) AS avg_cpu_ms,
    CAST(AVG(rs.avg_dop) AS DECIMAL(10,2)) AS avg_dop_seen,
    LEFT(qt.query_sql_text, 200) AS query_text_snippet
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p ON q.query_id = p.query_id
JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
JOIN sys.query_store_runtime_stats_interval rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
WHERE rsi.start_time >= DATEADD(HOUR, -24, SYSUTCDATETIME())
GROUP BY q.query_id, p.plan_id, qt.query_sql_text
ORDER BY total_cpu_ms DESC;
GO

PRINT 'Module 6 demo: Compare scans vs seeks, CPU/time, DOP, and file latency.';
GO
