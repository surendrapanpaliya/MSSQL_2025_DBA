/**********************************************************************************************
CSC Global DBA Demo Pack - Module 5 (TempDB & Memory Tuning)
Database: CSC_DBA_Demo

Use Cases:
  1) Detect TempDB contention
  2) Fix spills to TempDB
  3) Tune / observe memory grants
  4) Analyze memory clerks + internal/external pressure

Tools used (as requested):
  - sys.dm_os_memory_clerks
  - sys.dm_exec_query_memory_grants
  - sys.dm_os_process_memory

How to run in class:
  - Use 2 SSMS windows:
      Window A = run “heavy query”
      Window B = run DMVs while A is running
  - Turn on Actual Execution Plan (Ctrl+M) for heavy queries.
************************************************************************************************/

USE CSC_DBA_Demo;
GO

/*==============================================================================================
0) Setup helper: show “today” and row counts (so trainees know scale)
==============================================================================================*/
SELECT
    CAST(GETDATE() AS DATE) AS today,
    (SELECT COUNT_BIG(*) FROM dbo.Entities) AS entity_rows,
    (SELECT COUNT_BIG(*) FROM dbo.ComplianceFilings) AS filings_rows;
GO


/*==============================================================================================
USE CASE 1: Detect TempDB contention (symptoms + evidence)
Business Scenario (CSC):
  - Morning global overdue dashboard starts at 9 AM.
  - Many users run the report concurrently; several queries do sorts/hashes.
  - TempDB allocations become a hotspot (latch contention).
Goal:
  - Prove whether tempdb is involved and whether it is “latch” contention vs “IO” contention.
==============================================================================================*/

----------------------------------------------------------------------------------------------
-- 1A) Evidence: waits related to TempDB latch contention (PAGELATCH_*)
-- Purpose:
--   Latch waits = contention on in-memory pages (often PFS/GAM/SGAM allocation maps in tempdb).
-- Output:
--   Higher wait_time_ms + waiting_tasks_count for PAGELATCH_* indicates allocation hot pages.
----------------------------------------------------------------------------------------------
SELECT TOP (20)
    ws.wait_type,
    ws.waiting_tasks_count,
    ws.wait_time_ms,
    ws.signal_wait_time_ms
FROM sys.dm_os_wait_stats ws
WHERE ws.wait_type LIKE N'PAGELATCH_%'
ORDER BY ws.wait_time_ms DESC;
GO

----------------------------------------------------------------------------------------------
-- 1B) Current running/waiting requests (run WHILE workload is running)
-- Purpose:
--   Show which sessions are waiting and what they are waiting on.
-- Output:
--   wait_type may show PAGELATCH_UP / PAGELATCH_EX; combine with session_id for drilldown.
----------------------------------------------------------------------------------------------
SELECT
    r.session_id,
    r.status,
    r.command,
    r.cpu_time,
    r.total_elapsed_time,
    r.logical_reads,
    r.wait_type,
    r.wait_time,
    r.last_wait_type,
    r.blocking_session_id
FROM sys.dm_exec_requests r
WHERE r.session_id > 50
ORDER BY r.total_elapsed_time DESC;
GO

----------------------------------------------------------------------------------------------
-- 1C) Map hot latch waits to dbid:fileid:pageid (confirm tempdb = dbid 2)
-- Purpose:
--   resource_description like "2:1:12345" (dbid:fileid:pageid). If dbid=2 => tempdb.
-- Output:
--   Helps prove tempdb allocation contention, not user DB locking.
----------------------------------------------------------------------------------------------
SELECT TOP (20)
    wt.session_id,
    wt.wait_type,
    wt.wait_duration_ms,
    wt.resource_description
FROM sys.dm_os_waiting_tasks wt
WHERE wt.wait_type LIKE N'PAGELATCH_%'
ORDER BY wt.wait_duration_ms DESC;
GO


/*==============================================================================================
USE CASE 2: Fix spills to TempDB (Sort/Hash spill)
Business Scenario (CSC):
  - Stakeholders complain the overdue report is “sometimes” slow.
  - Root cause: a plan with Hash Aggregate + Sort gets a small memory grant and spills to tempdb.
Goal:
  - Run a query that commonly needs workspace memory.
  - Observe spill warnings in Actual Execution Plan.
  - Apply fixes: better index + better stats.
==============================================================================================*/

----------------------------------------------------------------------------------------------
-- 2A) Heavy query likely to request workspace memory (Hash/Sort)
-- Purpose:
--   Aggregation + ORDER BY often needs memory. If memory is insufficient, it may spill to tempdb.
-- Output:
--   Result set = overdue counts by country+client+type.
-- Plan to look for:
--   - Hash Match (Aggregate) and/or Sort operators
--   - Warnings: "Spill to tempdb" or "Hash/Sort warning"
----------------------------------------------------------------------------------------------
DECLARE @AsOfDate DATE = CAST(GETDATE() AS DATE);

SELECT
    e.country_code,
    e.client_name,
    cf.filing_type,
    COUNT_BIG(*) AS overdue_count
FROM dbo.ComplianceFilings cf
JOIN dbo.Entities e ON e.entity_id = cf.entity_id
WHERE
    cf.filing_status = N'OVERDUE'
    AND cf.due_date < @AsOfDate
GROUP BY
    e.country_code,
    e.client_name,
    cf.filing_type
ORDER BY
    overdue_count DESC;
GO

----------------------------------------------------------------------------------------------
-- 2B) Fix #1: Create a selective covering index for the WHERE + join columns
-- Purpose:
--   Reduce rows touched, reduce hash/sort workload, reduce memory grant size.
-- Output:
--   Index created. Plan should move from Scan to Seek and/or reduce operator costs.
----------------------------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dbo.ComplianceFilings')
      AND name = N'IX_Filings_Status_DueDate_Mem'
)
BEGIN
    CREATE INDEX IX_Filings_Status_DueDate_Mem
    ON dbo.ComplianceFilings (filing_status, due_date)
    INCLUDE (entity_id, filing_type);
END
GO

----------------------------------------------------------------------------------------------
-- 2C) Fix #2: Update stats (improves cardinality estimates -> better memory grant)
-- Purpose:
--   Stale stats can underestimate rows, leading to too-small memory grant -> spill.
-- Output:
--   Stats refreshed. Rerun query and compare: estimated vs actual rows; spill warnings.
----------------------------------------------------------------------------------------------
UPDATE STATISTICS dbo.ComplianceFilings WITH FULLSCAN;
UPDATE STATISTICS dbo.Entities WITH FULLSCAN;
GO

----------------------------------------------------------------------------------------------
-- 2D) Re-run heavy query after fixes (compare plan + runtime)
----------------------------------------------------------------------------------------------
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


/*==============================================================================================
USE CASE 3: Tune / Observe memory grants
Business Scenario (CSC):
  - During the report window, some queries run, some wait, and spills happen.
  - DBA needs to answer:
      “Which sessions requested big memory grants?”
      “Who is waiting for memory (RESOURCE_SEMAPHORE)?”
Goal:
  - Use sys.dm_exec_query_memory_grants to see requested/granted/used memory.
==============================================================================================*/

----------------------------------------------------------------------------------------------
-- 3A) Memory grants scoreboard (run WHILE heavy query is running)
-- Purpose:
--   Shows per-request workspace memory (sort/hash) grants.
-- Output:
--   - requested_memory_kb: optimizer asked for
--   - granted_memory_kb: what it actually got
--   - used_memory_kb/max_used_memory_kb: real consumption
--   - wait_time_ms + is_next_candidate: waiting for a grant
----------------------------------------------------------------------------------------------
SELECT TOP (25)
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

----------------------------------------------------------------------------------------------
-- 3B) Tie a big grant to the SQL text (what query is it?)
-- Purpose:
--   Identify the exact statement responsible for the grant.
-- Output:
--   statement_text = the currently running statement that requested/uses the grant.
----------------------------------------------------------------------------------------------
SELECT TOP (25)
    mg.session_id,
    mg.requested_memory_kb,
    mg.granted_memory_kb,
    r.status,
    r.wait_type,
    SUBSTRING(t.text, (r.statement_start_offset/2)+1,
              ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text)
                ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1) AS statement_text
FROM sys.dm_exec_query_memory_grants mg
JOIN sys.dm_exec_requests r
  ON r.session_id = mg.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
ORDER BY mg.requested_memory_kb DESC;
GO


/*==============================================================================================
USE CASE 4: Analyze memory clerks + internal vs external memory pressure
Business Scenario (CSC):
  - “SQL Server is slow today” — but is it query pressure or OS pressure?
Goal:
  - Use memory clerks to see where SQL Server memory is going.
  - Use dm_os_process_memory to detect external pressure signals.
==============================================================================================*/

----------------------------------------------------------------------------------------------
-- 4A) Memory clerks: where is SQL Server spending memory?
-- Purpose:
--   Break down memory usage by internal components (“clerks”).
-- Output (what to look for):
--   - MEMORYCLERK_SQLBUFFERPOOL: data cache (buffer pool)
--   - MEMORYCLERK_SQLQUERYEXEC: query execution (workspace)
--   - CACHESTORE_SQLCP / CACHESTORE_OBJCP: plan cache
----------------------------------------------------------------------------------------------
SELECT TOP (30)
    mc.type,
    mc.name,
    SUM(mc.pages_kb) AS pages_kb,
    SUM(mc.virtual_memory_committed_kb) AS vm_committed_kb,
    SUM(mc.shared_memory_committed_kb) AS shared_committed_kb
FROM sys.dm_os_memory_clerks mc
GROUP BY mc.type, mc.name
ORDER BY SUM(mc.pages_kb) DESC;
GO

----------------------------------------------------------------------------------------------
-- 4B) Process memory: detect external memory pressure
-- Purpose:
--   OS-level view from SQL Server’s perspective.
-- Output interpretation:
--   - process_physical_memory_low / process_virtual_memory_low = external pressure flags
--   - available_physical_memory_kb low => OS pressure
--   - memory_utilization_percentage high => SQL is consuming most available memory
----------------------------------------------------------------------------------------------
SELECT
    pm.physical_memory_in_use_kb,
    pm.large_page_allocations_kb,
    pm.locked_page_allocations_kb,
    pm.total_virtual_address_space_kb,
    pm.virtual_address_space_committed_kb,
    pm.virtual_address_space_available_kb,
    pm.page_fault_count,
    pm.memory_utilization_percentage,
    pm.available_commit_limit_kb,
    pm.process_physical_memory_low,
    pm.process_virtual_memory_low
FROM sys.dm_os_process_memory pm;
GO

PRINT 'Module 5 demo pack ready. Run the heavy query, then run the DMVs while it executes.';
GO