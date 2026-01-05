-- CSC_Global_Module2_Demo.sql
/*==============================================================================
 CSC Global - Demo Pack (Single Script)
 Topic: Execution Plans & Query Tuning (Bad plan → Observe → Fix → Verify)

 Works with your schema:
   dbo.Entities, dbo.Jurisdictions, dbo.ComplianceFilings
 Assumes you already loaded seed data (Sections 0–6).

 How to use in class (super simple):
  A) Open TWO SSMS windows:
     - Window 1: Run the "BAD PLAN" report (Step 2)
     - Window 2: Run the "OBSERVE" DMVs while it is running (Step 3)
  B) Then run FIX (Step 4) and VERIFY (Step 5)
==============================================================================*/

-------------------------------------------------------------------------------
-- STEP 0) Class setup toggles (optional but recommended)
-------------------------------------------------------------------------------
-- Show IO + TIME in Messages tab for before/after comparison
SET NOCOUNT ON;
SET STATISTICS IO ON;
SET STATISTICS TIME ON;

-- (Instructor) Tell participants: Turn on Actual Execution Plan (Ctrl+M)
-- In Azure Data Studio: enable "Include Actual Execution Plan" similarly.

-------------------------------------------------------------------------------
-- STEP 1) Baseline safety checks (optional quick sanity)
-------------------------------------------------------------------------------
-- Quick row counts (helps confirm dataset scale)
SELECT
  (SELECT COUNT(*) FROM dbo.Entities)              AS entities_count,
  (SELECT COUNT(*) FROM dbo.Jurisdictions)         AS jurisdictions_count,
  (SELECT COUNT(*) FROM dbo.ComplianceFilings)     AS filings_count;

-------------------------------------------------------------------------------
-- STEP 2) Create a "BAD PLAN" scenario (slow query)
--         Goal: force scans + memory-heavy operators (Hash Aggregate + Sort)
-------------------------------------------------------------------------------

-- 2.1 Drop the helpful index (if it exists)
IF EXISTS (
  SELECT 1
  FROM sys.indexes
  WHERE name = 'IX_Filings_Status_DueDate'
    AND object_id = OBJECT_ID('dbo.ComplianceFilings')
)
BEGIN
  DROP INDEX IX_Filings_Status_DueDate ON dbo.ComplianceFilings;
END
GO

-- 2.2 (Optional) Make stats more likely to be stale (demo effect)
-- NOTE: We do NOT recommend turning off auto stats in production without care.
-- This is a controlled demo only.
-- ALTER DATABASE CURRENT SET AUTO_UPDATE_STATISTICS OFF;

-- 2.3 BAD PLAN report (run this in Window 1)
--     This often produces: scans → hash join/aggregate → sort → big memory grant

DECLARE @AsOfDate_Bad DATE = CAST(GETDATE() AS DATE);

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
    AND cf.due_date < @AsOfDate_Bad
GROUP BY
    e.country_code,
    e.client_name,
    cf.filing_type
ORDER BY
    overdue_count DESC;
GO

-------------------------------------------------------------------------------
-- STEP 3) OBSERVE: Run these WHILE Step 2 query is executing (Window 2)
--         Pick the session_id of the slow query and plug it into @sid
-------------------------------------------------------------------------------

-- 3.1 Identify running requests, waits, reads
SELECT
    r.session_id,
    r.status,
    r.command,
    r.cpu_time,
    r.total_elapsed_time,
    r.logical_reads,
    r.reads,
    r.writes,
    r.wait_type,
    r.wait_time,
    r.last_wait_type,
    r.blocking_session_id
FROM sys.dm_exec_requests r
WHERE r.session_id > 50
ORDER BY r.total_elapsed_time DESC;
GO

-- 3.2 Get SQL text for the chosen session
DECLARE @sid INT = NULL; -- <-- set this to session_id of the slow report (e.g., 57)
SELECT
    r.session_id,
    t.text
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE r.session_id = @sid;
GO

-- 3.3 Scheduler pressure (CPU queue)
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

-- 3.4 Memory grants (sort/hash operators often request large grants)
SELECT
    mg.session_id,
    mg.request_time,
    mg.grant_time,
    mg.requested_memory_kb,
    mg.granted_memory_kb,
    mg.used_memory_kb,
    mg.max_used_memory_kb,
    mg.wait_time_ms,
    mg.is_next_candidate
FROM sys.dm_exec_query_memory_grants mg
ORDER BY mg.requested_memory_kb DESC;
GO

-------------------------------------------------------------------------------
-- STEP 4) FIX: Add the right index + refresh statistics
--         Goal: Seek/filter earlier, reduce row flow, reduce memory grant, avoid spills
-------------------------------------------------------------------------------

-- 4.1 Recreate the helpful covering index
CREATE INDEX IX_Filings_Status_DueDate
ON dbo.ComplianceFilings (filing_status, due_date)
INCLUDE (entity_id, jurisdiction_id, filing_type);
GO

-- 4.2 Add an index to help grouping by client/country (optional but nice for demo)
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes
  WHERE name = 'IX_Entities_Country_Client'
    AND object_id = OBJECT_ID('dbo.Entities')
)
BEGIN
  CREATE INDEX IX_Entities_Country_Client
  ON dbo.Entities (country_code, client_name)
  INCLUDE (entity_id);
END
GO

-- 4.3 Update statistics (helps cardinality estimation → better join/agg choices)
UPDATE STATISTICS dbo.ComplianceFilings WITH FULLSCAN;
UPDATE STATISTICS dbo.Entities WITH FULLSCAN;
GO

-- (Optional) Re-enable auto stats if you disabled earlier
-- ALTER DATABASE CURRENT SET AUTO_UPDATE_STATISTICS ON;

-------------------------------------------------------------------------------
-- STEP 5) VERIFY: Run the same report again (should improve)
-------------------------------------------------------------------------------
DECLARE @AsOfDate_Good DATE = CAST(GETDATE() AS DATE);

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
    AND cf.due_date < @AsOfDate_Good
GROUP BY
    e.country_code,
    e.client_name,
    cf.filing_type
ORDER BY
    overdue_count DESC;
GO

-------------------------------------------------------------------------------
-- STEP 6) Wrap up
-------------------------------------------------------------------------------
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

PRINT 'Demo complete: Compare (1) reads, (2) elapsed time, (3) plan operators, (4) memory grant.';