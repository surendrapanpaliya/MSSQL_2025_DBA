
/**********************************************************************************************
CSC Global - SQL Server DBA Demo Pack (Locks & Concurrency)
Database: CSC_DBA_Demo
Author: Surendra Panpaliya
Purpose:
  - Hands-on lock scenarios + solutions using the existing CSC_DBA_Demo database
  - Includes monitoring queries (DMVs), Extended Events notes, and safe cleanup steps
How to run:
  - Use SSMS with two query windows (Session A and Session B) where indicated.
  - Read the comments carefully; they tell you exactly what to do and what you should observe.
************************************************************************************************/

USE CSC_DBA_Demo;
GO

/*==============================================================================================
0) Quick Lock Mode Primer (read-only reference)
----------------------------------------------------------------------------------------------
Lock modes you will commonly see in sys.dm_tran_locks:

S   = Shared            (read lock)
X   = Exclusive         (write lock)
U   = Update            (prevents deadlocks in read-then-write patterns; converts to X)
IS  = Intent Shared     (signals lower-level S locks exist/needed)
IX  = Intent Exclusive  (signals lower-level X locks exist/needed)
SIX = Shared + Intent Exclusive (read many rows + modify some rows)

Sch-S = Schema Stability       (normal queries; prevents schema changes while query runs)
Sch-M = Schema Modification    (DDL changes; blocks most access)

Key takeaway:
  - Intent locks (IS/IX) are NORMAL and indicate locking at lower levels (KEY/RID/PAGE).
  - Blocking happens when incompatible locks conflict (e.g., S vs X, Sch-S vs Sch-M).
==============================================================================================*/


/*==============================================================================================
1) Monitoring Toolkit (run anytime)
==============================================================================================*/

-- 1.1 Who is running / waiting right now?

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

-- 1.2 What SQL is a session running? (Set @sid to a session_id from above)

DECLARE @sid INT = NULL; -- set e.g. 56
SELECT
    r.session_id,
    t.text
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE r.session_id = @sid;
GO

-- 1.3 What locks exist right now? (focus on user sessions > 50)

SELECT
    tl.request_session_id AS session_id,
    tl.resource_type,
    tl.resource_database_id,
    tl.resource_associated_entity_id,
    tl.request_mode,
    tl.request_status
FROM sys.dm_tran_locks tl
WHERE tl.request_session_id > 50
ORDER BY tl.request_session_id, tl.resource_type;
GO

-- 1.4 If blocked, show blocking chain (simple)

SELECT
    r.session_id,
    r.blocking_session_id,
    r.status,
    r.wait_type,
    r.wait_time,
    r.last_wait_type,
    r.command
FROM sys.dm_exec_requests r
WHERE r.session_id > 50
  AND (r.blocking_session_id <> 0 OR r.wait_type LIKE 'LCK_M%')
ORDER BY r.wait_time DESC;
GO


/*==============================================================================================
2) Use Case A - "Writer blocks Readers" (X blocks S)
Scenario:
  - An OLTP update holds X locks.
  - A dashboard SELECT waits for shared locks (LCK_M_S).
What you learn:
  - How to reproduce blocking, observe it via DMVs, then fix using RCSI (row versioning).
==============================================================================================*/

-- ===== Session A (Window 1) =====
-- Step A1: Start a transaction and hold locks.

BEGIN TRAN;

-- This update will take X locks on matching KEY/RID and IX on higher levels.
UPDATE cf
SET notes = N'Hold-lock demo - ' + CONVERT(NVARCHAR(19), SYSDATETIME(), 120)
FROM dbo.ComplianceFilings cf
WHERE cf.filing_status = N'OVERDUE'
  AND cf.due_date < DATEADD(DAY, -30, CAST(GETDATE() AS DATE));

-- IMPORTANT: Do NOT commit yet. Keep this window open.
-- Expectation: Locks remain held until COMMIT/ROLLBACK.

-- ===== Session B (Window 2) =====
-- Step B1: Run a read that touches the same rows.
-- In default READ COMMITTED, this will try to take S locks and may block behind Session A's X locks.
-- Observe LCK_M_S in sys.dm_exec_requests.
-- (Run in a separate window)
-- SELECT TOP (50)
--     cf.filing_id, cf.entity_id, cf.filing_status, cf.due_date, cf.notes
-- FROM dbo.ComplianceFilings cf
-- WHERE cf.filing_status = N'OVERDUE'
--   AND cf.due_date < DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
-- ORDER BY cf.due_date;

-- ===== Observe =====
-- In a 3rd window (or same Session B after it blocks), run Monitoring Toolkit 1.1 / 1.3.
-- You should see:
--   - Session B waiting with wait_type = LCK_M_S (or similar)
--   - Session A holding X/IX locks

-- ===== Fix Option 1 (Best practice for read-heavy reporting): Enable RCSI =====
/*
RCSI reduces reader/writer blocking by using row versions for READ COMMITTED readers.
Readers don't take long S locks on data rows, so they don't block behind writers.
Use in many OLTP systems after testing.

Run once (requires exclusive access in some environments; do in maintenance window):
ALTER DATABASE CSC_DBA_Demo SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
*/

-- ===== Clean up Session A =====
-- When done observing, release locks:
-- COMMIT;  -- or ROLLBACK;


/*==============================================================================================
3) Use Case B - "Reader blocks Writer" (S blocks X)
Scenario:
  - A long-running SELECT holds shared locks (S).
  - An UPDATE tries to take X and waits (LCK_M_X).
What you learn:
  - Even reads can block writes if row-versioning is not enabled.
==============================================================================================*/

-- ===== Session A =====
-- Use REPEATABLE READ to hold S locks until transaction ends.

SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN TRAN;

SELECT TOP (5000)
    cf.filing_id, cf.entity_id, cf.filing_status, cf.due_date
FROM dbo.ComplianceFilings cf
WHERE cf.filing_status = N'OVERDUE'
ORDER BY cf.due_date;

-- Keep the transaction open (do not commit yet).
-- Locks (S/IS) will remain.

-- ===== Session B =====
-- This update may block waiting for X.
-- UPDATE dbo.ComplianceFilings
-- SET notes = N'Writer waiting demo'
-- WHERE filing_id IN (SELECT TOP (10) filing_id FROM dbo.ComplianceFilings WHERE filing_status = N'OVERDUE' ORDER BY due_date);

-- Observe waits:
--   - Session B: LCK_M_X
--   - Session A: holding S locks

-- Cleanup Session A:
-- COMMIT;
-- Reset isolation level to default:
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
GO


/*==============================================================================================
4) Use Case C - Lock Escalation (many row locks -> table lock)
Scenario:
  - A large UPDATE touches many rows.
  - SQL may escalate locks from many KEY locks to OBJECT-level lock.
What you learn:
  - Why missing indexes and large updates can "freeze" a table.
  - Safer pattern: batching.
==============================================================================================*/

-- WARNING: This is a demo; keep it small if your laptop is limited.

-- 4.1 Check approximate row counts first (safe)
SELECT
    SUM(CASE WHEN filing_status = N'OVERDUE' THEN 1 ELSE 0 END) AS overdue_rows,
    COUNT_BIG(*) AS total_rows
FROM dbo.ComplianceFilings;
GO

-- 4.2 Batching pattern (recommended)
-- Update overdue rows in chunks to reduce lock footprint and log pressure.
DECLARE @BatchSize INT = 5000;

WHILE (1=1)
BEGIN
    ;WITH c AS
    (
        SELECT TOP (@BatchSize) filing_id
        FROM dbo.ComplianceFilings WITH (READPAST)
        WHERE filing_status = N'OVERDUE'
        ORDER BY filing_id
    )
    UPDATE cf
    SET notes = N'Batch updated - ' + CONVERT(NVARCHAR(19), SYSDATETIME(), 120)
    FROM dbo.ComplianceFilings cf
    JOIN c ON c.filing_id = cf.filing_id;

    IF @@ROWCOUNT = 0 BREAK;
END
GO

-- Observe locks while running with sys.dm_tran_locks.
-- If escalation occurs, you may see OBJECT X or OBJECT IX with fewer KEY locks overall.


/*==============================================================================================
5) Use Case D - Deadlock (two sessions, opposite order)
Scenario:
  - Session A updates row set 1 then row set 2
  - Session B updates row set 2 then row set 1
What you learn:
  - How deadlocks happen
  - How to fix: consistent access order + indexes + shorter transactions
==============================================================================================*/

-- Preparation: pick two different entity_ids that exist
SELECT TOP (5) entity_id FROM dbo.Entities ORDER BY entity_id;
GO

-- Choose two entity_ids (example uses 1 and 2; adjust if needed).
-- ===== Session A =====
-- BEGIN TRAN;
-- UPDATE dbo.ComplianceFilings SET notes = N'Deadlock-A-1'
-- WHERE entity_id = 1 AND filing_status = N'OVERDUE';
-- WAITFOR DELAY '00:00:05';
-- UPDATE dbo.ComplianceFilings SET notes = N'Deadlock-A-2'
-- WHERE entity_id = 2 AND filing_status = N'OVERDUE';
-- COMMIT;

-- ===== Session B (start quickly after A begins) =====
-- BEGIN TRAN;
-- UPDATE dbo.ComplianceFilings SET notes = N'Deadlock-B-2'
-- WHERE entity_id = 2 AND filing_status = N'OVERDUE';
-- WAITFOR DELAY '00:00:05';
-- UPDATE dbo.ComplianceFilings SET notes = N'Deadlock-B-1'
-- WHERE entity_id = 1 AND filing_status = N'OVERDUE';
-- COMMIT;

-- Expected:
--  - One session becomes deadlock victim (Error 1205).
-- Fix strategies:
--  1) Always access resources in same order (entity_id ascending)
--  2) Add supporting indexes to reduce lock duration
--  3) Keep transactions short
--  4) Retry logic in application (for deadlock victims)


/*==============================================================================================
6) Use Case E - Schema lock (Sch-M blocks everything)
Scenario:
  - DDL attempts to modify schema while queries hold Sch-S.
What you learn:
  - Why ALTER TABLE waits on LCK_M_SCH_M
  - Best practice: maintenance window / ONLINE where possible
==============================================================================================*/

-- ===== Session A =====
-- Run a long query to hold Sch-S (add WAITFOR to simulate long running)
-- BEGIN TRAN;
-- SELECT COUNT_BIG(*) FROM dbo.ComplianceFilings WITH (HOLDLOCK);
-- WAITFOR DELAY '00:00:30';
-- COMMIT;

-- ===== Session B =====
-- Try DDL while Session A is running:
-- ALTER TABLE dbo.ComplianceFilings ADD demo_col INT NULL;

-- Observe:
--  - Session B waiting with LCK_M_SCH_M
-- Cleanup:
--  - Let Session A finish, then rerun DDL


/*==============================================================================================
7) Use Case F - Isolation levels + Range locks (Serializable)
Scenario:
  - SERIALIZABLE can block inserts into a range (phantom protection).
What you learn:
  - Range locks exist to prevent phantom rows
  - Prefer Snapshot/RCSI for many reporting patterns
==============================================================================================*/

-- Setup: choose an entity_id you will use for both sessions
DECLARE @E INT = (SELECT TOP (1) entity_id FROM dbo.Entities ORDER BY entity_id);
SELECT @E AS demo_entity_id;
GO

-- ===== Session A =====
-- SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- BEGIN TRAN;
-- SELECT COUNT_BIG(*)
-- FROM dbo.ComplianceFilings
-- WHERE entity_id = @E AND due_date < CAST(GETDATE() AS DATE);
-- -- Keep open:
-- WAITFOR DELAY '00:00:20';
-- COMMIT;
-- SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- ===== Session B =====
-- While A is open, try inserting a row that fits the predicate:
-- INSERT INTO dbo.ComplianceFilings(entity_id, jurisdiction_id, filing_type, due_date, filing_status, notes)
-- SELECT TOP (1)
--     @E,
--     j.jurisdiction_id,
--     N'Annual Return',
--     DATEADD(DAY, -1, CAST(GETDATE() AS DATE)),
--     N'OVERDUE',
--     N'Range lock demo'
-- FROM dbo.Jurisdictions j
-- WHERE j.country_code = (SELECT country_code FROM dbo.Entities WHERE entity_id = @E);

-- Observe:
--  - Insert may block due to range locks.
-- Notes:
--  - Range lock modes may appear as RangeS-S / RangeX-X in deadlock graphs, not always as simple lock mode labels.


/*==============================================================================================
8) Practical "Solutions" Checklist (DBA playbook)
----------------------------------------------------------------------------------------------
When you see blocking / lock waits (LCK_M_*):
1) Identify blocker and victim
   - sys.dm_exec_requests (blocking_session_id)
   - sys.dm_tran_locks (request_mode/status)
   - sys.dm_exec_sql_text (what SQL)
2) Decide root cause:
   - Long transaction?
   - Missing index causing scan?
   - Hot spot (same key/page)?
   - Isolation level too strict?
   - DDL at wrong time?
3) Apply right fix:
   - Add/adjust index (reduce scan + lock duration)
   - Batch large modifications
   - Shorten transaction scope
   - Enable RCSI/SNAPSHOT for read-heavy systems
   - Schedule DDL and index rebuilds in maintenance windows
==============================================================================================*/


/*==============================================================================================
9) OPTIONAL: Fast reference - lock modes and what they usually mean in your output
----------------------------------------------------------------------------------------------
If you see in sys.dm_tran_locks output:

request_mode = S
  - Read lock (shared). Many can coexist. Blocks X.

request_mode = X
  - Write lock (exclusive). Blocks most others.

request_mode = IS / IX
  - Intent locks at higher granularity, indicating lower locks exist.
  - IX on OBJECT + X on KEY is typical for updates.

request_mode = Sch-S
  - Query is using the table. DDL will wait.

request_mode = Sch-M
  - DDL change. Queries wait.

request_status = GRANT
  - Lock acquired.

request_status = WAIT
  - Session waiting; check dm_exec_requests.wait_type (LCK_M_*).
==============================================================================================*/

-- End of script
