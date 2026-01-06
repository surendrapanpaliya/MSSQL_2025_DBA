/*==============================================================================
 CSC Global - Full Concurrency Demo Pack
 Topic: Blocking → Deadlock → RCSI Fix + Deadlock Graph Walkthrough
 Audience: Senior DBAs

 Prereqs:
   - Run in a LAB database (NOT production)
   - Two query windows (Session A and Session B) in SSMS
   - Permissions:
       * ALTER DATABASE (for RCSI)
       * CREATE EVENT SESSION (for deadlock capture) - typically sysadmin

 Uses your existing tables:
   dbo.Entities, dbo.ComplianceFilings

 Safety:
   - Uses small, targeted updates (entity_id 1 and 2)
==============================================================================*/

-------------------------------------------------------------------------------
-- PART 0) Setup (Run ONCE by Instructor)
-------------------------------------------------------------------------------
USE CSC_DBA_Demo;  -- change if needed
GO

-- Make sure XACT_ABORT is ON for safety in labs (auto-rollback on runtime errors)
SET XACT_ABORT ON;
GO

-- (Optional) Confirm rows exist
SELECT TOP (5) entity_id, client_name, country_code FROM dbo.Entities ORDER BY entity_id;
GO

-------------------------------------------------------------------------------
-- PART 1) BLOCKING DEMO (Read blocked by Write) - Two sessions
-- Goal: Show blocking chain and how to observe it using DMVs.
-------------------------------------------------------------------------------

/*--------------------------
 SESSION A (Window 1)
--------------------------*/
-- Run this in Session A:
BEGIN TRAN;

-- Take an X lock on a single row in Entities
UPDATE dbo.Entities
SET client_name = client_name  -- no-op update still takes locks
WHERE entity_id = 1;

-- Keep transaction open to hold locks (DO NOT COMMIT YET)
-- Instructor: leave this window as-is for 30–60 seconds.


/*--------------------------
 SESSION B (Window 2)
--------------------------*/
-- Run this in Session B (it will block under READ COMMITTED):
SET LOCK_TIMEOUT 10000; -- 10 seconds so it doesn't hang forever in class

SELECT entity_id, client_name, country_code
FROM dbo.Entities
WHERE entity_id = 1;
-- Expected: waits / times out while Session A holds the X lock


/*--------------------------
 OBSERVE (Window 2 or 3)
--------------------------*/
-- While Session B is blocked, run:
SELECT
    r.session_id,
    r.blocking_session_id,
    r.status,
    r.command,
    r.wait_type,
    r.wait_time,
    r.last_wait_type,
    r.cpu_time,
    r.total_elapsed_time
FROM sys.dm_exec_requests r
WHERE r.session_id IN (@@SPID)  -- in this window
   OR r.blocking_session_id <> 0
ORDER BY r.total_elapsed_time DESC;

-- Show lock details (who holds what)
SELECT
    tl.request_session_id AS session_id,
    tl.resource_type,
    tl.resource_database_id,
    tl.resource_associated_entity_id,
    tl.request_mode,
    tl.request_status
FROM sys.dm_tran_locks tl
WHERE tl.resource_database_id = DB_ID()
ORDER BY tl.request_session_id, tl.resource_type;

-- Get SQL text for blocked & blocker sessions
;WITH blockers AS
(
    SELECT DISTINCT blocking_session_id AS sid
    FROM sys.dm_exec_requests
    WHERE blocking_session_id <> 0
),
blocked AS
(
    SELECT DISTINCT session_id AS sid
    FROM sys.dm_exec_requests
    WHERE blocking_session_id <> 0
)
SELECT
    s.sid AS session_id,
    t.text
FROM (SELECT sid FROM blockers UNION ALL SELECT sid FROM blocked) s
JOIN sys.dm_exec_requests r ON r.session_id = s.sid
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t;


-- Cleanup for blocking demo:
-- SESSION A: COMMIT to release locks
-- (Run in Session A)
COMMIT;
GO

-------------------------------------------------------------------------------
-- PART 2) DEADLOCK DEMO - Two sessions + Extended Events capture
-- Goal: Create a real deadlock and capture the deadlock graph.
-------------------------------------------------------------------------------

/*--------------------------------------------------------------------------
  2.1 Create an Extended Events session to capture deadlocks (Run ONCE)
--------------------------------------------------------------------------*/
IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = 'Deadlock_Monitor_CSC')
BEGIN
    DROP EVENT SESSION Deadlock_Monitor_CSC ON SERVER;
END
GO

CREATE EVENT SESSION Deadlock_Monitor_CSC
ON SERVER
ADD EVENT sqlserver.xml_deadlock_report
ADD TARGET package0.ring_buffer
WITH (MAX_MEMORY = 4096 KB, EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS);
GO

ALTER EVENT SESSION Deadlock_Monitor_CSC ON SERVER STATE = START;
GO

/*--------------------------------------------------------------------------
  2.2 Deadlock scenario
  Strategy:
    - Session A locks Entities(1) then tries Entities(2)
    - Session B locks Entities(2) then tries Entities(1)
  This creates a cycle: A waits for B, B waits for A.
--------------------------------------------------------------------------*/

/*--------------------------
 SESSION A (Window 1)
--------------------------*/
-- Run in Session A:
SET DEADLOCK_PRIORITY NORMAL;
BEGIN TRAN;

UPDATE dbo.Entities
SET client_name = client_name
WHERE entity_id = 1;

-- Pause so Session B can lock entity_id = 2
WAITFOR DELAY '00:00:05';

-- Now try to lock the row Session B has
UPDATE dbo.Entities
SET client_name = client_name
WHERE entity_id = 2;

COMMIT;  -- likely won't reach if chosen as deadlock victim
GO


/*--------------------------
 SESSION B (Window 2)
--------------------------*/
-- Run in Session B quickly after Session A starts:
SET DEADLOCK_PRIORITY NORMAL;
BEGIN TRAN;

UPDATE dbo.Entities
SET client_name = client_name
WHERE entity_id = 2;

-- Pause so Session A is holding entity_id = 1
WAITFOR DELAY '00:00:05';

-- Now try to lock the row Session A has
UPDATE dbo.Entities
SET client_name = client_name
WHERE entity_id = 1;

COMMIT; -- one session will fail with deadlock error 1205
GO

/*--------------------------------------------------------------------------
  Expected Output:
   One session gets:
     Msg 1205, Level 13, State ...
     Transaction (Process ID ...) was deadlocked on lock resources ...
     and has been chosen as the deadlock victim.
--------------------------------------------------------------------------*/

-------------------------------------------------------------------------------
-- PART 3) READ THE DEADLOCK GRAPH (Instructor)
-- Goal: Pull XML from ring_buffer and explain it.
-------------------------------------------------------------------------------
;WITH x AS
(
    SELECT CAST(t.target_data AS XML) AS target_xml
    FROM sys.dm_xe_sessions s
    JOIN sys.dm_xe_session_targets t
      ON s.address = t.event_session_address
    WHERE s.name = 'Deadlock_Monitor_CSC'
      AND t.target_name = 'ring_buffer'
),
d AS
(
    SELECT
        n.value('(event/@timestamp)[1]', 'datetime2(3)') AS [utc_timestamp],
        n.query('.') AS event_xml
    FROM x
    CROSS APPLY x.target_xml.nodes('//RingBufferTarget/event[@name="xml_deadlock_report"]') AS q(n)
)
SELECT TOP (5)
    [utc_timestamp],
    event_xml.query('(event/data/value/deadlock)[1]') AS deadlock_graph_xml
FROM d
ORDER BY [utc_timestamp] DESC;
GO

-- Stop session after demo (optional)
ALTER EVENT SESSION Deadlock_Monitor_CSC ON SERVER STATE = STOP;
GO