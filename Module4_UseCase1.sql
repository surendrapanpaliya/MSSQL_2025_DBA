/* Module4_UseCase1.sql


Module4_UseCase1 – Wait Stats, Blocking & Concurrency

Use Case 1: Waits vs Bottlenecks

Scenario (CSC Production Reality)

DBAs see high waits and immediately start tuning queries or indexes.
But without a baseline, they don’t know if waits are:
•	normal
•	new
•	regression-related

 
Step 1: Capture Wait Stats Baseline */


SELECT
    wait_type,
    wait_time_ms,
    signal_wait_time_ms,
    waiting_tasks_count
FROM sys.dm_os_wait_stats
WHERE wait_type NOT LIKE '%SLEEP%'
ORDER BY wait_time_ms DESC;


/*

🔍 Line-by-line explanation
•	sys.dm_os_wait_stats
→ Stores cumulative wait statistics since last restart
•	wait_type
→ What SQL Server was waiting on
•	wait_time_ms
→ Total time waited (resource + scheduler)
•	signal_wait_time_ms
→ Time waiting to get CPU (scheduler delay)
•	waiting_tasks_count
→ How many times this wait occurred
•	NOT LIKE '%SLEEP%'
→ Removes idle waits (noise)
 
📊 Output interpretation
•	High wait_time_ms ≠ problem by default
•	Look for:
o	Top 5 waits
o	Sudden spikes compared to baseline
•	Signal wait high → CPU pressure
 
✅ CSC DBA takeaway

Waits are symptoms, not root causes.
Always baseline before tuning.
 
🔹 Use Case 2: CXPACKET vs SOS_SCHEDULER_YIELD (CPU Bottleneck)

🎯 Scenario

CSC OLTP system shows:
•	High CPU
•	CXPACKET and_toggle confusion
•	DBAs think “parallelism is bad”
 
🧪 Step 1: Identify Active CPU Waits */


SELECT
    session_id,
    status,
    cpu_time,
    total_elapsed_time,
    wait_type,
    wait_time,
    last_wait_type
FROM sys.dm_exec_requests
WHERE session_id > 50
ORDER BY cpu_time DESC;


/*

Explanation
•	sys.dm_exec_requests
→ Shows currently executing requests
•	cpu_time
→ CPU consumed by request
•	wait_type
→ Current wait (if waiting)
•	last_wait_type
→ Last completed wait


 
📊 Output interpretation
Pattern	Meaning
CXPACKET + High CPU	Parallelism imbalance
SOS_SCHEDULER_YIELD	CPU pressure
High signal wait	Runnable queue congestion
 
✅ Correct conclusion
•	CXPACKET ≠ always bad
•	Tune MAXDOP & Cost Threshold, not disable parallelism
•	Investigate bad estimates
 
🔹 Use Case 3: PAGEIOLATCH_* (Disk I/O Bottleneck)

🎯 Scenario

Reports run slow during peak hours.

DBAs suspect queries but issue is storage latency.
 
🧪 Identify I/O waits

*/


SELECT
    wait_type,
    wait_time_ms / 1000.0 AS wait_seconds,
    waiting_tasks_count
FROM sys.dm_os_wait_stats
WHERE wait_type LIKE 'PAGEIOLATCH%'
ORDER BY wait_time_ms DESC;


/*

🔍 Explanation
•	PAGEIOLATCH_*
→ Waiting for data pages from disk
•	SH = shared read
•	EX = exclusive write
 
📊 Output interpretation
•	High PAGEIOLATCH + low CPU → I/O bottleneck
•	Tuning queries alone won’t fix storage latency
 
✅ CSC DBA takeaway

If SQL Server is waiting on disk, fix disk – not SQL.
 
🔹 Use Case 4: Blocking (NOT a Deadlock)

🎯 Scenario

CSC billing transactions hang.
One session blocks many others.
 
🧪 Identify blocking chains

*/

SELECT
    session_id,
    blocking_session_id,
    wait_type,
    wait_time,
    status
FROM sys.dm_exec_requests
WHERE blocking_session_id <> 0;

/*

🔍 Explanation
•	blocking_session_id
→ Session holding the lock
•	wait_type = LCK_M_*
→ Lock wait
 
🧪 Find the blocker

*/

SELECT
    session_id,
    status,
    command,
    cpu_time,
    reads
FROM sys.dm_exec_sessions
WHERE session_id = <blocking_session_id>;


/*
 
📊 Interpretation
•	Blocking ≠ deadlock
•	Blocker may be:
o	Long transaction
o	Missing index
o	SERIALIZABLE isolation
 
❌ Wrong fix
KILL <session_id>;
 
✅ Correct fix
•	Reduce transaction scope
•	Add index
•	Adjust isolation level
 
🔹 Use Case 5: Deadlock Detection & Analysis

🎯 Scenario

CSC logs show deadlocks at random times.
 
🧪 Capture deadlocks using Extended Events  */


CREATE EVENT SESSION DeadlockMonitor
ON SERVER
ADD EVENT sqlserver.xml_deadlock_report
ADD TARGET package0.event_file
(
    SET filename = 'C:\XE\deadlocks.xel'
);
GO


ALTER EVENT SESSION DeadlockMonitor ON SERVER STATE = START;

GO

/*

🔍 Explanation
•	xml_deadlock_report
→ Captures deadlock graph XML
•	event_file
→ Stores events for analysis
 
🧪 View deadlock graph (SSMS)
1.	Management → Extended Events
2.	Open .xel file
3.	Click Deadlock graph
 
📊 Deadlock graph interpretation
•	Nodes → sessions
•	Edges → lock dependencies
•	Victim → rolled back
 
✅ CSC DBA takeaway

Deadlocks are design problems, not random failures.
 
🔹 Use Case 6: Fix Blocking with RCSI (Row Versioning)

🎯 Scenario

Readers block writers during reporting queries.
 
🧪 Enable Read Committed Snapshot Isolation */


ALTER DATABASE CSC_PerfDemo
SET READ_COMMITTED_SNAPSHOT ON;
GO


/*

🔍 Explanation
•	Readers use row versions in TempDB
•	Writers don’t block readers
•	Improves concurrency dramatically
 
📊 Post-change behavior
•	Fewer LCK_M_S waits
•	Higher TempDB usage (expected)
•	Better throughput
 
⚠️ Trade-off
•	TempDB growth
•	Must size TempDB correctly
*/


