
/***************************************************************************************************
TempDB Pages Demo Script (Single Flow)
Goal:
  • creates #Overdue
  • inserts data
  • prints real tempdb name (with internal suffix)
  • lists page allocations (sys.allocation_units + DBCC IND)
  • automatically picks a data page and runs DBCC PAGE on it

Notes for DBAs / instructors:
  - Run this in ONE SSMS window/session (temp table is session-scoped).
  - DBCC PAGE output appears in the Messages tab.
  - Requires sufficient permissions (typically sysadmin).
  - Works best on on-prem SQL Server / Managed Instance. (Azure SQL DB restricts some DBCC.)
****************************************************************************************************/

USE CSC_DBA_Demo;
GO

SET NOCOUNT ON;
PRINT '=== TempDB Pages Demo starting ===';
GO

/*-----------------------------------------------------------------------------------------------
1) Create and populate the temp table
-----------------------------------------------------------------------------------------------*/
IF OBJECT_ID('tempdb..#Overdue') IS NOT NULL
    DROP TABLE #Overdue;

CREATE TABLE #Overdue
(
    entity_id INT NOT NULL,
    due_date  DATE NOT NULL
);

INSERT INTO #Overdue(entity_id, due_date)
SELECT TOP (5000) entity_id, due_date
FROM dbo.ComplianceFilings
WHERE filing_status = 'OVERDUE'
ORDER BY due_date;
-- Increase TOP() if you want to force more pages.
GO

PRINT 'Step 1 complete: #Overdue created + data inserted.';
GO

/*-----------------------------------------------------------------------------------------------
2) Print the REAL tempdb object name (with internal suffix)
-----------------------------------------------------------------------------------------------*/
DECLARE @RealName SYSNAME, @ObjectId INT;

SELECT TOP (1)
    @RealName = t.name,
    @ObjectId = t.object_id
FROM tempdb.sys.tables t
WHERE t.name LIKE '#Overdue%';

PRINT '--- Real tempdb object name (internal) ---';
PRINT CONCAT('Temp table logical name: #Overdue');
PRINT CONCAT('Temp table internal name: ', COALESCE(@RealName, '<not found>'));
PRINT CONCAT('tempdb.object_id: ', COALESCE(CONVERT(varchar(20), @ObjectId), '<null>'));
GO

/*-----------------------------------------------------------------------------------------------
3) Show page usage from allocation units (reserved/used/data pages)
-----------------------------------------------------------------------------------------------*/
PRINT '--- Allocation Units page usage (8KB pages) ---';

SELECT
    OBJECT_NAME(p.object_id, DB_ID('tempdb')) AS object_name,
    p.index_id,
    i.name AS index_name,
    au.type_desc AS allocation_type,
    au.total_pages,
    au.used_pages,
    au.data_pages,
    CAST(au.used_pages * 8.0 / 1024 AS DECIMAL(18,2)) AS used_mb
FROM tempdb.sys.partitions p
JOIN tempdb.sys.allocation_units au
  ON au.container_id = p.hobt_id
LEFT JOIN tempdb.sys.indexes i
  ON i.object_id = p.object_id AND i.index_id = p.index_id
WHERE p.object_id = OBJECT_ID('tempdb..#Overdue');
GO

/*-----------------------------------------------------------------------------------------------
4) List allocated pages using DBCC IND into a temp table
-----------------------------------------------------------------------------------------------*/
PRINT '--- DBCC IND output (page list) ---';

IF OBJECT_ID('tempdb..#Ind') IS NOT NULL
    DROP TABLE #Ind;

CREATE TABLE #Ind
(
    PageFID         INT NULL,
    PagePID         INT NULL,
    IAMFID          INT NULL,
    IAMPID          INT NULL,
    ObjectID        INT NULL,
    IndexID         INT NULL,
    PartitionNumber INT NULL,
    PartitionID     BIGINT NULL,
    iam_chain_type  NVARCHAR(30) NULL,
    PageType        INT NULL,
    IndexLevel      INT NULL,
    NextPageFID     INT NULL,
    NextPagePID     INT NULL,
    PrevPageFID     INT NULL,
    PrevPagePID     INT NULL
);

INSERT INTO #Ind
EXEC('DBCC IND (''tempdb'', ''#Overdue'', -1) WITH NO_INFOMSGS');

SELECT TOP (200)
    PageFID,
    PagePID,
    PageType,
    IndexID,
    IndexLevel
FROM #Ind
ORDER BY PageType, IndexID, IndexLevel, PageFID, PagePID;
GO

/*-----------------------------------------------------------------------------------------------
5) Automatically pick a DATA page (PageType=1) and run DBCC PAGE
-----------------------------------------------------------------------------------------------*/
DECLARE @FileId INT, @PageId INT;

SELECT TOP (1)
    @FileId = PageFID,
    @PageId = PagePID
FROM #Ind
WHERE PageType = 1   -- 1 = Data page
ORDER BY PageFID, PagePID;

PRINT '--- Selected DATA page for DBCC PAGE ---';
PRINT CONCAT('FileId=', COALESCE(CONVERT(varchar(10), @FileId), '<null>'),
             ', PageId=', COALESCE(CONVERT(varchar(20), @PageId), '<null>'));

IF @FileId IS NULL OR @PageId IS NULL
BEGIN
    PRINT 'No data page found. Increase TOP() in insert or insert more rows into #Overdue.';
END
ELSE
BEGIN
    -- Send DBCC output to Messages tab
    DBCC TRACEON(3604);
    PRINT '--- DBCC PAGE output (level 3) is in the Messages tab ---';
    DECLARE @cmd NVARCHAR(4000) =
        N'DBCC PAGE (''tempdb'', ' + CONVERT(varchar(10), @FileId) + N', ' + CONVERT(varchar(20), @PageId) + N', 3) WITH NO_INFOMSGS;';
    EXEC (@cmd);
END
GO

PRINT '=== TempDB Pages Demo complete ===';
GO
