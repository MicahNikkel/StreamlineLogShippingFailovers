/*

Copyright (C) 2013 Micah Nikkel

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation 
files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, 
modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the 
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE 
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR 
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR 
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

==========================================================================================================================

Streamline Log Shipping Failovers

--------------------------------------------------------------------------------------------------------------------------
 
AUTHOR: Micah Nikkel (MicahNikkel@Outlook.com - see LinkedIn profile for more information)

		- www.sqlservercentral.com/articles/Log+Shipping/104391
		- streamlinelogshippingfailovers.codeplex.com
		- www.codeproject.com/Tips/807845/Streamline-Log-Shipping-Failovers
	
DESCRIPTION: Dynamically generates a set of scripts that can be used to systematically fail over all Log-Shipped databases
		from the Primary to the Secondary (and back again if desired).  Scripts 01.sql - 05.sql fail over to the
		Secondary and 06.sql - 10.sql fail back to the Primary, carrying over any changes that may have been made on
		the Secondary.  If the Primary is unavailable, a variable can be changed to simply bring up the Secondary 
		after applying all available logs.

RELEASE DATE: 07/30/2015

FEEDBACK: To assist in making this solution as helpful as possible, please contact me with your thoughts, suggestions,
		and experiences.  I greatly appreciate hearing from you.


==========================================================================================================================
--> READ THIS SECTION BEFORE RUNNING THIS SCRIPT! <--
--------------------------------------------------------------------------------------------------------------------------
1. Ensure a Linked Server exists on the SECONDARY, pointing back to the PRIMARY, if the PRIMARY is available. This
	Linked Server MUST have the two RPC settings set to 'True' (look at the Properties of the Linked Server).
	Also ensure that the SQL user it maps to on the Primary has sufficient permissions (such as sysadmin). Then add
	yourself and any other users necessary to the Linked Server, mapping them to the SQL user you created on the Primary.
	This helps ensure only the mapped users can use the Linked Server and avoids running into issues during the failover 
	process due to permissions. Consider removing the Linked Server or downgrading the users' permissions after failing
	over and back.

2. Execute this script against the SECONDARY database server in the original Log Shipping configuration.

3. Execute this script only after updating the variables further below that have the words 'ACTION REQUIRED:' beside them.

4. This script creates 10 scripts that will be saved to the designated file location for later execution. These scripts  
   	can then be used to Fail over to the SECONDARY and later to Fail back to the PRIMARY.

5. Executing this script does NOT actually attempt to fail anything over. It simply generates a series of scripts that can
   	then be used to systematically fail over and back (if desired). Therefore, at any time feel free to execute this script
   	and analyze the resulting scripts it produces. It's best to familiarize yourself with them before you actually need them!

6. This process temporarily enables xp_cmdshell during its execution (if it's not already enabled).

7. Select 'Query -> Results To -> Results To Grid' up above in SQL Management Studio before proceeding.

8. At the end of this script is an Appendix that contains the following sections:
	
	- Thoughts Influencing The Development Of This Solution
	- Overview of Steps Performed By This Failover Script
	- Common Questions
		
		
==========================================================================================================================
Helpful Note: 

	After running this script against the SECONDARY, go to the location specified below in Windows Explorer to find
	01.sql through 10.sql.  Select the 10 scripts and hit Enter to open them in SQL Management Studio.  Use the  
	drop-down arrow to the right of the tabs to choose each script when you're ready to execute it.  

Recommendations:
	
	If you would like to increase the speed at which you absorb this script, please consider doing the following...
	"Reverse Engineer" the script by first running it against a test system to see the output it produces.
	Once you understand the scripts it has produced, you can then dive into the details of this Master script
	to see how it goes about generating them.

	Please see the Appendix at the end of this script for the 'Thoughts Influencing The Development Of This Solution',
	'Overview of Steps Performed', and 'Common Questions' sections for additional information.

--------------------------------------------------------------------------------------------------------------------------

*/

USE master
GO

DECLARE @PRIMARYDatabaseServer VARCHAR(100)
DECLARE @SECONDARYDatabaseServer VARCHAR(100)
DECLARE @PRIMARY_MSDB VARCHAR(250)
DECLARE @FailOverFromPRIMARY CHAR(1)
DECLARE @SysJobsOnPRIMARY VARCHAR(150)
DECLARE @SysJobsOnSECONDARY VARCHAR(150)
DECLARE @StoredXP_cmdshellValue CHAR(1)
DECLARE @FormattedDateTime VARCHAR(30)
DECLARE @ScriptToExport VARCHAR(8000)
DECLARE @Execute_ScriptToExport VARCHAR(500)
DECLARE @ScriptToRun VARCHAR(8000)
DECLARE @ScriptsLocation VARCHAR(500)
DECLARE @RunType CHAR(10)
DECLARE @SQL VARCHAR(8000)

SET @PRIMARYDatabaseServer = (SELECT TOP(1) PRIMARY_server FROM MSDB.dbo.log_shipping_monitor_SECONDARY)
SET @SECONDARYDatabaseServer = (SELECT TOP(1) SECONDARY_server FROM MSDB.dbo.log_shipping_monitor_SECONDARY)
SET @SysJobsOnPRIMARY = (SELECT '[' + @PRIMARYDatabaseServer + ']' + '.msdb.dbo.sysjobs')
SET @SysJobsOnSECONDARY = (SELECT '[' + @SECONDARYDatabaseServer + ']' + '.msdb.dbo.sysjobs')
SET @FormattedDateTime = (SELECT REPLACE((REPLACE(CONVERT(VARCHAR(26),GETDATE(),120),':','-')), ' ', '_'))

IF (SELECT @@SERVERNAME) = @SECONDARYDatabaseServer GOTO ServerConfirmed
ELSE RAISERROR('INCORRECT SERVER DETECTED: This Script Should Be Executed On The SECONDARY Server In The Log Shipping Configuration... Please Close This Script, Reopen, And Verify The Connection Is To The SECONDARY Server Before Executing...', 20, -1) with log
ServerConfirmed:


--========================================================================================================================================================================

-- ATTENTION: VARIABLES BELOW MUST BE PROPERLY UPDATED IN ORDER TO SUCCESSFULLY EXECUTE THIS SCRIPT

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

----> ACTION REQUIRED: <---- 
--		Ensure the variable below is set to either 'Y' or 'N'. This will determine whether or not a backup 
--		of the log is attempted against the PRIMARY, jobs are stopped on the PRIMARY, etc.

SET @FailOverFromPRIMARY = '-' -- Valid entries are 'Y' or 'N'.  Please replace the dash with one of these values.


----> ACTION REQUIRED: <---- 
--		Ensure the variable below is set to the desired network folder location for output of the .SQL scripts to be used during the Failover.  
--		Ideally, it is also where this overall DR script is being run from so all files being used are stored together.

SET @ScriptsLocation = '-' -- Update dash to desired network location for generated scripts.  For example, '\\Server\FolderName'


----> ACTION REQUIRED: <---- 
--		Ensure the variable below is set to either 'Automatic' or 'Manual'. This will determine whether or not the Restore statements will be
--		automatically applied to the SECONDARY when failing over to it.  The statements it will run are displayed regardless, but this variable 
--		controls whether they run automatically (with helpful output) or you need to copy/paste them into a new window and run them from there. 

SET @RunType = 'Automatic'  -- Valid entries are 'Automatic' (default) or 'Manual'.  Please replace the entry if needed.


--========================================================================================================================================================================
-- After the variables above have been properly set, proceed in executing this script in order to generate the scripts needed for the  rest of the Failover process...
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Confirm Entered Values Are Valid:

IF @FailOverFromPRIMARY NOT IN ('Y', 'N') RAISERROR('The variable @FailOverFromPRIMARY has not been updated to reflect whether an attempt should be made to back up the PRIMARY before failing over to the SECONDARY. Please close and reopen this file, update this variable, reconnect, and run this script again.', 20, -1) WITH LOG
IF @ScriptsLocation LIKE '-' RAISERROR('The variable @ScriptsLocation has not been updated to reflect where the FailOver scripts should be created.  Please close and reopen this file, update this variable, reconnect, and run this script again.', 20, -1) WITH LOG
IF @RunType NOT IN ('Automatic', 'Manual') RAISERROR('The variable @RunType has not been updated to reflect whether Restore statements to be run on the SECONDARY will be either Manuaal or Automatic', 20, -1) WITH LOG --16, -1) WITH LOG

IF (select count(*) from sys.servers where name = @PRIMARYDatabaseServer and is_linked = 1) = 0 RAISERROR('A linked server must be created/configured that points back to the PRIMARY server.  Please close and reopen this file after creating the linked server, reconnect, and then run this script again.', 20, -1) WITH LOG

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT 'Review the values of the variables below.  if necessary, make corrections to the Streamline_LS_Failovers.sql script and rerun to create updated Failover scripts...' AS 'Begin Script Generation...																					'
SELECT 'Attempt to fail over from the PRIMARY server:				' AS '									', @FailOverFromPRIMARY AS ' '
SELECT 'Specified folder location to which .SQL scripts are being written:	' AS '									', @ScriptsLocation AS '															'
SELECT 'Execute resulting scripts in Automatic or Manual mode:		' AS '									', @RunType AS '	'

SET @StoredXP_cmdshellValue = ((SELECT CONVERT(CHAR(1), ISNULL(value, value_in_use)) AS config_value FROM sys.configurations WHERE name = 'xp_cmdshell'))

EXEC sp_configure 'show advanced options', 1
RECONFIGURE
EXEC sp_configure 'xp_cmdshell', 1
RECONFIGURE

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_SysJobNames'))
	DROP TABLE Tempdb.dbo.LS_SysJobNames

CREATE TABLE Tempdb.dbo.LS_SysJobNames
(
	Name VARCHAR(500),
	ServerRole VARCHAR(10)
)

IF @FailOverFromPRIMARY = 'N'
	INSERT Tempdb.dbo.LS_SysJobNames EXEC('SELECT ' + '''<...PRIMARY UNAVAILABLE...>''' + ', ' + '''SECONDARY''')
ELSE IF @FailOverFromPRIMARY = 'Y'
	INSERT Tempdb.dbo.LS_SysJobNames
	EXEC('SELECT name, ' + '''PRIMARY''' + ' FROM ' + @SysJobsOnPRIMARY + ' WHERE category_id <> 0 AND name LIKE ' + '''LSBackup%''' + ' AND enabled = 1 ORDER BY name ASC')
	
IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_DatabaseInfo_SECONDARY'))
	DROP TABLE Tempdb.dbo.LS_DatabaseInfo_SECONDARY

CREATE TABLE Tempdb.dbo.LS_DatabaseInfo_SECONDARY
(
	DatabaseName VARCHAR(200) NULL,
	TranLogBackupPath VARCHAR(750),
	LastTranFileApplied VARCHAR(250),
	TranNameForFailBack VARCHAR(300)
)

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_DatabaseInfo_PRIMARY'))
	DROP TABLE Tempdb.dbo.LS_DatabaseInfo_PRIMARY

CREATE TABLE Tempdb.dbo.LS_DatabaseInfo_PRIMARY
(
	DatabaseName VARCHAR(200) NULL, 
	TranLogBackupPath VARCHAR(750),
	LastTranFileBackedUp VARCHAR(250),
	TranNameForFailOver VARCHAR(300)
)

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_Step01'))
	DROP TABLE Tempdb.dbo.LS_Step01

CREATE TABLE Tempdb.dbo.LS_Step01
(
	ID INT NOT NULL IDENTITY (1,1) PRIMARY KEY CLUSTERED,
	SQLToRun 	VARCHAR(8000)
)

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_Step02'))
	DROP TABLE Tempdb.dbo.LS_Step02

CREATE TABLE Tempdb.dbo.LS_Step02
(
	ID INT NOT NULL IDENTITY (1,1) PRIMARY KEY CLUSTERED,
	SQLToRun 	VARCHAR(8000)
)

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_Step03'))
	DROP TABLE Tempdb.dbo.LS_Step03

CREATE TABLE Tempdb.dbo.LS_Step03
(
	ID INT NOT NULL IDENTITY (1,1) PRIMARY KEY CLUSTERED,
	SQLToRun 	VARCHAR(8000)
)

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_Step04'))
	DROP TABLE Tempdb.dbo.LS_Step04

CREATE TABLE Tempdb.dbo.LS_Step04
(
	ID INT NOT NULL IDENTITY (1,1) PRIMARY KEY CLUSTERED,
	SQLToRun 	VARCHAR(8000)
)

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_Step05'))
	DROP TABLE Tempdb.dbo.LS_Step05

CREATE TABLE Tempdb.dbo.LS_Step05
(
	ID INT NOT NULL IDENTITY (1,1) PRIMARY KEY CLUSTERED,
	SQLToRun 	VARCHAR(8000)
)

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_Step06'))
	DROP TABLE Tempdb.dbo.LS_Step06

CREATE TABLE Tempdb.dbo.LS_Step06
(
	ID INT NOT NULL IDENTITY (1,1) PRIMARY KEY CLUSTERED,
	SQLToRun 	VARCHAR(8000)
)

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_Step07'))
	DROP TABLE Tempdb.dbo.LS_Step07

CREATE TABLE Tempdb.dbo.LS_Step07
(
	ID INT NOT NULL IDENTITY (1,1) PRIMARY KEY CLUSTERED,
	SQLToRun 	VARCHAR(8000)
)

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_Step08'))
	DROP TABLE Tempdb.dbo.LS_Step08

CREATE TABLE Tempdb.dbo.LS_Step08
(
	ID INT NOT NULL IDENTITY (1,1) PRIMARY KEY CLUSTERED,
	SQLToRun 	VARCHAR(8000)
)

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_Step09'))
	DROP TABLE Tempdb.dbo.LS_Step09

CREATE TABLE Tempdb.dbo.LS_Step09
(
	ID INT NOT NULL IDENTITY (1,1) PRIMARY KEY CLUSTERED,
	SQLToRun 	VARCHAR(8000)
)

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_Step10'))
	DROP TABLE Tempdb.dbo.LS_Step10

CREATE TABLE Tempdb.dbo.LS_Step10
(
	ID INT NOT NULL IDENTITY (1,1) PRIMARY KEY CLUSTERED,
	SQLToRun 	VARCHAR(8000)
)

SELECT @SQL = ''


--========================================================================================================================================================================

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--								-----> STEP 01 of 10 <-----													' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--PRIMARY Server In Log Shipping Configuration:   ' + @PRIMARYDatabaseServer + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--SECONDARY Server In Log Shipping Configuration: ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

IF @FailOverFromPRIMARY = 'N' SELECT @SQL = @SQL +  '/*' + CHAR(13) + CHAR(10) + '!! A full failover from the PRIMARY was NOT chosen.  Therefore, this script is NOT used in the Failover.' + CHAR(13) + CHAR(10) + '!! Please disregard this script and proceed to the next one in the process.' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '--These scripts are designed to facilitate the failover of ALL LS user databases to their SECONDARY server.' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--	DO NOT RUN THIS OR ANY FURTHER COMMANDS UNTIL READY TO FAIL OVER TO ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' +CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '--PLEASE NOTE: Run each script on the appropriate database server, as noted in the top of each script.' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 

SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--' + @PRIMARYDatabaseServer + ': EXECUTE this script against ' + @PRIMARYDatabaseServer + ' - Disables all Log Shipping jobs and notes them for enabling later.' + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'IF (SELECT @@SERVERNAME) = ''' + @PRIMARYDatabaseServer + ''' GOTO ServerConfirmed' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RAISERROR(' + '''INCORRECT SERVER SPECIFIED: Please Close This Script, Reopen, And Verify Which Server To Connect To Before Executing...''' +', 20, -1) with log' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ServerConfirmed:' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step01
	SELECT @SQL

INSERT Tempdb.dbo.LS_Step01
	SELECT 'EXEC msdb.dbo.sp_start_job @job_name = ''' + name + '''; ' + CHAR(13) + CHAR(10)
	FROM Tempdb.dbo.LS_SysJobNames
	WHERE name LIKE 'LSBackup%' AND ServerRole = 'PRIMARY'
	ORDER BY name ASC;

INSERT Tempdb.dbo.LS_Step01
	SELECT 'DECLARE @ActiveJobCount SMALLINT' + CHAR(13) + CHAR(10)
	+ 'SET @ActiveJobCount = 1' + CHAR(13) + CHAR(10)
	+ 'WHILE @ActiveJobCount > 0 BEGIN ' + CHAR(13) + CHAR(10) 
	+ 'SET @ActiveJobCount = (SELECT COUNT(*) FROM msdb.dbo.sysjobactivity sja INNER JOIN msdb.dbo.sysjobs sj on sja.job_id = sj.job_id WHERE sj.name LIKE ' + '''LSBackup%''' + ' AND sj.enabled = 1 AND start_execution_date IS NOT NULL AND stop_execution_date IS NULL AND DATEDIFF(HOUR, start_execution_date, GETDATE()) <= 24)' + CHAR(13) + CHAR(10)
	+ 'END' + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step01
	SELECT 'EXEC msdb.dbo.sp_update_job @job_name = ''' + name + ''', @enabled = 0;' + CHAR(13) + CHAR(10)
	FROM Tempdb.dbo.LS_SysJobNames
	WHERE ServerRole = 'PRIMARY'
	ORDER BY name ASC;

INSERT Tempdb.dbo.LS_Step01
	SELECT CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + '--		*** End of Script ***'

IF @FailOverFromPRIMARY = 'N'
	INSERT Tempdb.dbo.LS_Step01 
		SELECT CHAR(13) + CHAR(10) + '*/' 

IF EXISTS (SELECT * FROM TempDB.dbo.sysobjects o WHERE o.xtype IN ('U') AND o.id = object_id('Tempdb.dbo.LS_RedirectOutput'))
	DROP TABLE Tempdb.dbo.LS_RedirectOutput

CREATE TABLE Tempdb.dbo.LS_RedirectOutput
(
	cmdoutput VARCHAR(4000)
)

SET @ScriptToRun = 'MKDIR ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime
INSERT Tempdb.dbo.LS_RedirectOutput
	EXEC master..xp_cmdshell @ScriptToRun

SET @Execute_ScriptToExport = 'BCP ' + QUOTENAME('SELECT SQLToRun FROM Tempdb.dbo.LS_Step01', '"') + ' QUERYOUT ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime + '\01.sql -T -c'

INSERT Tempdb.dbo.LS_RedirectOutput
	EXEC master..xp_cmdshell @Execute_ScriptToExport

SELECT @SQL = ''


--========================================================================================================================================================================

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--								-----> STEP 02 of 10 <-----													' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--PRIMARY Server In Log Shipping Configuration:   ' + @PRIMARYDatabaseServer + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--SECONDARY Server In Log Shipping Configuration: ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--' + @SECONDARYDatabaseServer + ': EXECUTE this script against ' + @SECONDARYDatabaseServer + ' - Executes all LOG RESTORE jobs one last time and then disables them.' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'IF (SELECT @@SERVERNAME) = ''' + @SECONDARYDatabaseServer + ''' GOTO ServerConfirmed' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RAISERROR(' + '''INCORRECT SERVER SPECIFIED: Please Close This Script, Reopen, And Verify Which Server To Connect To Before Executing...''' +', 20, -1) with log' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ServerConfirmed:' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)


INSERT Tempdb.dbo.LS_Step02
	SELECT @SQL

SET @SQL = ''

DECLARE @RestoreDelayForDatabases TABLE
(
	SecondaryDatabase VARCHAR(150),
	RestoreDelay VARCHAR(5)
)
INSERT @RestoreDelayForDatabases
	SELECT secondary_database, restore_delay FROM msdb.dbo.log_shipping_secondary_databases

INSERT Tempdb.dbo.LS_Step02 
	SELECT 'UPDATE msdb.dbo.log_shipping_secondary_databases' + CHAR(13) + CHAR(10) 
	+ '	SET restore_delay = 0' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_SysJobNames
	EXEC('SELECT name, ' + '''SECONDARY''' + 'FROM ' + @SysJobsOnSECONDARY + ' WHERE (category_id <> 0 AND name LIKE ' + '''LSCopy%''' + ' OR name LIKE ' + '''LSRestore%''' + ') AND enabled = 1 ORDER BY name ASC')

INSERT Tempdb.dbo.LS_Step02
	SELECT 'EXEC msdb.dbo.sp_start_job @job_name = ''' + name + ''';' + CHAR(13) + CHAR(10)
	FROM Tempdb.dbo.LS_SysJobNames
	WHERE Name LIKE 'LSCopy%' AND ServerRole = 'SECONDARY'
	ORDER BY name ASC;

INSERT Tempdb.dbo.LS_Step02
	SELECT 'DECLARE @ActiveJobCount SMALLINT' + CHAR(13) + CHAR(10)
	+ 'SET @ActiveJobCount = 1' + CHAR(13) + CHAR(10)
	+ 'WHILE @ActiveJobCount > 0 BEGIN ' + CHAR(13) + CHAR(10) 
	+ 'SET @ActiveJobCount = (SELECT COUNT(*) FROM msdb.dbo.sysjobactivity sja INNER JOIN msdb.dbo.sysjobs sj on sja.job_id = sj.job_id WHERE sj.name LIKE ' + '''LSCopy%''' + ' AND sj.enabled = 1 AND start_execution_date IS NOT NULL AND stop_execution_date IS NULL AND DATEDIFF(HOUR, start_execution_date, GETDATE()) <= 24)' + CHAR(13) + CHAR(10)
	+ 'END' + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step02
	SELECT 'EXEC msdb.dbo.sp_start_job @job_name = ''' + name + ''';' + CHAR(13) + CHAR(10)
	FROM Tempdb.dbo.LS_SysJobNames
	WHERE Name LIKE 'LSRestore%' AND ServerRole = 'SECONDARY'
	ORDER BY name ASC;

INSERT Tempdb.dbo.LS_Step02
	SELECT 'SET @ActiveJobCount = 1' + CHAR(13) + CHAR(10)
	+ 'WHILE @ActiveJobCount > 0 BEGIN ' + CHAR(13) + CHAR(10) 
	+ 'SET @ActiveJobCount = (SELECT COUNT(*) FROM msdb.dbo.sysjobactivity sja INNER JOIN msdb.dbo.sysjobs sj on sja.job_id = sj.job_id WHERE sj.name LIKE ' + '''LSRestore%''' + ' AND sj.enabled = 1 AND start_execution_date IS NOT NULL AND stop_execution_date IS NULL AND DATEDIFF(HOUR, start_execution_date, GETDATE()) <= 24)' + CHAR(13) + CHAR(10)
	+ 'END' + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step02
	SELECT 'EXEC msdb.dbo.sp_update_job @job_name = ''' + name + N''', @enabled = 0;' + CHAR(13) + CHAR(10)
	FROM Tempdb.dbo.LS_SysJobNames
	WHERE name LIKE 'LSCopy%' OR name LIKE 'LSRestore%' AND ServerRole = 'SECONDARY'
	ORDER BY name;

SELECT @SQL = @SQL + '--		*** End of Script ***'

INSERT Tempdb.dbo.LS_Step02
	SELECT @SQL

SET @Execute_ScriptToExport = 'BCP ' + QUOTENAME('SELECT SQLToRun FROM Tempdb.dbo.LS_Step02', '"') + ' QUERYOUT ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime + '\02.sql -T -c'

INSERT Tempdb.dbo.LS_RedirectOutput
	EXEC master..xp_cmdshell @Execute_ScriptToExport

SELECT @SQL = ''


--========================================================================================================================================================================

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--								-----> STEP 03 of 10 <-----													' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--PRIMARY Server In Log Shipping Configuration:   ' + @PRIMARYDatabaseServer + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--SECONDARY Server In Log Shipping Configuration: ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

IF @FailOverFromPRIMARY = 'N' SELECT @SQL = @SQL +  '/*' + CHAR(13) + CHAR(10) + '!! A full failover from the PRIMARY was NOT chosen.  Therefore, this script is NOT used in the Failover.' + CHAR(13) + CHAR(10) + '!! Please disregard this script and proceed to the next one in the process.' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--' + @SECONDARYDatabaseServer + ': EXECUTE this script against ' + @SECONDARYDatabaseServer + ' - Remotely performs last log backup and leaves databases in NORECOVERY mode...' + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--This script kills any other active connections, performs a final log backup, and puts each database into NORECOVERY mode.  This ensures no one can' + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--connect to databases on ' + @PRIMARYDatabaseServer + ' and that they are prepared for us to fail back to when we are ready.' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'IF (SELECT @@SERVERNAME) = ''' + @SECONDARYDatabaseServer + ''' GOTO ServerConfirmed' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RAISERROR(' + '''INCORRECT SERVER SPECIFIED: Please Close This Script, Reopen, And Verify Which Server To Connect To Before Executing...''' +', 20, -1) with log' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ServerConfirmed:' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @cmdQueue TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		cmd VARCHAR(5000) NULL' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @RunCommand TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		cmd varchar(5000)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @SQLToExecute VARCHAR(5000)
SET @SQLToExecute = ' + CHAR(13) + CHAR(10) 
		+ '''SELECT  primary_database ,
        LEFT(last_backup_file, ( LengthOfFullPath - LengthOfFileName ) - 1) AS ''' + ' + ' + '''''''TranLogBackupPath''''''' + ' + ' + ''', ''' + ' + ' +  
        '''LastTranFileBackedUp ,
        TranNameForFailover
FROM    ( SELECT    primary_database ,
                    last_backup_file ,
                    LEN(last_backup_file) AS ''' + ' + ' + '''''''LengthOfFullPath''''''' + ' + ' + ''', ''' + ' + ' + 
                    '''LEN(RIGHT(last_backup_file,
                              CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', REVERSE(last_backup_file), 1) - 1)) AS ''' + ' + ' + '''''''LengthOfFileName''''''' + ' + ' + ''', ''' + ' + ' + 
                    '''RIGHT(last_backup_file,
                          CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', REVERSE(last_backup_file), 1) - 1) AS ''' + ' + ' + '''''''LastTranFileBackedUp''''''' + ' + ' + ''', ' +
                    'primary_database + ''' + ' + ' + '''''''' + '_' + '''''''' + ' + ' + '''
                    + CAST(CAST(RIGHT(LEFT(RIGHT(last_backup_file,
                                                 CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', ''' + ' + ' + '''
                                                           REVERSE(last_backup_file),
                                                           1) - 1),
                                           LEN(RIGHT(last_backup_file,
                                                 CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', ''' + ' + ' + 
                                                              '''REVERSE(last_backup_file),
                                                              1) - 1)) - 4),
                                      	   LEN(LEFT(RIGHT(last_backup_file,
                                                 CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', ''' + ' + ' + 
                                                              '''REVERSE(last_backup_file),
                                                              1) - 1),
                                           LEN(RIGHT(last_backup_file,
                                                 CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', ''' + ' + ' + 
                                                              '''REVERSE(last_backup_file),
                                                              1) - 1)) - 4))
                                      - LEN(primary_database) - 1) AS BIGINT)
                    + 1 AS VARCHAR(1000)) + ''' + ' + ' + '''''''.trn''''''' + ' + ' + ''' AS ''' + ' + ' + '''''''TranNameForFailover''''''' + ' + ' + 
          ''' FROM ''' + ' + ''' + '[' + @PRIMARYDatabaseServer + ']' + '.msdb.dbo.log_shipping_monitor_primary ''' + ' + ' + ''')''' + ' + ' + ''' AS A ''' + ' ' + CHAR(13) + CHAR(10)  + CHAR(13) + CHAR(10) +
		 'INSERT Tempdb.[dbo].[LS_DatabaseInfo_PRIMARY] EXEC(@SQLToExecute)' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @whilecounter AS INT' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @i AS INT' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @ScriptToRun varchar(250)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @DBName VARCHAR(250)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @StatementToRun VARCHAR(5000)' + CHAR(13) + CHAR(10) 

SELECT @SQL = @SQL +  'INSERT @cmdQueue' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SELECT ALTERStatement' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'FROM' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SELECT ' + '''ALTER DATABASE [''' + ' + DatabaseName + ' + '''] SET SINGLE_USER with ROLLBACK IMMEDIATE;''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '+ ' + '''ALTER DATABASE [''' + ' + DatabaseName + ' + '''] SET MULTI_USER; ''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '+ ' + '''BACKUP LOG [''' + ' + DatabaseName + ' + '''] TO DISK = ''''''' + ' + TranLogBackupPath + ' + '''\''' + ' + TranNameForFailover + ''' + ''''' WITH NORECOVERY;''' + ' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) AS ' + '''ALTERStatement''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'FROM Tempdb.dbo.LS_DatabaseInfo_PRIMARY' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ') AS Statements' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ORDER BY ALTERStatement ASC' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'SELECT ' + '''Automatically Executing The Statement(s) Below.  Please wait...''' + ' AS ' + '''									''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SELECT cmd AS ' + '''Statement(s) To Execute:							''' + ' FROM @cmdQueue' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'SET @i = 1' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SET @whilecounter = (SELECT COUNT(*) FROM @cmdQueue)' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'WHILE @i <= @whilecounter' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'BEGIN' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DELETE TOP(1) FROM @cmdQueue' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		OUTPUT deleted.cmd into @RunCommand' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SET @StatementToRun = (select TOP(1) cmd from @RunCommand)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SELECT ' + '''Executing Statement Against ' + @PRIMARYDatabaseServer + ': ''' + ' + ' + ' @StatementToRun AS ' + '''                    							''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		EXEC (@StatementToRun) AT ' + '[' + @PRIMARYDatabaseServer + ']' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DELETE FROM @RunCommand' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SET @i = @i + 1' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'END' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step03
	SELECT @SQL



INSERT Tempdb.dbo.LS_Step03
	SELECT 'EXEC msdb.dbo.sp_start_job @job_name = ''' + name + ''';' + CHAR(13) + CHAR(10)
	FROM Tempdb.dbo.LS_SysJobNames
	WHERE Name LIKE 'LSCopy%' AND ServerRole = 'SECONDARY'
	ORDER BY name ASC;

INSERT Tempdb.dbo.LS_Step03
	SELECT 'DECLARE @ActiveJobCount SMALLINT' + CHAR(13) + CHAR(10)
	+ 'SET @ActiveJobCount = 1' + CHAR(13) + CHAR(10)
	+ 'WHILE @ActiveJobCount > 0 BEGIN ' + CHAR(13) + CHAR(10) 
	+ 'SET @ActiveJobCount = (SELECT COUNT(*) FROM msdb.dbo.sysjobactivity sja INNER JOIN msdb.dbo.sysjobs sj on sja.job_id = sj.job_id WHERE sj.name LIKE ' + '''LSCopy%''' + ' AND sj.enabled = 1 AND start_execution_date IS NOT NULL AND stop_execution_date IS NULL AND DATEDIFF(HOUR, start_execution_date, GETDATE()) <= 24)' + CHAR(13) + CHAR(10)
	+ 'END' + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step03
	SELECT '--		*** End of Script ***'

SET @Execute_ScriptToExport = 'BCP ' + QUOTENAME('SELECT SQLToRun FROM Tempdb.dbo.LS_Step03', '"') + ' QUERYOUT ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime + '\03.sql -T -c'

INSERT Tempdb.dbo.LS_RedirectOutput
	EXEC master..xp_cmdshell @Execute_ScriptToExport

SELECT @SQL = ''


--========================================================================================================================================================================

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--								-----> STEP 04 of 10 <-----													' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)  
SELECT @SQL = @SQL +  '--PRIMARY Server In Log Shipping Configuration:   ' + @PRIMARYDatabaseServer + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--SECONDARY Server In Log Shipping Configuration: ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--' + @SECONDARYDatabaseServer + ': EXECUTE this script against ' + @SECONDARYDatabaseServer + ' - Restores any remaining transactions on the SECONDARY databases.' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'IF (SELECT @@SERVERNAME) = ''' + @SECONDARYDatabaseServer + ''' GOTO ServerConfirmed' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RAISERROR(' + '''INCORRECT SERVER SPECIFIED: Please Close This Script, Reopen, And Verify Which Server To Connect To Before Executing...''' +', 20, -1) with log' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ServerConfirmed:' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @StoredXP_cmdshellValue CHAR(1)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SET @StoredXP_cmdshellValue = ((SELECT CONVERT(CHAR(1), ISNULL(value, value_in_use)) AS config_value FROM sys.configurations WHERE name = ''' + 'xp_cmdshell' + '''))' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'EXEC sp_configure ' + '''show advanced options''' + ', 1' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RECONFIGURE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'EXEC sp_configure '+ '''xp_cmdshell''' + ', @StoredXP_cmdshellValue' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RECONFIGURE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'EXEC sp_configure ' + '''show advanced options''' + ', 0' + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  'RECONFIGURE' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @cmdQueue TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DBName VARCHAR(200) NULL,' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		BackupDirectory VARCHAR(200) NULL,' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		cmd VARCHAR(500) NULL' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @cmd TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DBName VARCHAR(500) NULL,' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		BackupDirectory VARCHAR(1000) NULL,' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		cmd VARCHAR(500) NULL' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @sysContent TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DBName VARCHAR(500),' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		LastLogFileApplied VARCHAR(250) NULL' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @FileList TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		LogFileOrder INT NOT NULL IDENTITY (1,1) PRIMARY KEY CLUSTERED,' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DBName VARCHAR(500),' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		LogFileName SYSNAME NULL' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @Dir TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		Dir VARCHAR(200) NULL' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @RunCommand TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DBName varchar(500),' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		cmd varchar(1000)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'INSERT INTO @sysContent' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SELECT SECONDARY_database, last_restored_file FROM msdb.dbo.log_shipping_SECONDARY_databases WHERE last_restored_file IS NOT NULL' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'INSERT INTO @cmdQueue' + CHAR(13) + CHAR(10) 

SELECT @SQL = @SQL +  '		SELECT secondary_database AS ' + '''DBName''' + ', LEFT(last_restored_file, (LengthOfFullPath - LengthOfFileName)-1) AS ' + '''FolderPath''' + ',' + '''DIR ''' + ' + ' + 'LEFT(last_restored_file, (LengthOfFullPath - LengthOfFileName)-1)' + ' + '   + '''\''' + ' + ' + 'secondary_database' + ' + ' + '''*.trn''' + ' + ' + ''' /A-S-D /B /ODN /TW''' + ' AS ' + '''CMD''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		FROM (select secondary_database, last_restored_file, LEN(last_restored_file) as ' + '''LengthOfFullPath''' + ', RIGHT(last_restored_file, CHARINDEX(' + '''\''' + ', REVERSE(last_restored_file), 1)-1) AS ' + '''TranFileName''' + ', LEN(RIGHT(last_restored_file, CHARINDEX(' + '''\''' + ', REVERSE(last_restored_file), 1)-1)) as ' + '''LengthOfFileName''' + 'from msdb.dbo.log_shipping_monitor_secondary) AS A' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'INSERT INTO @cmd' + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '		SELECT * FROM @cmdQueue' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN (' + '''U''' + ') AND o.id = object_id(' + '''Tempdb.dbo.LS_RestoreStatements''' + '))' + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '		DROP TABLE Tempdb.dbo.LS_RestoreStatements'  + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'IF EXISTS (SELECT * FROM Tempdb.dbo.sysobjects o WHERE o.xtype IN (' + '''U''' + ') AND o.id = object_id(' + '''Tempdb.dbo.LS_RestoreStatementsKEEP''' + '))' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DROP TABLE Tempdb.dbo.LS_RestoreStatementsKEEP'  + CHAR(13) + CHAR(10)  + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'INSERT Tempdb.dbo.LS_Step04' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SELECT ' + '''EXEC msdb.dbo.sp_start_job @job_name = ''' + ' + ' + 'name' + ' + ' + ''';''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'FROM Tempdb.dbo.LS_SysJobNames'  + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'WHERE Name LIKE ' + '''LSCopy%''' + ' AND ServerRole = ' + '''SECONDARY''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ORDER BY name ASC;' + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @whilecounter AS INT' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @i AS INT' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @ScriptToRun varchar(1000)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @DBName VARCHAR(500)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SET @i = 1' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SET @whilecounter = (SELECT COUNT(*) FROM @cmdQueue)' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'WHILE @i <= @whilecounter' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'BEGIN' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DELETE TOP(1) FROM @cmdQueue' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		OUTPUT deleted.DBName, deleted.cmd INTO @RunCommand' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SET @ScriptToRun = (SELECT cmd FROM @RunCommand)'  + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		INSERT @FileList (LogFileName)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '			EXEC master..xp_cmdshell @ScriptToRun' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DELETE FROM @FileList WHERE LogFileName IS NULL OR LogFileName NOT LIKE ' + '''%.trn%''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SET @DBName = (select DBName from @RunCommand)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		UPDATE @FileList set DBName = @DBName where DBName IS NULL' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DELETE FROM @RunCommand' + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '		SET @i = @i + 1' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'END' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @StatementToRun VARCHAR(1000)' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @RestoreLogToSECONDARY TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		RestoreLogStatement VARCHAR(1000) NULL' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DECLARE @RestoreLogToSECONDARYQueue table' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DBName VARCHAR(500),' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		LastLogFileApplied VARCHAR(1000),' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		LastLogFileToBeApplied VARCHAR(1000),' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		CandidateLogsForApplying VARCHAR(1000),' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		RestoreStatements VARCHAR(1000) NULL' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  ';WITH A AS' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)																																																												
SELECT @SQL = @SQL +  '		SELECT FL.LogFileOrder, FL.DBName, LastLogFileApplied, RIGHT(LastLogFileApplied, CHARINDEX(' + '''\''' + ', REVERSE(LastLogFileApplied), 1)-1) AS ' + '''LastLogFileToBeApplied''' + ', ' + 'FL.LogFileName AS ' + '''CandidateLogsForApplying''' + ', ' + ''' RESTORE LOG [''' + ' + FL.DBName + ' + '''] FROM DISK = ''''''' + ' + BackupDirectory + ' + '''\''' + ' + ' + 'FL.LogFileName' + ' + ' + ''''''' WITH NORECOVERY ''' + ' AS ' + '''RestoreStatements''' + ', ' + 'BackupDirectory, FL.LogFileName, MINLog, MaxLog' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		FROM @FileList FL' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		LEFT OUTER JOIN @sysContent sC ON FL.DBName = sC.DBName' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		LEFT OUTER JOIN (SELECT DBName, MIN(LogFileOrder) AS MINLog, Max(LogFileOrder) AS MaxLog FROM @FileList GROUP BY DBName) AS LogFileNum ON FL.DBName = LogFileNum.DBName'  + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		LEFT OUTER JOIN @cmd CMD ON FL.DBName = CMD.DBName' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '), B AS' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SELECT DBName, LogFileOrder AS ' + '''LogFileOrderWhereEqual''' + ' FROM A WHERE LastLogFileToBeApplied = CandidateLogsForApplying' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '), C AS' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SELECT A.DBName, A.LogFileOrder, LogFileOrderWhereEqual, MinLog, MaxLog, LastLogFileToBeApplied, CandidateLogsForApplying, RestoreStatements FROM A INNER JOIN B ON A.DBName = B.DBName' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SELECT LogFileOrder, DBName, RestoreStatements into Tempdb.dbo.LS_RestoreStatements FROM C where LogFileOrder > LogFileOrderWhereEqual and LogFileOrder <= MaxLog ORDER BY C.LogFileOrder ASC' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

IF @RunType = 'Manual'
	SELECT @SQL = @SQL +  'SELECT DBName, RestoreStatements AS ' + '''Copy, Save, & Run The Statements Below (In A New Query) Against ' + @SECONDARYDatabaseServer + ' To Complete Step 4.''' + 'FROM Tempdb.dbo.LS_RestoreStatements ORDER BY LogFileOrder ASC' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
ELSE IF @RunType = 'Automatic'
BEGIN
	SELECT @SQL = @SQL +  'SELECT * INTO Tempdb.dbo.LS_RestoreStatementsKEEP' +  CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  'FROM Tempdb.dbo.LS_RestoreStatements ORDER BY LogFileOrder ASC' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  'SELECT ' + '''Automatically Executing The Statements Below.  Please wait...''' + ' AS ' + '''										''' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  'SELECT RestoreStatements FROM Tempdb.dbo.LS_RestoreStatements ORDER BY LogFileOrder ASC' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

	SELECT @SQL = @SQL +  'SET @i = 1' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  'SET @whilecounter = (SELECT COUNT(*) FROM Tempdb.dbo.LS_RestoreStatements)' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

	SELECT @SQL = @SQL +  'WHILE @i <= @whilecounter' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  'BEGIN' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  '		DELETE TOP(1) FROM Tempdb.dbo.LS_RestoreStatements' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  '		OUTPUT deleted.DBName, deleted.RestoreStatements into @RunCommand' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  '		SET @StatementToRun = (select TOP(1) cmd from @RunCommand)' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  '		USE master' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  '		SELECT ''' + 'Executing Statement: ''' + ' + ' + '@StatementToRun AS ' + '''										''' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  '		EXEC (@StatementToRun)' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  '		DELETE FROM @RunCommand' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  '		SET @i = @i + 1' + CHAR(13) + CHAR(10)
	SELECT @SQL = @SQL +  'END' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
END

SELECT @SQL = @SQL +  'EXEC sp_configure ' + '''show advanced options''' + ', 1' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RECONFIGURE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'EXEC sp_configure '+ '''xp_cmdshell''' + ', @StoredXP_cmdshellValue' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RECONFIGURE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'EXEC sp_configure ' + '''show advanced options''' + ', 0' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RECONFIGURE' + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--		*** End of Script ***'
SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step04
SELECT @SQL

SET @Execute_ScriptToExport = 'BCP ' + QUOTENAME('SELECT SQLToRun FROM Tempdb.dbo.LS_Step04', '"') + ' QUERYOUT ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime + '\04.sql -T -c'

INSERT Tempdb.dbo.LS_RedirectOutput
	EXEC master..xp_cmdshell @Execute_ScriptToExport

SELECT @SQL = ''


--========================================================================================================================================================================

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--								-----> STEP 05 of 10 <-----													' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--PRIMARY Server In Log Shipping Configuration:   ' + @PRIMARYDatabaseServer + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--SECONDARY Server In Log Shipping Configuration: ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--' + @SECONDARYDatabaseServer + ': EXECUTE this script against ' + @SECONDARYDatabaseServer + ' - Makes all databases available for use.' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'IF (SELECT @@SERVERNAME) = ''' + @SECONDARYDatabaseServer + ''' GOTO ServerConfirmed' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RAISERROR(' + '''INCORRECT SERVER SPECIFIED: Please Close This Script, Reopen, And Verify Which Server To Connect To Before Executing...''' +', 20, -1) with log' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ServerConfirmed:' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step05
	SELECT @SQL

INSERT Tempdb.dbo.LS_Step05
	SELECT 'RESTORE LOG [' + SECONDARY_database + '] WITH RECOVERY' + CHAR(13) + CHAR(10) 
	FROM MSDB.dbo.log_shipping_SECONDARY_databases
	ORDER BY SECONDARY_database ASC  

INSERT Tempdb.dbo.LS_Step05
	SELECT 'ALTER DATABASE [' + SECONDARY_database + '] SET MULTI_USER;' + CHAR(13) + CHAR(10) 
	FROM MSDB.dbo.log_shipping_SECONDARY_databases 
	ORDER BY SECONDARY_database ASC

SELECT @SQL = ''
SELECT @SQL = @SQL +  'SELECT ' + '''DATABASES ARE NOW AVAILABLE ON ''' + ' + ''' + @SECONDARYDatabaseServer + '''' + ' AS ' + '''													''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SELECT ' + '''DO NOT RUN ANY FURTHER SQL FILES UNTIL READY TO FAIL BACK TO ''' + ' + ''' + @PRIMARYDatabaseServer + '''' + ' AS ' + '''													''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '--		*** End of Script ***'
SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step05
	SELECT @SQL

SET @Execute_ScriptToExport = 'BCP ' + QUOTENAME('SELECT SQLToRun FROM Tempdb.dbo.LS_Step05', '"') + ' QUERYOUT ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime + '\05.sql -T -c'

INSERT Tempdb.dbo.LS_RedirectOutput
	EXEC master..xp_cmdshell @Execute_ScriptToExport

SELECT @SQL = ''


--========================================================================================================================================================================

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--								-----> STEP 06 of 10 <-----													' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--PRIMARY Server In Log Shipping Configuration:   ' + @PRIMARYDatabaseServer + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--SECONDARY Server In Log Shipping Configuration: ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--	DO NOT RUN THIS OR ANY FURTHER COMMANDS UNTIL READY TO FAIL BACK TO ' + @PRIMARYDatabaseServer + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' +CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--' + @SECONDARYDatabaseServer + ': EXECUTE this script against ' + @SECONDARYDatabaseServer + ' - Backs up logs from SECONDARY to apply back to the PRIMARY to ensure databases are in sync.' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  'IF (SELECT @@SERVERNAME) = ''' + @SECONDARYDatabaseServer + ''' GOTO ServerConfirmed' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RAISERROR(' + '''INCORRECT SERVER SPECIFIED: Please Close This Script, Reopen, And Verify Which Server To Connect To Before Executing...''' +', 20, -1) with log' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ServerConfirmed:' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects o WHERE o.xtype IN (''' + 'U' + ''') AND o.id = object_id(' + '''Tempdb.dbo.LS_StatementQueue''' + '))' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '	DROP TABLE Tempdb.dbo.LS_StatementQueue' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @SQLToExecute VARCHAR(2500) ' + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL 
		+ 'SET @SQLToExecute = ' 
		+ '''SELECT  secondary_database ,
        LEFT(last_restored_file, ( LengthOfFullPath - LengthOfFileName ) - 1) AS ''' + ' + ' + '''''''TranLogBackupPath''''''' + ' + ' + ''', ''' + ' + ' +  
        '''LastTranFileBackedUp ,
        TranNameForFailBack
FROM    ( SELECT    secondary_database ,
                    last_restored_file ,
                    LEN(last_restored_file) AS ''' + ' + ' + '''''''LengthOfFullPath''''''' + ' + ' + ''', ''' + ' + ' + 
                    '''LEN(RIGHT(last_restored_file,
                              CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', REVERSE(last_restored_file), 1) - 1)) AS ''' + ' + ' + '''''''LengthOfFileName''''''' + ' + ' + ''', ''' + ' + ' + 
                    '''RIGHT(last_restored_file,
                          CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', REVERSE(last_restored_file), 1) - 1) AS ''' + ' + ' + '''''''LastTranFileBackedUp''''''' + ' + ' + ''', ' +
                    'secondary_database + ''' + ' + ' + '''''''' + '_' + '''''''' + ' + ' + '''
                    + CAST(CAST(RIGHT(LEFT(RIGHT(last_restored_file,
                                                 	CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', ''' + ' + ' + '''
                                                           REVERSE(last_restored_file),
                                                           1) - 1),
                                        LEN(RIGHT(last_restored_file,
                                                     	CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', ''' + ' + ' + 
                                                              '''REVERSE(last_restored_file),
                                                              1) - 1)) - 4),
                                      	LEN(LEFT(RIGHT(last_restored_file,
                                                     	CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', ''' + ' + ' + 
                                                              '''REVERSE(last_restored_file),
                                                              1) - 1),
                                        LEN(RIGHT(last_restored_file,
                                                        CHARINDEX(''' + ' + ' + '''''''\''''''' + ' + ' + ''', ''' + ' + ' + 
                                                              '''REVERSE(last_restored_file),
                                                              1) - 1)) - 4))
                                      - LEN(secondary_database) - 1) AS BIGINT)
                    + 2 AS VARCHAR(1000)) + ''' + ' + ' + '''''''.trn''''''' + ' + ' + ''' AS ''' + ' + ' + '''''''TranNameForFailBack''''''' + ' + ' + 
          ''' FROM ''' + ' + ''' + '[' + @SECONDARYDatabaseServer + ']' + '.msdb.dbo.log_shipping_monitor_secondary ''' + ' + ' + ''')''' + ' + ' + ''' AS A ''' + ' ' + CHAR(13) + CHAR(10)  + CHAR(13) + CHAR(10) +
		 'INSERT Tempdb.[dbo].[LS_DatabaseInfo_SECONDARY] EXEC(@SQLToExecute)' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)


SELECT @SQL = @SQL +  'SELECT * INTO Tempdb.dbo.LS_StatementQueue' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'FROM ' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SELECT ' + '''ALTER DATABASE [''' + ' + SECONDARY_database  + ' + '''] SET SINGLE_USER with ROLLBACK IMMEDIATE; ''' +  + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ' + ' + '''ALTER DATABASE [''' + ' + SECONDARY_database + ' + '''] SET MULTI_USER ''' +  + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ' + ' + '''BACKUP LOG [''' + ' + SECONDARY_database + ' + '''] TO DISK = ' + ''''''' + TranLogBackupPath + ' + '''\''' + ' + TranNameForFailBack + ' + '''''''' + ' WITH NORECOVERY;''' +  ' AS ' + '''StatementToRun''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'FROM MSDB.dbo.log_shipping_SECONDARY_databases LSS INNER JOIN Tempdb.dbo.LS_DatabaseInfo_SECONDARY DBI on LSS.secondary_database = DBI.DatabaseName' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ') AS StatementToRunTable' + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'ORDER BY StatementToRun ASC' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @SQLToRun TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ExecuteStatement VARCHAR(5000)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'DECLARE @whilecounter AS INT' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @i AS INT' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @StatementToRun AS VARCHAR(5000)' + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'SET @i = 1' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SET @whilecounter = (SELECT COUNT(*) FROM Tempdb.dbo.LS_StatementQueue)' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'WHILE @i <= @whilecounter' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'BEGIN' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DELETE TOP(1) FROM Tempdb.dbo.LS_StatementQueue' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		OUTPUT DELETED.StatementToRun INTO @SQLToRun' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		INSERT Tempdb.dbo.LS_Step06' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SELECT ExecuteStatement FROM @SQLToRun' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '			SET @StatementToRun = (select TOP(1) ExecuteStatement from @SQLToRun)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '			USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '			SELECT ''' + 'Executing Statement: ''' + ' + ' + '@StatementToRun AS ' + '''										''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '			EXEC (@StatementToRun)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DELETE FROM @SQLToRun' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SET @i = @i + 1' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'END' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step06
	SELECT @SQL

INSERT Tempdb.dbo.LS_Step06
	SELECT '--		*** End of Script ***' 

SET @Execute_ScriptToExport = 'BCP ' + QUOTENAME('SELECT SQLToRun FROM Tempdb.dbo.LS_Step06', '"') + ' QUERYOUT ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime + '\06.sql -T -c'

INSERT Tempdb.dbo.LS_RedirectOutput
	EXEC master..xp_cmdshell @Execute_ScriptToExport

SELECT @SQL = ''


--========================================================================================================================================================================

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--								-----> STEP 07 of 10 <-----													' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--PRIMARY Server In Log Shipping Configuration:   ' + @PRIMARYDatabaseServer + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--SECONDARY Server In Log Shipping Configuration: ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

IF @FailOverFromPRIMARY = 'N' SELECT @SQL = @SQL +  '/*' + CHAR(13) + CHAR(10) + '!! A full failover from the PRIMARY was NOT chosen.  Therefore, this script is NOT used in the Failover.' + CHAR(13) + CHAR(10) + '!! Please disregard this script and proceed to the next one in the process.' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--' + @SECONDARYDatabaseServer + ': EXECUTE this script against ' + @SECONDARYDatabaseServer + ' - Remotely applies log files created earlier to bring PRIMARY in sync with the SECONDARY and back online.' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10) 

SELECT @SQL = @SQL +  'IF (SELECT @@SERVERNAME) = ''' + @SECONDARYDatabaseServer + ''' GOTO ServerConfirmed' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RAISERROR(' + '''INCORRECT SERVER SPECIFIED: Please Close This Script, Reopen, And Verify Which Server To Connect To Before Executing...''' +', 20, -1) with log' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ServerConfirmed:' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step07
SELECT @SQL

SELECT @SQL = ''

SELECT @SQL = @SQL +  'DECLARE @SQLToRun TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ExecuteStatement VARCHAR(5000)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @SQLToRunQueue TABLE' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '(' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ExecuteStatement VARCHAR(5000)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  ')' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'INSERT @SQLToRunQueue ' + 'SELECT ' + '''RESTORE LOG [''' + ' + SECONDARY_database + ' + '''] FROM DISK = ' + ''''''' + TranLogBackupPath + ' + '''\''' + ' + TranNameForFailBack + ' + '''''''' + ' WITH RECOVERY;''' +  ' AS ' + '''StatementToRun''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'FROM MSDB.dbo.log_shipping_SECONDARY_databases LSS INNER JOIN Tempdb.dbo.LS_DatabaseInfo_SECONDARY DBI on LSS.secondary_database = DBI.DatabaseName' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @whilecounter AS INT' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @i AS INT' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'DECLARE @StatementToRun AS VARCHAR(5000)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SET @i = 1' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'SET @whilecounter = (SELECT COUNT(*) FROM @SQLToRunQueue)' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  'WHILE @i <= @whilecounter' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'BEGIN' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DELETE TOP(1) FROM @SQLToRunQueue' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		OUTPUT DELETED.ExecuteStatement INTO @SQLToRun' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		INSERT Tempdb.dbo.LS_Step07' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SELECT ExecuteStatement FROM @SQLToRun' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '			SET @StatementToRun = (select TOP(1) ExecuteStatement from @SQLToRun)' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '			USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '			SELECT ''' + 'Executing Statement: ''' + ' + ' + '@StatementToRun AS ' + '''										''' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '			EXEC (@StatementToRun)' + ' AT ' + '[' + @PRIMARYDatabaseServer + ']' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		DELETE FROM @SQLToRun' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '		SET @i = @i + 1' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'END' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '--		*** End of Script ***'
SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)

IF @FailOverFromPRIMARY = 'N' 
	SELECT @SQL = @SQL +  CHAR(13) + CHAR(10) + '*/' 

INSERT Tempdb.dbo.LS_Step07
	SELECT @SQL

SET @Execute_ScriptToExport = 'BCP ' + QUOTENAME('SELECT SQLToRun FROM Tempdb.dbo.LS_Step07', '"') + ' QUERYOUT ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime + '\07.sql -T -c'

INSERT Tempdb.dbo.LS_RedirectOutput
	EXEC master..xp_cmdshell @Execute_ScriptToExport

SELECT @SQL = ''


--========================================================================================================================================================================

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--								-----> STEP 08 of 10 <-----													' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  'USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--PRIMARY Server In Log Shipping Configuration:   ' + @PRIMARYDatabaseServer + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--SECONDARY Server In Log Shipping Configuration: ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

IF @FailOverFromPRIMARY = 'N' SELECT @SQL = @SQL +  '/*' + CHAR(13) + CHAR(10) + '!! A full failover from the PRIMARY was NOT chosen.  Therefore, this script is NOT used in the Failover.' + CHAR(13) + CHAR(10) + '!! Please disregard this script and proceed to the next one in the process.' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--' + @PRIMARYDatabaseServer + ': EXECUTE this script against ' + @PRIMARYDatabaseServer + ' - Sets all databases back to MULTI_USER mode instead of SINGLE_USER. ' +  + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  'IF (SELECT @@SERVERNAME) = ''' + @PRIMARYDatabaseServer + ''' GOTO ServerConfirmed' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RAISERROR(' + '''INCORRECT SERVER SPECIFIED: Please Close This Script, Reopen, And Verify Which Server To Connect To Before Executing...''' +', 20, -1) with log' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ServerConfirmed:' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step08
	SELECT @SQL

INSERT Tempdb.dbo.LS_Step08
	SELECT 'ALTER DATABASE [' + SECONDARY_database + '] SET MULTI_USER;' + CHAR(13) + CHAR(10) 
	FROM MSDB.dbo.log_shipping_SECONDARY_databases
	ORDER BY SECONDARY_database ASC

SELECT @SQL = ''

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--		*** End of Script ***'
SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step08
	SELECT @SQL

IF @FailOverFromPRIMARY = 'N' 
	SELECT @SQL = @SQL +  CHAR(13) + CHAR(10) + '*/'

SET @Execute_ScriptToExport = 'BCP ' + QUOTENAME('SELECT SQLToRun FROM Tempdb.dbo.LS_Step08', '"') + ' QUERYOUT ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime + '\08.sql -T -c'

INSERT Tempdb.dbo.LS_RedirectOutput
	EXEC master..xp_cmdshell @Execute_ScriptToExport

SELECT @SQL = ''


--========================================================================================================================================================================

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--								-----> STEP 09 of 10 <-----													' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  'USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--PRIMARY Server In Log Shipping Configuration:   ' + @PRIMARYDatabaseServer + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--SECONDARY Server In Log Shipping Configuration: ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

IF @FailOverFromPRIMARY = 'N' SELECT @SQL = @SQL +  '/*' + CHAR(13) + CHAR(10) + '!! A full failover from the PRIMARY was NOT chosen.  Therefore, this script is NOT used in the Failover.' + CHAR(13) + CHAR(10) + '!! Please disregard this script and proceed to the next one in the process.' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--' + @PRIMARYDatabaseServer + ': EXECUTE this script against ' + @PRIMARYDatabaseServer + ' - This script re-enables all log BACKUP jobs on the PRIMARY.' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'IF (SELECT @@SERVERNAME) = ''' + @PRIMARYDatabaseServer + ''' GOTO ServerConfirmed' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RAISERROR(' + '''INCORRECT SERVER SPECIFIED: Please Close This Script, Reopen, And Verify Which Server To Connect To Before Executing...''' +', 20, -1) with log' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ServerConfirmed:' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step09
	SELECT @SQL

INSERT Tempdb.dbo.LS_Step09
	SELECT 'EXEC msdb.dbo.sp_update_job @job_name = ''' + name + N''', @enabled = 1;' + CHAR(13) + CHAR(10) 
	FROM Tempdb.dbo.LS_SysJobNames
	WHERE name LIKE 'LSBackup%'
	ORDER BY name

SELECT @SQL = ''

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--		*** End of Script ***'
SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step09
	SELECT @SQL

IF @FailOverFromPRIMARY = 'N' 
	SELECT @SQL = @SQL +  CHAR(13) + CHAR(10) + '*/' 

SET @Execute_ScriptToExport = 'BCP ' + QUOTENAME('SELECT SQLToRun FROM Tempdb.dbo.LS_Step09', '"') + ' QUERYOUT ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime + '\09.sql -T -c'

INSERT Tempdb.dbo.LS_RedirectOutput
	EXEC master..xp_cmdshell @Execute_ScriptToExport

SELECT @SQL = ''


--========================================================================================================================================================================

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--								-----> STEP 10 of 10 <-----													' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  'USE master' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--PRIMARY Server In Log Shipping Configuration:   ' + @PRIMARYDatabaseServer + CHAR(13) + CHAR(10) 
SELECT @SQL = @SQL +  '--SECONDARY Server In Log Shipping Configuration: ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--' + @SECONDARYDatabaseServer + ': EXECUTE this script against ' + @SECONDARYDatabaseServer + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '-- This script re-enables all log RESTORE jobs on the SECONDARY and resets the restore delay back to what it was before.' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'IF (SELECT @@SERVERNAME) = ''' + @SECONDARYDatabaseServer + ''' GOTO ServerConfirmed' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'RAISERROR(' + '''INCORRECT SERVER SPECIFIED: Please Close This Script, Reopen, And Verify Which Server To Connect To Before Executing...''' +', 20, -1) with log' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '---------------------------------------------------------------------------------------------------------------------------------------------------------------' + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  'ServerConfirmed:' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step10
	SELECT @SQL

SELECT @SQL = ''

INSERT Tempdb.dbo.LS_Step10
	SELECT 'EXEC msdb.dbo.sp_update_job @job_name = ''' + name + N''', @enabled = 1;' + CHAR(13) + CHAR(10) 
	FROM Tempdb.dbo.LS_SysJobNames
	WHERE name LIKE 'LSCopy%' OR name LIKE 'LSRestore%'
	ORDER BY name

INSERT Tempdb.dbo.LS_Step10
	SELECT 'UPDATE msdb.dbo.log_shipping_secondary_databases SET restore_delay = ' + RestoreDelay + ' WHERE secondary_database = ' + '''' + SecondaryDatabase + '''' + CHAR(13) + CHAR(10)
	FROM @RestoreDelayForDatabases 

SELECT @SQL = @SQL +  CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
SELECT @SQL = @SQL +  '--		*** End of Script ***'
SELECT @SQL = @SQL +  CHAR(13) + CHAR(10)

INSERT Tempdb.dbo.LS_Step10
	SELECT @SQL

SET @Execute_ScriptToExport = 'BCP ' + QUOTENAME('SELECT SQLToRun FROM Tempdb.dbo.LS_Step10', '"') + ' QUERYOUT ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime + + '\10.sql -T -c'

INSERT Tempdb.dbo.LS_RedirectOutput
	EXEC master..xp_cmdshell @Execute_ScriptToExport

EXEC sp_configure 'show advanced options', 1
RECONFIGURE
EXEC sp_configure 'xp_cmdshell', @StoredXP_cmdshellValue
RECONFIGURE
EXEC sp_configure 'show advanced options', 0
RECONFIGURE

SELECT 'Scripts 01.sql through 10.sql are now available at the following location:' AS 'Script Generation Complete...						', @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime AS '															' 

SELECT 'Open ' + @ScriptsLocation + '\Streamline_LS_Failovers_' + @FormattedDateTime + ' within SQL Management Studio, select all 10 .SQL scripts and press Enter to open them.' AS 'What To Do Next:																					'


--> End of Master Script


/*
==============================================================================================================================================
								
							Appendix: 

==============================================================================================================================================
Thoughts Influencing The Development Of This Solution:
----------------------------------------------------------------------------------------------------------------------------------------------
	1. The desire to fully utilize Microsoft's proven Log Shipping technology in repeatably failing over between servers.

	2. The opportunity to make improvements to the overall database failover process:
		a. Provides logical, Step-By-Step scripts to help systematically fail over to the SECONDARY server and then back  
			to the PRIMARY server.  The code being run is highly visible to the user, which promotes understanding
			and avoids working with a 'black box' process.
		b. Gives ability to troubleshoot any issues by narrowing problems down to a particular script/step.
		c. Generates dynamic handling of every database participating in Log Shipping.  This is a very important benefit for
			failing over SharePoint servers and consolidated database servers, both of which can have a high number of databases.

	3. The overall desire to have a simple, low-tech method of performing failovers.  This requires resisting the urge to overautomate
		the process, which can lead to obscure errors / issues and make it difficult for anyone other than the author to run and 
		troubleshoot.  


==============================================================================================================================================
Overview of Steps Performed By This Failover Script:
----------------------------------------------------------------------------------------------------------------------------------------------
-- Declares and populates variables used throughout the script.
-- Allows user to set necessary variables according to their needs and environment.
-- Prepares output helpful to the user running this script.
-- Ensures xp_cmdshell is enabled to help generate scripts for later use.

-- Creates Script 01 to run against the PRIMARY and saves it to the designated folder.
	--> Enables xp_cmdshell temporarily to help run the remainder of this script.
	--> Generates some necessary constructs (temp tables, etc.) for later use in the script.
	--> Creates a directory for the Failover scripts to be stored in for later use.

-- Creates Script 02 to run against the SECONDARY and saves it to the designated folder.
	--> Updates the restore delay to 0 for all database jobs.
	--> Executes all active Restore Log jobs and then disables them, saving a list for enabling them
		again later.

-- Creates Script 03 to run against the SECONDARY and saves it to the designated folder.
	--> Kills any active connections (except yours) to all the log shipped databases.
	--> Performs final log backup for each database (to be later applied to SECONDARY).
	--> Places each database into NO_RECOVERY mode so they cannot be accessed and are ready 
		for failback at any time.

-- Creates Script 04 to run on the SECONDARY and saves it to the designated folder.
	--> Reads each database's backup location and creates a list of files from each directory.
	--> Restores all available logs on the SECONDARY's databases so they can be enabled for use.

-- Creates Script 05 to run against the SECONDARY and saves it to the designated folder.
	--> Makes each database accessible for use.

-- > This completes failing over to the SECONDARY.  The SECONDARY can then be used until it is time to fail back.
-- > PLEASE NOTE: if failing over for a significant amount of time, a trained DBA must be involved to ensure log
-- > backups are properly configured and running on the SECONDARY, etc.
-- > The second set of 5 scripts fail back to the PRIMARY, taking any changes made on the SECONDARY with them.

-- Creates Script 06 to run against the SECONDARY and saves it to the designated folder.
	--> Backs up any database changes performed on the SECONDARY in order to apply back to the PRIMARY.  Leaves
		databases in NO_RECOVERY mode so they can begin receiving logs in the log shipping configuration.

-- Creates Script 07 to run against the PRIMARY and saves it to the designated folder.
	--> Applies changes mentioned above on PRIMARY in order to bring the PRIMARY and SECONDARY
		databases back into sync.

-- Creates Script 08 to run against the PRIMARY and saves it to the designated folder.
	--> Enables each database so they're now accessible for use again.

-- Creates Script 09 to run against the PRIMARY and saves it to the designated folder.
	--> Enables all previously active log backup jobs.

-- Creates Script 10 to run against the SECONDARY and saves it to the designated folder.
	--> Enables all previously active log restore jobs.
	--> Ensures xp_cmdshell is set to the value it was at before this process began.


==============================================================================================================================================
Common Questions:
----------------------------------------------------------------------------------------------------------------------------------------------
	1. Couldn't SQLCMD mode, PowerShell, etc. be utilized to help with automating this process?
		- Absolutely, these tools are certainly available for use and could be used to help further automate this process.
			However, my goal was to keep the overall process as simple as possible, which meant not needing anything more
			than a SQL script.  Having this and being able to connect to the SECONDARY server is all you really need.

	2. Why do you use both table variables and tables explicitly created in TempDB?
		- Table variables are used unless I want to keep the data around for troubleshooting purposes.  Should a script 
			be run and it fail, one could check these tables for diagnostic information while troubleshooting
			the issue.  Anything stored in a table variable is obviously gone once the script stops running...

	3. For Step 4, couldn't you simplify things by letting SQL Server jobs apply all the logs?
		- Yes, this could potentially be done.  However, the code implemented for Step 4 allows the process of failing over
			to still take place even if the SQL jobs are not able to run due to some issue. This gives greater assurance to
			the user that, in the event of a disaster, they don't have to rely on a component that may not be available.    

	4. Why do you output scripts 01.sql through 10.sql instead of 1.sql through 10.sql?
		- This is to help make things easier when you open the files.  If you select them all through SQL Management Studio
			to open, they open in the desired order.  Naming the files the other way results in sorting issues when looking
			at the scripts.

	5. When creating the last log backup before failing over, why is the backup named the same as the last one, but incremented by 1?
		- Interestingly, naming the log backups something more custom (ex. <tran backup name>_Failover.trn) confuses the Copy job and it won't 
			be copied across to the Secondary server.  This then causes issues when trying to bring the Secondary online.

	6. What types of Log Shipping configurations does this solution support?
		- This solution is meant to support two main configurations (both using native, built-in Microsoft Log Shipping), which are as follows:
			a. A typical Microsoft Log Shipping configuration in which Log Shipping does a backup of the log on the PRIMARY, copies it  
			   	across to the SECONDARY, and then restores it.
			b. A Microsoft Log Shipping configuration in which the logs of the PRIMARY are backed up to a network location from which the
			   	SECONDARY then applies them. In this configuration the Copy jobs are disabled as they are not needed.
	
	7. Why use Log Shipping when there's (depending on version...) database mirroring, Availability Groups, etc.?
		- Log Shipping is really a Disaster Recovery solution and not a High Availability one.  This distinction is important, as Log Shipping
			can be used to complement a High Availability solution rather than to try and replace it. 

*/