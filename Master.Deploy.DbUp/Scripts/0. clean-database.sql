--DROP DATABASE OBJECTS

declare @n char(1)
set @n = char(10)

declare @stmt nvarchar(max)

select @stmt = isnull( @stmt + @n, '' ) +
    'drop procedure [' + schema_name(schema_id) + '].[' + name + ']'
from sys.procedures

select @stmt = isnull( @stmt + @n, '' ) +
'alter table [' + schema_name(schema_id) + '].[' + object_name( parent_object_id ) + ']    drop constraint [' + name + ']'
from sys.check_constraints

select @stmt = isnull( @stmt + @n, '' ) +
    'drop function [' + schema_name(schema_id) + '].[' + name + ']'
from sys.objects
where type in ( 'FN', 'IF', 'TF' )

select @stmt = isnull( @stmt + @n, '' ) +
    'drop view [' + schema_name(schema_id) + '].[' + name + ']'
from sys.views

select @stmt = isnull( @stmt + @n, '' ) +
    'alter table [' + schema_name(schema_id) + '].[' + object_name( parent_object_id ) + '] drop constraint [' + name + ']'
from sys.foreign_keys

select @stmt = isnull( @stmt + @n, '' ) +
    'drop table [' + schema_name(schema_id) + '].[' + name + ']'
from sys.tables

select @stmt = isnull( @stmt + @n, '' ) +
    'drop type [' + schema_name(schema_id) + '].[' + name + ']'
from sys.types
where is_user_defined = 1

exec sp_executesql @stmt

--DROP DATABASE SCHEMAS
DECLARE @name VARCHAR(128), @sqlCommand NVARCHAR(1000), @Rows INT = 0, @i INT = 1;
DECLARE @t TABLE(RowID INT IDENTITY(1,1), ObjectName VARCHAR(128));
 
INSERT INTO @t(ObjectName)
SELECT s.[SCHEMA_NAME] FROM INFORMATION_SCHEMA.SCHEMATA s
WHERE s.[SCHEMA_NAME] NOT IN('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
 
SELECT @Rows = (SELECT COUNT(RowID) FROM @t), @i = 1;
 
WHILE (@i <= @Rows) 
BEGIN
    SELECT @sqlCommand = 'DROP SCHEMA [' + t.ObjectName + '];', @name = t.ObjectName FROM @t t WHERE RowID = @i;
    EXEC sp_executesql @sqlCommand;        
    PRINT 'Dropped SCHEMA: [' + @name + ']';    
    SET @i = @i + 1;
END
GO
/***** END - DROP SCHEMAs *****/

