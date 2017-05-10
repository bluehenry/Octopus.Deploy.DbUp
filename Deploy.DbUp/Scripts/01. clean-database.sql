--USE *database_name*

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

DROP SCHEMA vdt
DROP SCHEMA staging