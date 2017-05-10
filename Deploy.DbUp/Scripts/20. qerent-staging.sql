
--USE *database_name*

create schema staging;
go

create type staging.QerentDataType as table (
  [Object Path] nvarchar(256) not null,
  [Attribute] nvarchar(100) not null,
  [Value] double precision not null);
go


CREATE TYPE [staging].[QerentStructureType] AS TABLE (
    [QerentAttrId] INT NULL,
	[O1] [nvarchar](100) NULL,
	[O2] [nvarchar](100) NULL,
	[O3] [nvarchar](100) NULL,
	[O4] [nvarchar](100) NULL,
	[O5] [nvarchar](100) NULL,
	[O6] [nvarchar](100) NULL,
	[O7] [nvarchar](100) NULL,
	[O8] [nvarchar](100) NULL,
	[O9] [nvarchar](100) NULL,
	[O10] [nvarchar](100) NULL,
	[O11] [nvarchar](100) NULL,
	[Object Path] [nvarchar](256) NOT NULL,
	[Attribute] [nvarchar](100) NOT NULL,
	[Value] [float] NOT NULL,
	[Type] [nvarchar](10) NOT NULL,
	[DisplayUnit] [nvarchar](20) NOT NULL,
	[SecLevel] [int] NULL
);
GO

create table [staging].[qerent_attr_ids] (
  model_id int not null references [vdt].model (model_id),
  attr_id int not null references [vdt].[attribute] (attr_id),
  object_path nvarchar(256) not null,
  qerent_attr_id int,
  attr_name nvarchar(100) not null);
  
create index qerent_attr_ids_object_path_attr_name on [staging].[qerent_attr_ids] (model_id, object_path, attr_name);

go

create procedure staging.sp_get_attribute_id_map_to_qerent
  @model_filename nvarchar(256)
as
select object_path + '[' + attr_name + ']' as full_path, I.attr_id,
  case
    when org_group in ('Rail', 'Port', 'WAIO') then '/#!/scenario_planning?site=' + org_group
    when org_group='Mines' then '/#!/scenario_planning?site=' + REPLACE(location, ' ', '%20')
	else ''
  end as link
from staging.qerent_attr_ids I inner join vdt.attribute_category C on I.attr_id=C.attr_id
--inner join vdt.model M on I.model_id=M.model_id
--where (M.filename=@model_filename or M.filename='_debug' and left(@model_filename, 1)='_')
go

create procedure staging.sp_update_scenario_from_qerent (
  @dataset_id int,
  @data staging.QerentDataType readonly)
as
begin
  set nocount on

  /* This can happen after a dataset import */
  insert into vdt.value (dataset_id, attr_id, value)
  select @dataset_id, A.attr_id, D.Value
  from @data D inner join staging.qerent_attr_ids A
    on D.[Object Path]=A.object_path
	and D.[Attribute]=A.attr_name
  where A.attr_id not in (select attr_id from vdt.value where dataset_id=@dataset_id)

  update vdt.value set value.value = D.[Value]
  from @data D inner join staging.qerent_attr_ids A
    on D.[Object Path]=A.object_path
	and D.[Attribute]=A.attr_name
  where value.attr_id=A.attr_id and value.dataset_id=@dataset_id;
  
  update vdt.dataset set last_update=CURRENT_TIMESTAMP
  where dataset_id=@dataset_id;
end

go

create view staging.qerent_import as
select V.dataset_id, Q.object_path, Q.attr_name, coalesce(C.new_value, V.value) as value
from vdt.value V inner join vdt.attribute A on V.attr_id=A.attr_id
inner join vdt.dataset D on V.dataset_id=D.dataset_id
inner join staging.qerent_attr_ids Q on A.attr_id=Q.attr_id --and D.model_id=Q.model_id
left join vdt.scenario S on S.view_dataset_id=V.dataset_id
left join vdt.scenario_change C on S.scenario_id=C.scenario_id and C.attr_id=A.attr_id
where A.is_calculated=0

go

CREATE TYPE [staging].[QerentDependencyType] AS TABLE (
    [QerentAttrId] INT,
	[DependsOnQerentAttrId] INT
)
go

create procedure [staging].[sp_import_qerent_model_structure] (
    @model_id int,
    @filename nvarchar(256),
	@data staging.QerentStructureType readonly,
	@deps staging.QerentDependencyType readonly,
	@top_of_tree_qattr_id int)
as
begin
  set nocount on

  -- debugging purposes only
  if object_id(N'staging.temp_qerent_import', N'U') is not null
    drop table staging.temp_qerent_import;
  select * into staging.temp_qerent_import from @data;
  -- end debugging

  if left(@filename, 1) = '_' set @filename = '_debug'
  declare @should_copy_data bit = 0
  set @should_copy_data = 1;

  -- This lengthy query sets up a view of how the attributes from Qerent should be mapped into the vdt.attributes table.
  -- The result of this will get stored in #qerent_attrs later.
  with QAttrs as (
  select * from @data)
  
  ,DrillFleets as (
  select [Object Path], [O2] as Mine, [O5] as Fleet from QAttrs where O1='Mines' and O4='Drill' and [Attribute]='Target tonnes')
  
  ,DrillProd as (
  select 'Mines' as org_group, 'Mining' as [function], 'Drill' as activity, 'Operations' as workstream, F.Mine as location,
    'Drills' as equipment_type, F.Fleet as equipment, null as cost_type, null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name, A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from DrillFleets F inner join QAttrs A on F.[Object Path]=A.[Object Path]
  )--where A.[Attribute] not in ('Headroom', 'Cost', 'Fixed cost', 'Variable cost'))
  
  ,DrillFleetCostLevers as (
  select 'Mines' as org_group, 'Mining' as [function], 'Drill' as activity,
    case when substring([O7], 1, 11)='Maintenance' then 'Maintenance' else 'Operations' end as workstream,
    F.Mine as location, 'Drills' as equipment_type, F.Fleet as equipment, coalesce([O8], [O7]) as cost_type, null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name, A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A inner join DrillFleets F on substring(A.[Object Path], 1, len(F.[Object Path]) + 7) = F.[Object Path] + '.Costs.'
  where left(A.[Type],1)='C')
  
  ,DrillOperationsCostLevers as (
  select 'Mines' as org_group, 'Mining' as [function], 'Drill' as activity, 'Operations' as workstream,
    A.[O2] as location, null as equipment_type, null as equipment, coalesce([O7], [O6]) as cost_type, null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name, A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O4]='Drill' and [O5]='Operations' and [O6] is not null
  and left(A.[Type],1) = 'C'
  and (A.[O6] <> 'Fuels & Oils' or A.[O7] is not null))
  
  ,DrillSummary as (
  select 'Mines' as org_group, 'Mining' as [function], 'Drill' as activity, 'Operations' as workstream,
    A.[O2] as location, null as equipment_type, null as equipment, null as cost_type, null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name, A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O4]='Drill' and [O5] is null
  and [Attribute] not in ('Use of Headroom'))
  
  ,DrillStock as (
  select 'Mines' as org_group, 'Mining' as [function], 'Drill' as activity, 'Operations' as workstream,
    A.[O2] as location, 'Stockpile' as equipment_type, 'Drill Stock' as equipment, null as cost_type, null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O4]='Drill' and [O5]='Drill Inventory'
  )
  
  ,Blast as (
  select 'Mines' as org_group, 'Mining' as [function], 'Blast' as activity, 'Operations' as workstream,
    A.[O2] as location,
    case when [O5]='Blast Inventory' then 'Stockpile' else null end as equipment_type,
    case when [O5]='Blast Inventory' then [O5] else null end as equipment,
    [O6] as cost_type, null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O4]='Blast'
  )

  ,LoadHaul as (
  select 'Mines' as org_group, 'Mining' as [function],
    case when [O5] is null or [O5]='Operations' then 'Load & Haul' else [O5] end as activity,
	'Operations' as workstream,
    A.[O2] as location,
    case when [O5]='Load' then left([O6], len([O6])-1) when [O6] is null then null when [O5]='Haul' then 'Truck' else null end as equipment_type,
    case when [O5]='Load' then [O7] else [O6] end as equipment,
    case [O5] when 'Operations' then [O6] when 'Load' then [O9] else [O8] end as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O4]='Load & haul'
  )
  
  ,MineServicesAndOverheads as (
  select 'Mines' as org_group, 'Mining' as [function],
    [O4] as activity,
	case when [O4] is null then null when [O5]='Operations' or [O4]='Overheads' then 'Operations' else 'Support' end as workstream,
    A.[O2] as location,
    case when [O4] is null then null when [O5]='Operations' then null when [O4]='Overheads' then [O5] else 'Support' end as equipment_type,
    case when [O5]='Operations' then null when [O4]='Overheads' then [O6] else [O5] end as equipment,
    case when [O4]='Overheads' then [O7] else [O6] end as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O3]='Mining' and ([O4] is null or [O4] not in ('Drill', 'Blast', 'Load & haul'))
  )
  
  ,OverheadsMines as (
  select 'Mines' as org_group, 'Mining' as [function],
    'Overheads' as activity,
	coalesce([O4], 'Operations') as workstream,
    null as location,
    null as equipment_type,
    [O3] as equipment,
    coalesce([O5], [O4], [O3]) as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O2]='Overheads Mines'
  )

  ,ProcessingPlants as (
  select 'Mines' as org_group, 'Processing' as [function],
    [O4] as activity,
	'Operations' as workstream,
    [O2] as location,
    'Plant' as equipment_type,
    [O5] as equipment,
    null as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O3]='Processing' and [O6] is null and [O4] not in ('Non Allocated Cost', 'Overheads')
  )

  ,ProcessingPlantCosts as (
  select 'Mines' as org_group, 'Processing' as [function],
    [O4] as activity,
	case when [O8]='Maintenance' then 'Maintenance' else 'Operations' end as workstream,
    [O2] as location,
    'Plant' as equipment_type,
    [O7] as equipment,
    coalesce([O9], [O8]) as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O3]='Processing' and [O6]='Costs'
  )
  
  ,ProcessingNonAllocatedAndOverheads as (
  select 'Mines' as org_group, 'Processing' as [function],
    [O4] as activity,
	[O5] as workstream,
    [O2] as location,
    case when [O7] is not null then [O6] else null end as equipment_type,
    null as equipment,
    coalesce([O7], [O6], [O5]) as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O3]='Processing' and [O4] in ('Non Allocated Cost', 'Overheads')
  )

  ,ProcessingTopLevel as (
  select 'Mines' as org_group, 'Processing' as [function],
    [O4] as activity,
	null as workstream,
    [O2] as location,
    null as equipment_type,
    null as equipment,
    coalesce([O7], [O6], [O5]) as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O3]='Processing' and [O4] is null
  )
  
  ,MinesOverheads as (
  select 'Mines' as org_group, 'Overheads' as [function],
    [O4] as activity,
	case when [O5]='Maintenance' then [O5] else 'Operations' end as workstream,
    [O2] as location,
    case when [O5]='Maintenance' then null else [O5] end as equipment_type,
    null as equipment,
    coalesce([O6], [O5]) as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 3) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O3]='Overheads'
  )

  ,MinesTopLevel as (
  select 'Mines' as org_group, null as [function],
    null as activity,
	null as workstream,
    [O2] as location,
    null as equipment_type,
    null as equipment,
    null as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Mines' and [O3] is null
  )
   
  ,RailVolume as (
  select
    'Rail' as org_group,
	case [O2] when 'Mines OOR' then 'Mainline' when 'Mines OFH' then 'Shuttle' else [O2] end as [function],
    'Rail' as activity,
	'Operations' as workstream,
    case when [O3]='Virtual Stockpile' then null else coalesce([O4], [O3]) end as location,
    null as equipment_type,
    null as equipment,
    coalesce([O6], [O5]) as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Rail' and substring(A.[Type], 1, 1) <> 'C' and [O2] is not null
  )

  ,RailProdCosts as (
  select
    'Rail' as org_group,
	case when [O2] in ('Mainline', 'Shuttle') then 'Production' else [O2] end as [function],
    case when [O2] in ('Mainline', 'Shuttle') then 'Operations' else [O3] end as activity,
	coalesce([O4], 'Direct Cost') as workstream,
    case when [O2] in ('Mainline', 'Shuttle') then coalesce([O3], [O2]) else null end as location,
    null as equipment_type,
    null as equipment,
    coalesce([O6], [O5], 'Diesel') as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Rail' and substring(A.[Type], 1, 1) = 'C' and [O2] not in ('Maintenance', 'Overheads') and [O2] is not null
  )

  ,RailOverheads as (
  select
    'Rail' as org_group,
	[O2] as [function],
    [O3] as activity,
	null as workstream,
    null as location,
    null as equipment_type,
    null as equipment,
    coalesce([O5], [O4]) as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Rail' and [O2]='Overheads'
  )

  ,RailMaintenance as (
  select
    'Rail' as org_group,
	[O2] as [function],
    [O3] as activity,
	case when [o6] is null then 'Supporting Cost' else [o5] end as workstream,
    null as location,
    case when [O4] <> 'Overheads' and [o5] is not null then [o4] else null end as equipment_type,
    null as equipment,
    coalesce([O6], [O5], [O4]) as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Rail' and [O2]='Maintenance'
  )

  ,RailTopLevel as (
  select
    [O1] as org_group,
	null as [function],
    null as activity,
	null as workstream,
    null as location,
    null as equipment_type,
    null as equipment,
    null as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Rail' and [O2] is null
  )

  ,PortInflow as (
  select
    [O1] as org_group,
	'Production' as [function],
    [O2] as activity,
	'Operations' as workstream,
    [O4] as location,
    case when [O3] is null then null else 'Car Dumper' end as equipment_type,
    [O3] as equipment,
    null as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Port' and [O2]='Inflow'
  )

  ,PortOutflow as (
  select
    [O1] as org_group,
	'Production' as [function],
    [O3] as activity,
	'Operations' as workstream,
    coalesce([O6], [O4]) as location,
    left([O7], len([o7])-1) as equipment_type,
    [O8] as equipment,
    null as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Port' and [O3]='Outflow'
  )

  ,PortProductionCosts as (
  select
    [O1] as org_group,
	[O2] as [function],
    [O4] as activity,
	case when [O6] is null then [O4] else [O5] end as workstream,
    [O3] as location,
    left([O7], len([o7])-1) as equipment_type,
    [O8] as equipment,
    coalesce([O6], [O5]) as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Port' and [O3]<>'Outflow' and [O2]='Production'
  )

  ,PortSiteMaintenance as (
  select
    [O1] as org_group,
	[O2] as [function],
    case when [O4]='Overheads' then 'Overheads' else 'Maintenance' end as activity,
	case when [O4]='Overheads' then [O5] else 'Maintenance' end as workstream,
    [O3] as location,
    case when [O4]='Overheads' then null else left([O4], len([O4])-1) end as equipment_type,
    case when [O4]='Overheads' then null else [O5] end as equipment,
    coalesce([O6], [O5]) as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Port' and [O2]='Maintenance' and [O3] in ('Nelson Point', 'Finucane Island')
  )

  ,PortMaintenance as (
  select
    [O1] as org_group,
	[O2] as [function],
    [O3] as activity,
	case when [O5] is null then 'Operations' when [O4]='Overheads' then [O5] when [O4] in ('Nelson Point', 'Finucane Island') then 'Operations' when [O4]='Other' then null else [O4] end as workstream,
    case when [O4] in ('Nelson Point', 'Finucane Island') then [O4] else null end as location,
    null as equipment_type,
    null as equipment,
    coalesce([O6], [O5], [O4]) as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Port' and [O2]='Maintenance' and [O3] not in ('Nelson Point', 'Finucane Island')
  )

  ,PortMarine as (
    select
    [O1] as org_group,
	[O2] as [function],
    [O3] as activity,
	case when [O5] is null then 'Operations' else [O4] end as workstream,
    null as location,
    case when [O3]='Demurrage' then [O4] else null end as equipment_type,
    null as equipment,
    case when [O3]='Demurrage' then 'Demurrage' else coalesce([O5], [O4]) end as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Port' and [O2]='Marine'
  )

  ,PortOverheads as (
  select
    [O1] as org_group,
	[O2] as [function],
    [O3] as activity,
	'Operations' as workstream,
    null as location,
    null as equipment_type,
    null as equipment,
    [O4] as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Port' and [O2]='Overheads'
  )

  ,PortTopLevel as (
  select
    [O1] as org_group,
	null as [function],
    null as activity,
	null as workstream,
    null as location,
    null as equipment_type,
    null as equipment,
    null as cost_type,
	null as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 1) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='Port' and [O2] is null
  )

  ,FunctionsCosts as (
  select
    'WAIO' as org_group,
	[O1] as [function],
    [O2] as activity,
	[O3] as workstream,
    [O4] as location,
    [O5] as equipment_type,
    [O6] as equipment,
    [O7] as cost_type,
	[O8] as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 3) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1] not in ('EBITDA', 'Mines', 'Rail', 'Port')
  )

  ,Revenue as (
  select 'WAIO' as org_group, 'Revenue' as [function],
    'Sales' as activity,
	null as workstream,
    null as location,
    null as equipment_type,
    null as equipment,
    null as cost_type,
	[O2] as product,
    A.DisplayUnit as unit, coalesce(A.[SecLevel], 3) as sec_level,
    case substring(A.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
    case substring(A.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
    case substring(A.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
    case substring(A.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
    case substring(A.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi,
    A.[Attribute] as name,
    A.[Value] as value, A.[Object Path] as path, A.[Attribute] as attribute
  from QAttrs A
  where [O1]='EBITDA'
  )

  ,AllAttrs as (
    select * from DrillProd
    union all select * from DrillFleetCostLevers
    union all select * from DrillOperationsCostLevers
    union all select * from DrillSummary
    union all select * from DrillStock
    union all select * from Blast
	union all select * from LoadHaul
	union all select * from MineServicesAndOverheads
	union all select * from ProcessingPlants
	union all select * from ProcessingPlantCosts
	union all select * from ProcessingNonAllocatedAndOverheads
	union all select * from ProcessingTopLevel
	union all select * from MinesOverheads
	union all select * from MinesTopLevel
	union all select * from OverheadsMines
	union all select * from RailVolume
	union all select * from RailProdCosts
	union all select * from RailOverheads
	union all select * from RailMaintenance
	union all select * from RailTopLevel
	union all select * from PortInflow
	union all select * from PortOutflow
	union all select * from PortProductionCosts
	union all select * from PortSiteMaintenance
	union all select * from PortMaintenance
	union all select * from PortMarine
	union all select * from PortOverheads
	union all select * from PortTopLevel
	union all select * from FunctionsCosts
	union all select * from Revenue
  )

  select * into #qerent_attrs_raw from AllAttrs;

  -- Update category values for each category
  with Names as (select distinct org_group from #qerent_attrs_raw)
  insert into vdt.org_group (og_name) select org_group from Names
  where not exists (select * from vdt.org_group where og_name=Names.org_group)
  and Names.org_group is not null;

  with Names as (select distinct [function] from #qerent_attrs_raw)
  insert into vdt.[function] (func_name) select [function] from Names
  where not exists (select * from vdt.[function] where func_name=Names.[function])
  and Names.[function] is not null;

  with Names as (select distinct activity from #qerent_attrs_raw)
  insert into vdt.activity (act_name) select activity from Names
  where not exists (select * from vdt.activity where act_name=Names.activity)
  and Names.activity is not null;

  with Names as (select distinct workstream from #qerent_attrs_raw)
  insert into vdt.workstream (ws_name) select workstream from Names
  where not exists (select * from vdt.workstream where ws_name=Names.workstream)
  and Names.workstream is not null;

  with Names as (select distinct location from #qerent_attrs_raw)
  insert into vdt.location (loc_name) select location from Names
  where not exists (select * from vdt.location where loc_name=Names.location)
  and Names.location is not null;

  with Names as (select distinct equipment_type from #qerent_attrs_raw)
  insert into vdt.eqp_type (eqp_type_name) select equipment_type from Names
  where not exists (select * from vdt.eqp_type where eqp_type_name=Names.equipment_type)
  and Names.equipment_type is not null;

  with Names as (select distinct equipment from #qerent_attrs_raw)
  insert into vdt.equipment (eqp_name) select equipment from Names
  where not exists (select * from vdt.equipment where eqp_name=Names.equipment)
  and Names.equipment is not null;

  with Names as (select distinct cost_type from #qerent_attrs_raw)
  insert into vdt.cost_type (ct_name) select cost_type from Names
  where not exists (select * from vdt.cost_type where ct_name=Names.cost_type)
  and Names.cost_type is not null;

  with Names as (select distinct product from #qerent_attrs_raw)
  insert into vdt.product (prod_name) select product from Names
  where not exists (select * from vdt.product where prod_name=Names.product)
  and Names.product is not null;
  

  -- Join back to the qerent attrs to get the category IDs
  with WithIds as (
	select I.name as attr_name, I.unit, I.sec_level, I.is_cost, I.is_lever, I.is_calculated, I.is_aggregate, I.is_kpi,
	  O.og_id, F.func_id, A.act_id, W.ws_id, L.loc_id, ET.eqp_type_id, E.eqp_id, C.ct_id, P.prod_id,
	  I.value, I.path as qerent_path, I.attribute as qerent_attribute, D.QerentAttrId as qerent_attr_id
	from #qerent_attrs_raw I
	inner join @data D on I.[path]=D.[Object Path] and I.attribute=D.Attribute
	left join vdt.org_group O on I.org_group=O.og_name
	left join vdt.[function] F on I.[function]=F.[func_name]
	left join vdt.activity A on I.activity=A.act_name
	left join vdt.workstream W on I.workstream=W.ws_name
	left join vdt.location L on I.location=L.loc_name
	left join vdt.eqp_type ET on I.equipment_type=ET.eqp_type_name
	left join vdt.equipment E on I.equipment=E.eqp_name
	left join vdt.cost_type C on I.cost_type=C.ct_name
	left join vdt.product P on I.product=P.prod_name
  )
  
  select * into #qerent_attrs from WithIds;

  -- Udate is_cost etc on existing attributes
  update vdt.attribute set unit=Q.unit, sec_level=Q.sec_level, is_cost=Q.is_cost, is_lever=Q.is_lever, is_calculated=Q.is_calculated,
                           is_aggregate=Q.is_aggregate, is_kpi=Q.is_kpi
  from #qerent_attrs Q inner join staging.qerent_attr_ids I on Q.qerent_path=I.object_path and Q.qerent_attribute=I.attr_name
  where attribute.attr_id=I.attr_id --and I.model_id=@model_id

  -- Add any new attributes to the database
  insert into vdt.attribute (attr_name, unit, sec_level, is_cost, is_lever, is_calculated, is_aggregate, is_kpi, og_id, func_id, act_id, ws_id, loc_id, eqp_type_id, eqp_id, ct_id, prod_id)
  select attr_name, coalesce(unit, ''), sec_level, is_cost, is_lever, is_calculated, is_aggregate, is_kpi, og_id, func_id, act_id, ws_id, loc_id, eqp_type_id, eqp_id, ct_id, prod_id
  from #qerent_attrs Q
  where not exists (select * from staging.qerent_attr_ids E where E.object_path=Q.qerent_path and E.attr_name=Q.qerent_attribute /*and E.model_id=@model_id*/);

  -- Update existing qerent_ids in the staging map (these could change due to a scale-out, etc.)
  update staging.qerent_attr_ids set qerent_attr_id=Q.qerent_attr_id
  from #qerent_attrs Q where qerent_attr_ids.object_path=Q.qerent_path and qerent_attr_ids.attr_name=Q.qerent_attribute;
  
  -- Add new attributes to the qerent_attr_ids map
  insert into staging.qerent_attr_ids (model_id, attr_id, object_path, attr_name, qerent_attr_id)
  select @model_id, A.attr_id, Q.qerent_path, Q.qerent_attribute, Q.qerent_attr_id
  from #qerent_attrs Q inner join vdt.attribute A
  on Q.attr_name=A.attr_name
  and coalesce(Q.og_id, -1)=coalesce(A.og_id, -1)
  and coalesce(Q.func_id, -1)=coalesce(A.func_id, -1)
  and coalesce(Q.act_id, -1)=coalesce(A.act_id, -1)
  and coalesce(Q.ws_id, -1)=coalesce(A.ws_id, -1)
  and coalesce(Q.loc_id, -1)=coalesce(A.loc_id, -1)
  and coalesce(Q.eqp_type_id, -1)=coalesce(A.eqp_type_id, -1)
  and coalesce(Q.eqp_id, -1)=coalesce(A.eqp_id, -1)
  and coalesce(Q.ct_id, -1)=coalesce(A.ct_id, -1)
  and coalesce(Q.prod_id, -1)=coalesce(A.prod_id, -1)
  and not exists (select * from staging.qerent_attr_ids X where object_path=Q.qerent_path and attr_name=Q.qerent_attribute)

  -- Overwrite the dependency map for any attributes that were listed in @data
  -- NB we are relying on the script ensuring that attributes are in the @deps map if and only if they're in the @data table
  delete from vdt.attribute_dependency where attr_id in (select A.attr_id
  from staging.qerent_attr_ids A inner join @data D on A.object_path=D.[Object Path] and A.attr_name=D.Attribute);

  insert into vdt.attribute_dependency (attr_id, depends_on_attr_id)
  select X.attr_id, Y.attr_id
  from @deps D
  inner join staging.qerent_attr_ids X on D.QerentAttrId=X.qerent_attr_id
  inner join staging.qerent_attr_ids Y on D.DependsOnQerentAttrId=Y.qerent_attr_id
  where X.attr_id <> Y.attr_id;

  -- Copy data over from the model
  declare @dataset_id int = null;
  select @dataset_id = dataset_id from vdt.dataset where origin='model' and dataset_name='Model Data - ' + @filename;
  if @dataset_id is null
  begin
    declare @cat_id int = (select category_id from vdt.dataset_category where category_name='Scenario');
	insert into vdt.dataset (model_id, dataset_name, origin, category_id, last_update)
	values (@model_id, 'Model Data - ' + @filename, 'model', @cat_id, current_timestamp);
	set @dataset_id = SCOPE_IDENTITY();
  end
  if @dataset_id is not null
  begin
    update vdt.dataset set model_id=@model_id where dataset_id=@dataset_id;
    delete from vdt.value where dataset_id=@dataset_id;
	insert into vdt.value (dataset_id, attr_id, value)
	select @dataset_id, I.attr_id, Q.value from staging.qerent_attr_ids I
	inner join #qerent_attrs Q on I.[object_path] = Q.[qerent_path] and I.[attr_name] = Q.[qerent_attribute] --and I.model_id=@model_id;
  end
  
  -- NB the Qerent Attr Id for the top of tree is not the same as the database attr_id...
  declare @top_of_tree_attr_id int = (select attr_id from staging.qerent_attr_ids where qerent_attr_id=@top_of_tree_qattr_id);

  update vdt.model set
    top_of_tree_attr_id = @top_of_tree_attr_id,
	[filename] = @filename
  where model_id = @model_id

  drop table #qerent_attrs;
  drop table #qerent_attrs_raw;
end

GO

create type staging.QerentSensitivityType as table (
  [Object Path] nvarchar(256) not null,
  [Attribute] nvarchar(100) not null,
  [Impact of Increase] float not null,
  [Impact of Decrease] float not null
);

go

create procedure staging.sp_import_sensitivity (
  @dataset_id int,
  @pc_change float,
  @target_object_path nvarchar(256),
  @target_attr nvarchar(100),
  @target_base_value float,
  @data staging.QerentSensitivityType readonly)
as
begin
  set nocount on

  declare @target_attr_id int
  select @target_attr_id=attr_id from staging.qerent_attr_ids
  where object_path=@target_object_path and attr_name=@target_attr
  if @target_attr_id is null
  begin
    raiserror (N'Invalid attribute %s[%s]', 10, 1, @target_object_path, @target_attr)
	return
  end
  
  declare @model_id int
  select @model_id=model_id from vdt.dataset where dataset_id=@dataset_id

  declare @sensitivity_id int
  select @sensitivity_id=sensitivity_id from vdt.sensitivity
  where dataset_id=@dataset_id and target_attr_id=@target_attr_id
  if @sensitivity_id is null
  begin
    insert into vdt.sensitivity (dataset_id, percent_change, target_attr_id, target_base_value, last_update)
	values (@dataset_id, @pc_change, @target_attr_id, @target_base_value, CURRENT_TIMESTAMP)
	set @sensitivity_id=SCOPE_IDENTITY()
  end;

  delete from vdt.sensitivity_value where sensitivity_id=@sensitivity_id;

  insert into vdt.sensitivity_value (sensitivity_id, attr_id, impact_of_increase, impact_of_decrease)
  select @sensitivity_id, Q.attr_id, D.[Impact of Increase] as impact_of_increase, D.[Impact of Decrease] as impact_of_decrease
  from @data D inner join staging.qerent_attr_ids Q on D.[Object Path]=Q.object_path and D.Attribute=Q.attr_name; --and Q.model_id=@model_id;

  update vdt.sensitivity set target_base_value=@target_base_value, last_update=CURRENT_TIMESTAMP where sensitivity_id=@sensitivity_id;
end
go


create type staging.QerentAttributionType as table (
  [Object Path] nvarchar(256) not null,
  [Attribute] nvarchar(100) not null,
  [Sequence] int not null,
  [Impact on Target] float not null
);

go

create procedure staging.sp_import_attribution (
  @base_dataset_id int,
  @benchmark_dataset_id int,
  @is_cumulative bit,
  @target_object_path nvarchar(256),
  @target_attr nvarchar(100),
  @target_base_value float,
  @data staging.QerentAttributionType readonly)
as
begin
  set nocount on

  declare @target_attr_id int
  select @target_attr_id=attr_id from staging.qerent_attr_ids
  where object_path=@target_object_path and attr_name=@target_attr
  if @target_attr_id is null
  begin
    raiserror (N'Invalid attribute %s[%s]', 10, 1, @target_object_path, @target_attr)
	return
  end
  
  declare @model_id int
  select @model_id=model_id from vdt.dataset where dataset_id=@base_dataset_id

  declare @attribution_id int
  select @attribution_id=attribution_id from vdt.attribution
  where base_dataset_id=@base_dataset_id and benchmark_dataset_id=@benchmark_dataset_id
  and target_attr_id=@target_attr_id and is_cumulative=@is_cumulative
  if @attribution_id is null
  begin
    insert into vdt.attribution (base_dataset_id, benchmark_dataset_id, is_cumulative, target_attr_id, target_base_value, last_update)
	values (@base_dataset_id, @benchmark_dataset_id, @is_cumulative, @target_attr_id, @target_base_value, CURRENT_TIMESTAMP)
	set @attribution_id=SCOPE_IDENTITY()
  end;

  delete from vdt.attribution_value where attribution_id=@attribution_id;

  insert into vdt.attribution_value (attribution_id, attr_id, impact_on_target, seq_number)
  select @attribution_id, Q.attr_id, D.[Impact on Target] as impact_on_target, D.[Sequence] as seq_number
  from @data D inner join staging.qerent_attr_ids Q on D.[Object Path]=Q.object_path and D.Attribute=Q.attr_name; --and Q.model_id=@model_id;

  update vdt.attribution set target_base_value=@target_base_value, last_update=CURRENT_TIMESTAMP where attribution_id=@attribution_id;
end
go


/*
create view [staging].[import_qerent_ids] as
select I.name as attr_name, I.unit, I.sec_level, I.is_cost, I.is_lever, I.is_aggregate, I.is_kpi,
  O.og_id, F.func_id, A.act_id, W.ws_id, L.loc_id, ET.eqp_type_id, E.eqp_id, C.ct_id, P.prod_id,
  I.value, I.path as qerent_path, I.attribute as qerent_attribute
from staging.import_qerent I
left join vdt.org_group O on I.org_group=O.og_name
left join vdt.[function] F on I.[function]=F.[func_name]
left join vdt.activity A on I.activity=A.act_name
left join vdt.workstream W on I.workstream=W.ws_name
left join vdt.location L on I.location=L.loc_name
left join vdt.eqp_type ET on I.equipment_type=ET.eqp_type_name
left join vdt.equipment E on I.equipment=E.eqp_name
left join vdt.cost_type C on I.cost_type=C.ct_name
left join vdt.product P on I.product=P.prod_name
go

create procedure staging.sync_qerent_attrs as
begin
insert into vdt.attribute (attr_name, unit, sec_level, is_cost, is_lever, is_aggregate, is_kpi, og_id, func_id, act_id, ws_id, loc_id, eqp_type_id, eqp_id, ct_id, prod_id)
select attr_name, coalesce(unit, ''), sec_level, is_cost, is_lever, is_aggregate, is_kpi, og_id, func_id, act_id, ws_id, loc_id, eqp_type_id, eqp_id, ct_id, prod_id
from staging.import_qerent_ids Q
where not exists (select * from staging.qerent_attr_ids E where E.object_path=Q.qerent_path and E.attr_name=Q.qerent_attribute);

insert into staging.qerent_attr_ids (attr_id, object_path, attr_name)
select A.attr_id, Q.qerent_path, Q.qerent_attribute
from vdt.attribute A inner join staging.import_qerent_ids Q
on A.attr_name=Q.attr_name and A.is_cost=Q.is_cost and A.is_lever=Q.is_lever and A.is_aggregate=Q.is_aggregate and A.is_kpi=Q.is_kpi
and coalesce(A.og_id, -1)=coalesce(Q.act_id, -1)
and coalesce(A.func_id, -1)=coalesce(Q.func_id, -1)
and coalesce(A.act_id, -1)=coalesce(Q.act_id, -1)
and coalesce(A.ws_id, -1)=coalesce(Q.ws_id, -1)
and coalesce(A.loc_id, -1)=coalesce(Q.loc_id, -1)
and coalesce(A.eqp_type_id, -1)=coalesce(Q.eqp_type_id, -1)
and coalesce(A.eqp_id, -1)=coalesce(Q.eqp_id, -1)
and coalesce(A.ct_id, -1)=coalesce(Q.ct_id, -1)
and coalesce(A.prod_id, -1)=coalesce(Q.prod_id, -1)
where not exists (select * from staging.qerent_attr_ids E where E.object_path=Q.qerent_path and E.attr_name=Q.qerent_attribute);
end
*/

CREATE CLUSTERED INDEX [_dta_index_qerent_attr_ids_c_50_1799677459__K2_K3_K4_9987] ON [staging].[qerent_attr_ids]
(
                [attr_id] ASC,
                [object_path] ASC,
                [attr_name] ASC
)WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [_dta_index_qerent_attr_ids_50_1799677459__K4_K3_1_2] ON [staging].[qerent_attr_ids]
(
                [attr_name] ASC,
                [object_path] ASC
)
INCLUDE ([model_id],
                [attr_id]) WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [_dta_index_value_50_1335675806__K2_K1_3_1410] ON [vdt].[value]
(
                [attr_id] ASC,
                [dataset_id] ASC
)
INCLUDE ([value]) WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO





