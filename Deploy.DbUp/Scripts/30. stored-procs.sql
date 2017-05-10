--USE *database_name*

create procedure vdt.sp_get_unit_formats
as
begin
  set nocount on;

  select unit, [format] from vdt.unit_format;
end
go

create procedure vdt.sp_get_sites
  @user_id int
as
begin
  set nocount on;

  declare @locations table (bucket int, location nvarchar(100));
  insert into @locations (bucket, location)
  select distinct 1 as bucket, location from vdt.data
  where org_group = 'Mines' and location is not null
  union all select 2 as bucket, 'Rail' as location
  union all select 3 as bucket, 'Port' as location
  union all select 4 as bucket, 'WAIO' as location
  order by bucket, location
  
  select location from @locations
end
go

create procedure vdt.sp_get_site_filters
  @site nvarchar(100)
as
begin
  set nocount on;

  if exists (select * from vdt.data where org_group='Mines' and location=@site)
  begin
    -- it's a mining location: activity, workstream, equipment, cost type, attribute
	with attributes as (
	  select * from vdt.attribute_category
	  where org_group='Mines' and location=@site and activity <> 'Overheads' and [function] <> 'Overheads'
	  and (attribute_is_kpi<>0 or attribute_is_lever<>0))
	
	select distinct 'Activity' as filter, 'act_id' as colname, cast(act_id as nvarchar) as id, activity as value
	from attributes where activity is not null
	union all
	select distinct 'Workstream' as filter, 'ws_id' as colname, cast(ws_id as nvarchar) as id, workstream as value
	from attributes where workstream is not null
	union all
	select distinct 'Equipment' as filter, 'eqp_id' as colname, cast(eqp_id as nvarchar) as id, equipment as value 
	from attributes where equipment is not null and attribute_is_cost=0
	union all
	select distinct 'Cost Type' as filter, 'ct_id' as colname, cast(ct_id as nvarchar) as id, cost_type as value 
	from attributes where cost_type is not null
	union all
	select distinct 'Attribute' as filter, 'attribute' as colname, attribute as id, attribute as value 
	from attributes
  end
  else
  begin
    -- it's rail, port, or functions
	with attributes as (
	  select * from vdt.attribute_category
	  where org_group=@site and activity <> 'Overheads' and [function] <> 'Overheads'
	  and (attribute_is_kpi<>0 or attribute_is_lever<>0))
	
	--select distinct 'Location' as filter, 'loc_id' as colname, cast(loc_id as nvarchar) as id, location as value
	--from attributes where location is not null
	--union all
	select distinct 'Activity' as filter, 'act_id' as colname, cast(act_id as nvarchar) as id, activity as value
	from attributes where activity is not null
	union all
	select distinct 'Workstream' as filter, 'ws_id' as colname, cast(ws_id as nvarchar) as id, workstream as value
	from attributes where workstream is not null
	union all
	select distinct 'Equipment' as filter, 'eqp_id' as colname, cast(eqp_id as nvarchar) as id, equipment as value 
	from attributes where equipment is not null
	union all
	select distinct 'Cost Type' as filter, 'ct_id' as colname, cast(ct_id as nvarchar) as id, cost_type as value 
	from attributes where cost_type is not null
	union all
	select distinct 'Attribute' as filter, 'attribute' as colname, attribute as id, attribute as value 
	from attributes
  end
end
go

create procedure [vdt].[sp_get_site_data]
  @site nvarchar(100),
  @dataset_id int,
  @user_id int
as
begin
  set nocount on;

  if exists (select * from vdt.data where org_group='Mines' and location=@site)
  begin
    -- it's a mining location
	with allattrs as (
	select 
	  coalesce([function], 'Mining') as tab_name, coalesce([activity], [function], 'Mining') as panel, 'ActivityTemplate' as template,
	  org_group, [function], activity, workstream, location, equipment_type, equipment, cost_type, product, attr_id, attribute, attribute_unit,
	  attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	from vdt.data where org_group='Mines' and location=@site and dataset_id=@dataset_id
	and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
    and ([function] is not null or attribute_is_kpi=0) and (attribute_is_kpi=1 or attribute_is_lever=1))
	,customattrs as (
	select [function] as tab_name, [activity] as panel, 'ActivityTemplate' as template,
	  org_group, [function], activity, workstream, location, equipment_type, 'Operations Costs' as equipment, cost_type, product, attr_id, attribute, attribute_unit,
	  attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	from allattrs where [activity]='Blast' and attribute_is_cost=1
	union all
	select [function] as tab_name, [activity] as panel, 'ActivityTemplate' as template,
	  org_group, [function], activity, workstream, location, equipment_type, null as equipment, cost_type, product, attr_id, attribute, attribute_unit,
	  attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	from allattrs where [activity]='Load & Haul' and attribute_is_cost=1
    union all
    select tab_name, panel, 'DrillDownTemplate' as template,
      org_group, [function], activity, workstream, null as location, case when equipment_type=cost_type then null else equipment_type end as equipment_type,
	  equipment, cost_type, product, attr_id, attribute, attribute_unit,
      attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
    from allattrs where [activity] in ('Overheads', 'Non Allocated Cost') or [function] = 'Overheads'
	)
	,unioned as (
	select * from customattrs
	union all
	select * from allattrs where attr_id not in (select attr_id from customattrs))

	select * from unioned
	where tab_name is not null and panel is not null
	order by
	case coalesce([function], 'Mining')
	  when 'Mining' then 1
	  when 'Processing' then 2
	  else 3
	end,
	case coalesce([activity], 'Mining')
	  when 'Mining' then 1
	  when 'Drill' then 10
	  when 'Blast' then 20
	  when 'Load & haul' then 30
	  when 'Load' then 40
	  when 'Haul' then 50
	  when 'Ore Sources' then 60
	  when 'Beneficiation' then 70
	  when 'OFH' then 80
	  when 'OFR' then 90
	  when 'TLO' then 100
	  else 999
	end, [function], [activity]
  end
  else if @site='Rail'
  begin
    with allattrs as (
	  select 
	    [function] as tab_name, [activity] as panel, 'ActivityTemplate' as template,
	    org_group, [function], activity, workstream, location, equipment_type, equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from vdt.data where org_group=@site and dataset_id=@dataset_id
	  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
	  and (attribute_is_kpi=1 or attribute_is_lever=1)
	)
	,customattrs as (
	  select 
	    [function] as tab_name, [activity] as panel, 'ActivityTemplate' as template,
	    org_group, [function], activity, workstream, location, equipment_type,
		case when location like 'CD%' then 'Car Dumpers' else location end as equipment,
		cost_type, product, attr_id,
		case when location like 'CD%' then location + ' ' + attribute else attribute end as attribute,
		attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from allattrs where [activity]='Rail'
	  union all
	  select 
	    [function] as tab_name, [activity] as panel, 'ActivityTemplate' as template,
	    org_group, [function], activity, workstream, location, equipment_type, coalesce(equipment, equipment_type, workstream) as equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from vdt.data where org_group=@site and dataset_id=@dataset_id
	  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
	  and (attribute_is_kpi=1 or attribute_is_lever=1)
	  and [function]='Maintenance'
	  union all
	  select 
	    [function] as tab_name, [activity] as panel, 'DrillDownTemplate' as template,
	    org_group, [function], activity, workstream, location, equipment_type, equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from vdt.data where org_group=@site and dataset_id=@dataset_id
	  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
	  and (attribute_is_kpi=1 or attribute_is_lever=1)
	  and [function]='Production'
	)
	,unioned as (
	select * from customattrs
	union all
	select * from allattrs where attr_id not in (select attr_id from customattrs))

	select * from unioned
	where tab_name is not null and panel is not null
	order by
	case coalesce([function], 'Rail')
	  when 'Mainline' then 1
	  when 'Shuttle' then 2
	  else 3
	end, [function], [activity]
  end
  else if @site='Port'
  begin
    with allattrs as (
	  select 
	    [function] as tab_name, coalesce([activity], [function]) as panel, 'ActivityTemplate' as template,
	    org_group, [function], activity, workstream, location, equipment_type, equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from vdt.data where org_group=@site and dataset_id=@dataset_id
	  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
	  and (attribute_is_lever=1 or attribute_is_kpi=1)
	)
	,customattrs as (
	  select 
	    [function] as tab_name, [location] as panel, 'ActivityTemplate' as template,
	    org_group, [function], activity, workstream, location, equipment_type,
		equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from allattrs where [activity]='Outflow'
	  union all
	  select 
	    [function] as tab_name, [activity] as panel, 'ActivityTemplate' as template,
	    org_group, [function], activity, workstream, location, equipment_type,
		workstream as equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from allattrs where [activity] in ('Overheads') and [function] in ('Production', 'Marine')
	  union all
	  select 
	    [function] as tab_name, [activity] as panel, 'ActivityTemplate' as template,
	    org_group, [function], activity, workstream, location, equipment_type,
		location as equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from allattrs where [activity] in ('Operations') and [function]='Production'
	  union all
	  select 
	    [function] as tab_name, [activity] as panel, 'ActivityTemplate' as template,
	    org_group, [function], activity, workstream, location, equipment_type,
		equipment_type as equipment, attribute as cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from allattrs where [activity] in ('Demurrage') and [function]='Marine'
	  union all
	  select 
	    [function] as tab_name, [location] as panel, 'DrillDownTemplate' as template,
	    org_group, [function], null as activity, null as workstream, null as location, equipment_type,
		equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from allattrs where [activity]='Maintenance' and [function]='Maintenance'
	  union all
	  select 
	    [function] as tab_name, [location] as panel, 'DrillDownTemplate' as template,
	    org_group, [function], null as activity, activity as workstream, workstream as location, equipment_type,
		equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from allattrs where [activity]='Overheads' and location is not null and [function]='Maintenance'
	  union all
	  select 
	    [function] as tab_name, activity as panel, 'DrillDownTemplate' as template,
	    org_group, [function], null as activity, null as workstream, null as location, equipment_type,
		equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from allattrs where workstream='Operations' and location is null and [function]='Maintenance'
	  union all
	  select 
	    [function] as tab_name, [activity] as panel, 'DrillDownTemplate' as template,
	    org_group, [function], activity, case workstream when 'Maintenance' then null else 'Overheads' end as workstream, workstream as location, equipment_type,
		equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from allattrs where [activity]<>'Maintenance' and not ([activity]='Overheads' and location is not null) and not (workstream='Operations' and location is null)
	    and [function]='Maintenance' and [activity]='Shutdown'
	  union all
	  select 
	    [function] as tab_name, activity as panel, 'DrillDownTemplate' as template,
	    org_group, [function], null as activity,
		case when workstream is null or workstream='Operations' then null else 'Overheads' end as workstream,
		case when workstream is null then 'Other' else coalesce(location, workstream) end as location, equipment_type,
		equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from allattrs where activity='Site Infrastructure' and [function]='Maintenance'
	  union all
	  select 
	    [function] as tab_name, [activity] as panel, 'DrillDownTemplate' as template,
	    org_group, [function], activity, workstream, location, equipment_type,
		equipment, cost_type, product, attr_id, attribute, attribute_unit,
	    attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	  from allattrs where [activity]<>'Maintenance' and not ([activity]='Overheads' and location is not null) and not (workstream='Operations' and location is null)
	    and [function]='Maintenance' and [activity]<>'Shutdown' and [activity]<>'Site Infrastructure'
	)
	,unioned as (
	select * from customattrs
	union all
	select * from allattrs where attr_id not in (select attr_id from customattrs))

	select * from unioned
	where tab_name is not null and panel is not null
	order by
	case coalesce([function], 'Rail')
	  when 'Production' then 1
	  when 'Marine' then 2
	  when 'Maintenance' then 3
	  else 4
	end,
	case [panel]
	  when 'Inflow' then 1
	  when 'North Yard' then 10
	  when 'South Yard' then 20
	  when 'East Yard' then 30
	  when 'West Yard' then 40
	  else 999
	end, [function], [activity]
  end
  else
  begin
    -- it's functions
	with AllAttrs as (
	select 
      [function] as tab_name, [activity] as panel, 'DrillDownTemplate' as template,
	  org_group, [function], activity, workstream, location, equipment_type, equipment,
	  case when attribute_is_cost=1 then coalesce(cost_type, attribute) else cost_type end as cost_type,
	  product, attr_id, attribute, attribute_unit,
	  attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	from vdt.data where org_group=@site and dataset_id=@dataset_id
	and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
	),
	unioned as (
	  select * from AllAttrs
	)
	select * from unioned
	where tab_name in ('Functions', 'Logistic & Infrastructure', 'Operations Infrastructure', 'Other', 'Revenue')
	and panel is not null and panel <> 'Aggregate Fixed Cost'
  end
end
GO

create procedure [vdt].[sp_get_activities]
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT act_id, act_name
  FROM [vdt].[activity]

END
GO

create procedure [vdt].[sp_get_attributes]
@user_id int
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT attr_id, attr_name
  FROM [vdt].[attribute]
  WHERE (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  sec_level

END 


GO


create procedure [vdt].[sp_get_cost_types]
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT ct_id, ct_name
  FROM [vdt].[cost_type]

END 

GO


create procedure [vdt].[sp_get_data_by_dataset_and_site]
@dataset_id int  , 
@site_name nvarchar(100),
@user_id int   
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT [dataset]
,[dataset_id]
	,[scenario_id]
	,[attr_id]
      ,[org_group]
      ,[function]
      ,[activity]
      ,[workstream]
      ,[location]
      ,[equipment_type]
      ,[equipment]
      ,[cost_type]
      ,[product]
	  ,[attribute]
      ,[attribute_unit]
      ,[attribute_sec_level]
      ,[attribute_is_cost]
      ,[attribute_is_lever]
      ,[attribute_is_aggregate]
      ,[attribute_is_kpi]
      ,[value]
      ,[base_value]
      ,[override_value]
  FROM [vdt].[data]
  WHERE dataset_id = @dataset_id
  AND location = @site_name
  AND (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level

END 


GO


create procedure [vdt].[sp_get_equipment]
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT eqp_id,eqp_name
  FROM [vdt].[equipment]

END 

GO


create procedure [vdt].[sp_get_functions]
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT func_id,func_name
  FROM [vdt].[function]

END 

GO


create procedure [vdt].[sp_get_locations]
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT loc_id, loc_name
  FROM [vdt].[location]

END

GO

create procedure [vdt].[sp_get_user_by_id]
       @user_id int               
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT [user_id]
      ,[login]
      ,[ad_name]
      ,[pwd_hash]
  FROM [vdt].[user]
  where [user_id] = @user_id

END 

GO


create procedure [vdt].[sp_get_user_by_name]
       @login nvarchar(128)                
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT [user_id]
      ,[login]
      ,[ad_name]
	  ,[sec_level]
      ,[pwd_hash]
	  ,[is_power_user]
	  ,[is_admin_user]
  FROM [vdt].[user]
  where [login] = @login

END 

GO


create procedure [vdt].[sp_get_workstreams]
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT ws_id, ws_name
  FROM [vdt].[workstream]

END 

GO


create procedure [vdt].[sp_insert_user]
       @login VARCHAR(15)= NULL, 
       @ad_name NVARCHAR(128)= NULL, 
       @pwd_hash NVARCHAR(64)= NULL,
	   @sec_level int,
	   @is_power_user  bit,
	   @is_admin_user  bit                    
AS 
BEGIN 
     SET NOCOUNT ON 

     INSERT INTO vdt.[user]
          ( 
            [login],
            ad_name,
            pwd_hash,
			sec_level,
			is_power_user,
			is_admin_user         
          ) 
     VALUES 
          ( 
            @login,
            @ad_name,
            @pwd_hash,
			@sec_level,
			@is_power_user,
			@is_admin_user                
          ) 

END 


GO

CREATE TYPE [vdt].[ScenarioTableType] AS TABLE(
	[scenario_id] [int] NULL,
	[attr_id] [int] NULL,
	[new_value] [float] NULL,
	[old_value] [float] NULL
)
GO

GO

CREATE TYPE [vdt].[FilterTableType] AS TABLE(
	[item] [nvarchar](100) NULL
)
GO

create procedure [vdt].[sp_update_scenario_value]
 @stt ScenarioTableType READONLY, 
 @user_id int
AS 

BEGIN 
     SET NOCOUNT ON 


   INSERT INTO [vdt].[scenario_change]  
           (scenario_id, attr_id ,new_value) 
        SELECT scenario_id, attr_id,new_value
        FROM  @stt s1 where NOT EXISTS(select * from [vdt].[scenario_change] s2 where s1.scenario_id = s2.scenario_id AND s1.attr_id = s2.attr_id) ;  

	UPDATE
		[vdt].[scenario_change] 
	SET
		new_value = change.new_value
	FROM
	   @stt change
	INNER JOIN
		[vdt].[scenario_change]  original
	ON 
		original.scenario_id = change.scenario_id AND original.attr_id = change.attr_id;

	INSERT INTO  [vdt].[scenario_change_log] (scenario_id, attr_id,new_value ,old_value,[user_id],change_date) 
	  SELECT scenario_id, attr_id,new_value,old_value,@user_id,GETDATE()
			FROM  @stt
END 


GO

create procedure [vdt].[sp_get_base_dataset_id]
@view_dataset_id int 
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT 
 TOP 1 [base_dataset_id] FROM 
 [vdt].[scenario]
  WHERE [view_dataset_id] = @view_dataset_id;

END 


GO

create procedure [vdt].[sp_get_datasets]
   @user_id int
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT Dataset.[dataset_id]
		 ,Dataset.[dataset_name]
		 ,Category.[category_name]
		 ,[Data].value as ebitda
		 ,[Format].[format] as ebitda_unit
         ,CASE WHEN Model.is_active = 1 THEN [LockUser].[login] ELSE NULL END as locked_by
		 ,CASE WHEN Model.is_active = 1 THEN [LockUser].[user_id] ELSE NULL END as locked_by_user_id
         ,[BaseDataset].dataset_name as base_dataset_name
		 ,[BaseDataset].dataset_id as base_dataset_id
         ,coalesce([OwnerUser].[login] ,'System')as owned_by
         ,[OwnerUser].[user_id] as owned_by_id
		 ,Dataset.[last_update]
		 ,Dataset.[description]
		 ,Scenario.scenario_id
         ,coalesce(Scenario.[is_public],'True') as is_public
         ,CASE WHEN Scenario.scenario_id IS NULL THEN 0 ELSE 1 END as is_whatif
		 ,CASE WHEN is_public = 1 OR is_public IS NULL THEN 'Public' ELSE 'Private' END as access_level
		  ,Model.is_active
  FROM [vdt].[dataset] Dataset
  inner join vdt.model Model on Dataset.model_id=Model.model_id
  left join vdt.scenario Scenario on Scenario.view_dataset_id = Dataset.dataset_id
  left join vdt.[dataset] [BaseDataset] on BaseDataset.dataset_id =  Scenario.base_dataset_id
  left join vdt.[user] [LockUser] on Scenario.editing_by = [LockUser].[user_id]
  left join vdt.[user] [OwnerUser] on Scenario.created_by = [OwnerUser].[user_id]
  left join vdt.[data] [Data] on [Data].dataset_id = [Dataset].dataset_id 
  left join vdt.[unit_format] [Format] on [Format].unit = [Data].attribute_unit 
  left join vdt.[dataset_category] [Category] on [Dataset].category_id = [Category].category_id 
    WHERE Dataset.deleted = 0
  AND [Data].attr_id = Model.top_of_tree_attr_id
  AND (
  coalesce(Scenario.[is_public],'True') = 1
  OR ( is_public = 0 AND ((select is_power_user from vdt.[user] where [user_id] = @user_id) = 1) 
  OR Scenario.created_by = @user_id)
  )

END 

GO


create procedure [vdt].[sp_copy_dataset]
   @base_dataset_id int,
   @dataset_name nvarchar(100),
   @description nvarchar(100),
   @user_id int,
   @category_id int,
   @is_public bit
AS 


declare @model_id int;
declare @dataset_id int;
select @model_id =model_id from VDT.dataset where dataset_id = @base_dataset_id

BEGIN 
     SET NOCOUNT ON 

INSERT into VDT.dataset (model_id, dataset_name,origin,last_update,[description],created,deleted,category_id)
values(@model_id,@dataset_name,'user',GETDATE(),@description,GETDATE(),0,@category_id)

select @dataset_id= CAST(scope_identity() AS int)

INSERT into VDT.value 
(dataset_id, attr_id, value) 
select @dataset_id, attr_id, value from VDT.data where dataset_id = @base_dataset_id


INSERT into VDT.scenario 
(model_id, base_dataset_id, view_dataset_id,scenario_name,created_by,editing_by,is_public) 

values (@model_id,@base_dataset_id,@dataset_id,@dataset_name,@user_id,null,@is_public)

END  


GO


create procedure [vdt].[sp_get_data_by_dataset_and_site_filtered]
@dataset_id int, 
@site_name nvarchar(100) ,
@functions FilterTableType READONLY,
@activities FilterTableType READONLY,
@workstreams FilterTableType READONLY,
@equipment FilterTableType READONLY,
@costtypes FilterTableType READONLY,
@attributes FilterTableType READONLY,
@user_id int
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT[scenario_id]
	,[attr_id]
	,[activity]
    ,[equipment]
    ,[cost_type]
	,[attribute]
    ,[attribute_unit]
    ,[attribute_sec_level]
    ,[attribute_is_cost]
    ,[attribute_is_lever]
    ,[attribute_is_kpi]
    ,[base_value]
    ,coalesce([override_value],[base_value]) as override_value
	,CASE WHEN [override_value] IS NULL THEN 'False' ELSE 'True' END AS is_overriden
  FROM [vdt].[data]
  WHERE dataset_id = @dataset_id
  AND location = @site_name
  AND ([function] in (select item from @functions) OR [function] IS NULL)
  AND ([activity] in (select item from @activities) OR [activity] IS NULL)
  AND ([workstream] in (select item from @workstreams) OR [workstream] IS NULL)
  AND ([equipment] in (select item from @equipment) OR [equipment] IS NULL)
  AND ([cost_type] in (select item from @costtypes) OR [cost_type] IS NULL)
  AND ([attribute] in (select item from @attributes) OR [attribute] IS NULL)
  AND (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
END 


GO

create procedure [vdt].[sp_get_override_values_by_dataset_and_site]
@dataset_id int  , 
@site_name nvarchar(100),
@user_id int
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT [attr_id], value
  FROM [vdt].[data]
  WHERE dataset_id = @dataset_id
  AND ((org_group='Mines' and location = @site_name) or (org_group=@site_name))
  AND (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level

END 


GO

create procedure [vdt].[sp_is_dataset_name_unique]
   @dataset_name nvarchar(100)
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT CASE WHEN Count(dataset_id) = 1 THEN 'True' ELSE 'False' END 
  FROM [vdt].[dataset]
  WHERE dataset_name = @dataset_name

END


GO


create procedure [vdt].[sp_is_scenario_name_unique]
   @scenario_name nvarchar(100)
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT CASE WHEN Count(scenario_id) = 1 THEN 'True' ELSE 'False' END 
  FROM [vdt].[scenario]
  WHERE scenario_name = @scenario_name

END


GO

create procedure [vdt].[sp_mark_dataset_deleted]
   @dataset_id int
AS 
BEGIN 
     SET NOCOUNT ON 

UPDATE vdt.[dataset]
	SET deleted = 1
	WHERE dataset_id = @dataset_id

END 

GO


create procedure [vdt].[sp_toggle_dataset_lock]
   @dataset_id int,
   @user_id int = null
AS 

BEGIN 
     SET NOCOUNT ON 

UPDATE vdt.[scenario]
	SET editing_by = @user_id
	WHERE view_dataset_id = @dataset_id

END 


GO



create procedure [vdt].[sp_update_dataset]
   @dataset_id int,
   @dataset_name nvarchar(100),
   @description nvarchar(1000),
   @category_id int,
   @is_public bit
AS 
BEGIN 
     SET NOCOUNT ON 

UPDATE vdt.[dataset]
	SET dataset_name = @dataset_name, category_id = @category_id,
	[description] = @description
	WHERE dataset_id = @dataset_id

--Currently on what if scenarios will have the is_public
UPDATE vdt.[scenario]
	SET is_public = @is_public
	WHERE view_dataset_id = @dataset_id

END 


GO

create procedure [vdt].[sp_is_dataset_create_unique]
   @dataset_name nvarchar(100),
   @scenario_name nvarchar(100)
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT CASE WHEN Count(dataset_id) = 1 THEN 0 ELSE 1 END
  FROM [vdt].[dataset]
  WHERE dataset_name = @dataset_name

  UNION ALL

  SELECT CASE WHEN Count(scenario_id) = 1 THEN 0 ELSE 1 END 
  FROM [vdt].[scenario]
  WHERE scenario_name = @scenario_name
  

END


GO


create procedure [vdt].[sp_get_user_details]
	@user_id int
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT [user_id]
      ,[login]
      ,[sec_level]
      ,[ad_name]
      ,[is_power_user]
	  ,[is_admin_user]
	  ,[pwd_hash]
  FROM [vdt].[user]
  WHERE [user_id] = @user_id

END


GO

create procedure [vdt].[sp_get_scenario_categories]
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT category_id, category_name
  FROM [vdt].[dataset_category]

END


GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create procedure [vdt].[sp_get_sensitivity_analysis_chart_data]
	@DataSetId int,
	@user_id int
AS
BEGIN
	SET NOCOUNT ON;

		SELECT 
			A.attr_id,
			CASE A.is_cost
				WHEN 1 THEN CT.ct_name 
				ELSE A.attr_name 
			END as attribute, 
			CT.ct_name as cost_type,
			A.is_cost, 
			SV.impact_of_decrease , 
			SV.impact_of_increase,
			OG.og_name as org_group,
			FN.func_name as [function],
			ACT.act_name as activity,
			WS.ws_name as workstream,
			LOC.loc_name as location,
			ETP.eqp_type_name as equipment_type,
			EQP.eqp_name as equipment
		
		FROM [vdt].[sensitivity] S
			inner JOIN [vdt].[sensitivity_value] SV ON (SV.sensitivity_id = S.sensitivity_id) 		
			left join vdt.attribute A on A.attr_id = sv.attr_id
			left join vdt.location LOC on A.loc_id=LOC.loc_id
			left join vdt.[workstream] WS on A.ws_id=WS.ws_id
			left join vdt.activity ACT on A.act_id=ACT.act_id
			left join vdt.eqp_type ETP on A.eqp_type_id=ETP.eqp_type_id
			left join vdt.equipment EQP on A.eqp_id=EQP.eqp_id
			left join vdt.cost_type CT on A.ct_id=CT.CT_id
			left join vdt.org_group OG on A.og_id=OG.og_id
			left join vdt.[function] FN on A.func_id=FN.func_id

		WHERE s.dataset_id = @DataSetId
		AND (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  A.sec_level
END


GO


create procedure [vdt].[sp_get_attribution_analysis_chart_data]
	@BaseDatasetId int,
	@BenchmarkDatasetId int,
	@user_id int
AS
BEGIN
	SET NOCOUNT ON;

		SELECT 
			A.attr_id,
			CASE A.is_cost
				WHEN 1 THEN CT.ct_name 
				ELSE A.attr_name 
			END as attribute, 
			CT.ct_name as cost_type,
			A.is_cost, 
			UF.[format] as [format],
			AV.impact_on_target, 
			DS.value as scenario_value, 
			DB.value as benchmark_value, 
			OG.og_name as org_group,
			FN.func_name as [function],
			ACT.act_name as activity,
			WS.ws_name as workstream,
			LOC.loc_name as location,
			ETP.eqp_type_name as equipment_type,
			EQP.eqp_name as equipment
		
		FROM [vdt].[attribution] AA
			inner JOIN [vdt].[attribution_value] AV ON (AV.attribution_id = AA.attribution_id) 		
			left join vdt.attribute A on A.attr_id = AV.attr_id
			left join vdt.unit_format UF on A.unit = UF.unit
			left join vdt.location LOC on A.loc_id=LOC.loc_id
			left join vdt.[workstream] WS on A.ws_id=WS.ws_id
			left join vdt.activity ACT on A.act_id=ACT.act_id
			left join vdt.eqp_type ETP on A.eqp_type_id=ETP.eqp_type_id
			left join vdt.equipment EQP on A.eqp_id=EQP.eqp_id
			left join vdt.cost_type CT on A.ct_id=CT.CT_id
			left join vdt.org_group OG on A.og_id=OG.og_id
			left join vdt.[function] FN on A.func_id=FN.func_id
			left join [vdt].[data] DS on (DS.dataset_id =  @BaseDatasetId AND DS.attr_id = A.attr_id)
			left join [vdt].[data] DB on (DB.dataset_id =  @BenchmarkDatasetId AND DB.attr_id = A.attr_id)

		WHERE AA.base_dataset_id = @BaseDatasetId
		AND AA.benchmark_dataset_id = @BenchmarkDatasetId
		AND(select sec_level from vdt.[user] where [user_id] = @user_id)  >=  A.sec_level

END

GO

create procedure [vdt].[sp_get_attribution_analysis_summary]
	@BaseDatasetId int,
	@BenchmarkDatasetId int
AS
BEGIN

	SET NOCOUNT ON;

		SELECT 
		A.attr_name,
		AA.base_dataset_id,
		AA.benchmark_dataset_id,
		AA.is_cumulative,
		AA.target_attr_id,
		AA.target_base_value,
		AA.last_update,
		UF.[format] as attribute_format
			
		FROM [vdt].[attribution] AA
			left join vdt.attribute A on A.attr_id = AA.target_attr_id
			left join vdt.unit_format UF on A.unit = UF.unit

		WHERE AA.base_dataset_id = @BaseDatasetId
		AND AA.benchmark_dataset_id = @BenchmarkDatasetId

END
		
GO


GO


create  procedure [vdt].[sp_get_vdt_data]
@dataset_id int, 
@benchmark_id int=null,
@user_id int,
@attributeIds vdt.IdFilterTableType READONLY

AS 
BEGIN 
     SET NOCOUNT ON 

	 declare @sec_level int;
	 set @sec_level = (select sec_level from vdt.[user] where [user_id] = @user_id);

SELECT
	V.[attr_id]
    ,UF.[format]
	,UF.[unit]
	,CASE WHEN @sec_level >=  A.sec_level THEN coalesce(B.value, V.value) ELSE null END AS base_value
	,CASE WHEN @sec_level >=  A.sec_level THEN BM.value ELSE null END AS benchmark
	,coalesce(((BM.value - coalesce(B.value, V.value)) / nullif(coalesce(B.value, V.value),0)),null) as target_change
	,coalesce(((coalesce(C.new_value, V.value) - coalesce(B.value, V.value)) / nullif(coalesce(B.value, V.value),0)),null) as what_if_change
	,CASE WHEN @sec_level >=  A.sec_level THEN coalesce(C.new_value, V.value) ELSE null END AS override_value
  FROM [vdt].[value] V
  inner join vdt.attribute A on V.attr_id = A.attr_id
  inner join vdt.dataset DS on V.dataset_id = DS.dataset_id
  left join vdt.scenario DSS on DS.dataset_id = DSS.view_dataset_id
  left join vdt.scenario_change C on DSS.scenario_id = C.scenario_id and A.attr_id = C.attr_id
  left join vdt.unit_format UF on A.unit = UF.unit
  left join vdt.value B on V.attr_id = B.attr_id and B.dataset_id = DSS.base_dataset_id
  left join vdt.value BM on V.attr_id = BM.attr_id and BM.dataset_id = @benchmark_id
  WHERE V.dataset_id = @dataset_id
  AND (A.attr_id IN (select id from @attributeIds))
  

END 
 
 

GO


create procedure [vdt].[sp_get_sensitivity_analysis_summary]
	@DataSetId int
AS
BEGIN

	SET NOCOUNT ON;

		SELECT 
		A.attr_name,
		S.percent_change,
		S.last_update,
		S.target_attr_id,
		S.sensitivity_id,
		S.target_base_value,
		UF.[format] as attribute_format
			
		FROM [vdt].[sensitivity] S
			left join vdt.attribute A on A.attr_id = S.target_attr_id
			left join vdt.unit_format UF on A.unit = UF.unit

		WHERE s.dataset_id = @DataSetId

END
		

GO


create procedure [vdt].[sp_get_user_sec_level] 
@user_id int

AS 
BEGIN 
     SET NOCOUNT ON 

select sec_level from vdt.[user] where [user_id] = @user_id
  
END 

GO

create procedure vdt.sp_get_models
  @user_id int
as
begin
  set nocount on;

  select model_id, [filename], file_date from vdt.model;
end
GO

create procedure [vdt].[sp_get_filters]
as
begin
  set nocount on;
  begin
    -- it's a mining location: activity, workstream, equipment, cost type, attribute
	with attributes as (
	  select * from vdt.attribute_category
	  where  activity <> 'Overheads' and [function] <> 'Overheads'
	  and (attribute_is_kpi<>0 or attribute_is_lever<>0))
	
	select distinct 'Location' as filter, 'loc_name' as colname, cast(loc_id as nvarchar) as id, loc_name as value
	from location where loc_name is not null
	union all
	select distinct 'Activity' as filter, 'act_id' as colname, cast(act_id as nvarchar) as id, activity as value
	from attributes where activity is not null
	union all
	select distinct 'Workstream' as filter, 'ws_id' as colname, cast(ws_id as nvarchar) as id, workstream as value
	from attributes where workstream is not null
	union all
	select distinct 'Equipment' as filter, 'eqp_id' as colname, cast(eqp_id as nvarchar) as id, equipment as value 
	from attributes where equipment is not null and attribute_is_cost=0
	union all
	select distinct 'Cost Type' as filter, 'ct_id' as colname, cast(ct_id as nvarchar) as id, cost_type as value 
	from attributes where cost_type is not null
	union all
	select distinct 'Attribute' as filter, 'attribute' as colname, attribute as id, attribute as value 
	from attributes
  end
end

GO

create procedure vdt.sp_get_dataset_excel_export
  @user_id int,
  @dataset_id int
as
begin
  set nocount on;

  declare @sec_level int
  select @sec_level=sec_level from vdt.[user] where [user_id]=@user_id
  select
    [org_group] as [Org Group],
    [function] as [Function],
    [activity] as [Activity],
    [workstream] as [Workstream],
    [location] as [Location],
    [equipment_type] as [Equipment Type],
    [equipment] as [Equipment],
    [cost_type] as [Cost Type],
    [product] as [Product],
    [attribute] as [Attribute],
    [attribute_unit] as [Unit],
    [attribute_is_cost] as [Cost?],
    [attribute_is_lever] as [Lever?],
    [attribute_is_aggregate] as [Aggregate?],
    [attribute_is_kpi] as [KPI?],
    case when abs([value]) > 1.79769313486231E+308 then 1.79769313486231E+308 else [value] end as [Value]
  from vdt.data
  where dataset_id=@dataset_id
    and attribute_sec_level <= @sec_level
end

go

create procedure [vdt].[sp_update_user]
	   @user_id int,
	   @sec_level int,
	   @is_power_user  bit,
	   @is_admin_user bit	                   
as 
begin 
     set NOCOUNT ON 

    update vdt.[user]
	set is_power_user = @is_power_user, sec_level = @sec_level, is_admin_user = @is_admin_user
	where [user_id] = @user_id

end

go


create procedure vdt.sp_get_model_by_dataset
  @dataset_id int,
  @user_id int
as
begin
  set nocount on;

  select M.model_id, M.[filename], M.file_date, MG.[name] as model_group, M.[version]
  from vdt.model M 
  inner join vdt.dataset D on M.model_id=D.model_id
  inner join vdt.model_group MG on M.model_group_id = MG.model_group_id
  where D.dataset_id=@dataset_id
end

go

create procedure [vdt].[sp_get_custom_views]
AS 
BEGIN 
     SET NOCOUNT ON 


declare @views table (
    name nvarchar(50),
	sproc nvarchar(50),
	template nvarchar(100),
	filter_columns nvarchar(50),
	key_columns nvarchar(50));

 insert into @views (name, sproc, template, filter_columns, key_columns) values ('System View', 'vdt.sp_get_system_view', '/Content/templates/SystemView.html', 'Case,Display', 'Process,Attribute');

 select * from @views

END 

go


CREATE procedure [vdt].[sp_get_system_view]  
@dataset_id int,
@user_id int
as

begin

with activity_level as (
select
  case org_group
    when 'Mines' then [location] + ' ' + [activity]
	when 'Rail' then 'Rail Mainline'
	when 'Port' then 'Port ' + coalesce([location], [activity])
  end as Process,
  attribute,
  attr_id, null as h_attr_id,
  attribute_is_cost,attribute_is_lever,attribute_is_kpi,
  base_value,
  value
from vdt.data where attribute in ('Internal Capacity', 'Value Chain Tonnes')
and org_group <> 'Port' and ([function] <> 'Shuttle' or [function] is null) 
and dataset_id=@dataset_id
)
,equipment_level as (
select
  concat([location], ' ', [equipment]) as Process,
  case attribute
    when 'Internal process capacity' then 'Internal Capacity'
	when 'Tonnes processed' then 'Value Chain Tonnes'
  end as attribute,
  attr_id,null as h_attr_id,
  attribute_is_cost,attribute_is_lever,attribute_is_kpi,
  base_value,
  value
from vdt.data where attribute in ('Internal process capacity', 'Tonnes processed')
and org_group='Mines' and [function]='Processing' and location in ('Whaleback', 'Eastern Ridge', 'Jimblebar')
and [equipment] not in ('Crusher 9', 'OHP 5')
and dataset_id=@dataset_id
)
,excl_port_and_shuttle as (
select * from activity_level
union all
select * from equipment_level
)
,data as (
select * from excl_port_and_shuttle
union all
select 'Rail Shuttle' as Process, case when attribute='Actual Throughput' then 'Value Chain Tonnes' else attribute end as attribute, attr_id,null as h_attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi,base_value, value
from vdt.data where [function]='Shuttle' and attribute in ('Actual Throughput', 'Internal Capacity') and dataset_id = @dataset_id
union all
select equipment as Process, case attribute /* Car dumpers */
  when 'Tonnes processed' then 'Value Chain Tonnes'
  when 'Internal process capacity' then 'Internal Capacity' end as attribute, attr_id,null as h_attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi,
base_value, value from vdt.data where org_group='Port' and [activity]='Inflow' and equipment_type='Car Dumper'
and attribute in ('Tonnes processed', 'Internal process capacity') and dataset_id = @dataset_id
union all
select location as Process, case attribute /* Yards */
  when 'Yard inflow' then 'Value Chain Tonnes'
  when 'Yard capacity' then 'Internal Capacity' end as attribute, attr_id,null as h_attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi,
base_value, value from vdt.data where org_group='Port' and activity='Outflow'
and attribute in ('Yard capacity', 'Yard inflow') and dataset_id = @dataset_id
union all
select case location   /* Shiploaders */
  when 'North Yard' then 'SL1&2'
  when 'East Yard' then 'SL3&4'
  when 'South Yard' then 'SL5&6'
  when 'West Yard' then 'SL7&8'
end as Process, case attribute
  when 'Yard outflow' then 'Value Chain Tonnes'
  when 'Shiploader capacity' then 'Internal Capacity' end as attribute,  attr_id,null as h_attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi,
base_value, value from vdt.data where org_group='Port' and activity='Outflow'
and attribute in ('Shiploader capacity', 'Yard outflow') and dataset_id = @dataset_id
)
,pivoted_prod as (
select T.attr_id,C.attr_id as h_attr_id,
        T.attribute_is_cost,T.attribute_is_lever,T.attribute_is_kpi,
		C.attribute_is_cost as h_attribute_is_cost,C.attribute_is_lever as h_attribute_is_lever,C.attribute_is_kpi as h_attribute_is_kpi,
         T.Process, T.base_value as base_tonnes, T.value as scenario_tonnes, C.base_value as base_capacity, C.value as scenario_capacity,
         case when T.base_value > C.base_value then 1 else T.base_value / nullif(C.base_value,0) end as base_uoc,
		 case when T.value > C.value then 1 else T.value / nullif(C.value,0) end as scenario_uoc,
		 case when T.base_value / nullif(C.base_value,0) > 0.9 then 14428421--'#dc2905'
		      when T.base_value / nullif(C.base_value,0) > 0.65 then 14448901--'#dc7905'
			  when T.base_value / nullif(C.base_value,0) > 0.40 then 6347781--'#60dc05'
			  else 9962972/*'#9805dc'*/ end as base_color,
		 case when T.value / nullif(C.value,0) > 0.9 then 14428421--'#dc2905'
		      when T.value / nullif(C.value,0) > 0.65 then 14448901--'#dc7905'
			  when T.value / nullif(C.value,0) > 0.40 then 6347781--'#60dc05'
			  else 9962972/*'#9805dc'*/ end as scenario_color
from data T inner join data C on T.Process=C.Process
where T.attribute='Value Chain Tonnes' and C.attribute='Internal Capacity')
,stock_data as (
select location + ' ' + equipment as Process, attribute,attr_id,null as h_attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi, base_value, value
from vdt.data where attribute in ('Closing Balance', 'Maximum Level', 'Minimum Level', 'Days Inventory')
  and equipment is not null and equipment <> 'Orebody'
and dataset_id=@dataset_id
)
,pivoted_stock as (
select T.attr_id,L.attr_id as h_attr_id, 
       T.attribute_is_cost,T.attribute_is_lever,T.attribute_is_kpi,
		L.attribute_is_cost as h_attribute_is_cost,L.attribute_is_lever as h_attribute_is_lever,L.attribute_is_kpi as h_attribute_is_kpi,
         T.Process, T.base_value as base_tonnes, T.value as scenario_tonnes, coalesce(C.base_value, 2*T.base_value) as base_capacity, coalesce(C.value, 2*T.value) as scenario_capacity,
         case when T.base_value >= coalesce(C.base_value, 2*T.base_value) then 1 else T.base_value / nullif(coalesce(C.base_value, 2*T.base_value),0) end as base_uoc,
		 case when T.value >= coalesce(C.value, 2*T.value) then 1 else T.value / nullif(coalesce(C.value, 2*T.value),0) end as scenario_uoc,
		 case when T.base_value / nullif(coalesce(C.base_value, 2*T.base_value),0) > 0.9 or T.base_value < coalesce(L.base_value, 0) then 14428421--'#dc2905'
			  else 11579568/*9962972*//*'#9805dc'*/ end as base_color,
		 case when T.value / nullif(coalesce(C.value, 2*T.value),0) > 0.9 or T.value < coalesce(L.value, 0) then 14428421--'#dc2905'
			  else 11579568/*9962972*//*'#9805dc'*/ end as scenario_color
from stock_data T
left join stock_data C on T.Process=C.Process and C.attribute='Maximum Level'
left join stock_data L on T.Process=L.Process and L.attribute='Minimum Level'
where T.attribute='Closing Balance'
union all
select attr_id,attr_id as h_attr_id,
        attribute_is_cost,attribute_is_lever,attribute_is_kpi,
		attribute_is_cost as h_attribute_is_cost,attribute_is_lever as h_attribute_is_lever,attribute_is_kpi as h_attribute_is_kpi,
   [location] + ' Stock' as Process, base_value as base_tonnes, value as scenario_tonnes, 10000000 as base_capacity, 10000000 as scenario_capacity,
  case when base_value > 10000000 then 1 else base_value / 10000000 end as base_uoc,
  case when value > 10000000 then 1 else value / 10000000 end as scenario_uoc,
  case when base_value / 10000000 > 0.9 or base_value < 0 then 11579568--14428421--'#dc2905'
    --else 9962972/*'#9805dc'*/ end as base_color,
	else 11579568 end as base_color,
  case when value / 10000000 > 0.9 or value < 0 then 11579568-- 14428421--'#dc2905'
    --else 9962972/*'#9805dc'*/ end as scenario_color
	else 11579568 end as scenario_color
from vdt.data where attribute in ('Closing stock')
and dataset_id=@dataset_id
)
,ranked_prod as (
select *, rank() over (order by base_uoc desc) as base_rank, rank() over (order by scenario_uoc desc) as scenario_rank from pivoted_prod
)
,pivoted_prod_rank_color as (
select attr_id, h_attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi,h_attribute_is_cost,h_attribute_is_lever,h_attribute_is_kpi, Process, base_tonnes, scenario_tonnes, base_capacity, scenario_capacity, base_uoc, scenario_uoc,
  case when base_rank=1 then 14428421--'#dc2905'
       when base_rank<5 then 14448901--'#dc7905'
	                    else 6347781--'#60dc05'
  end as base_color,
  case when scenario_rank=1 then 14428421--'#dc2905'
       when scenario_rank<5 then 14448901--'#dc7905'
	                       else 6347781--'#60dc05'
  end as scenario_color
from ranked_prod
)
,pivoted as (
select * from pivoted_prod_rank_color
union all
select * from pivoted_stock
)
,result as (
select 'Scenario' as [Case], 'Current Tonnes' as [Display], Process, 'Tonnes' as Attribute,attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi, scenario_tonnes as Value, 'Mtpa' as Unit from pivoted
union all
select 'Scenario' as [Case], 'Current Tonnes' as [Display], Process, 'Use of Capacity' as Attribute,attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi, scenario_uoc as Value, '%' as Unit from pivoted
union all
select 'Scenario' as [Case], 'Current Tonnes' as [Display], Process, 'Fill Color' as Attribute,attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi, scenario_color as Value, 'color' as Unit from pivoted

union all

select 'Scenario' as [Case], 'Headroom' as [Display], Process, 'Tonnes' as Attribute,h_attr_id as attr_id,h_attribute_is_cost as attribute_is_cost,h_attribute_is_lever as attribute_is_lever,h_attribute_is_kpi as attribute_is_kpi, case when scenario_tonnes > scenario_capacity then 0 else scenario_capacity - scenario_tonnes end as Value, 'Mtpa' as Unit from pivoted
union all
select 'Scenario' as [Case], 'Headroom' as [Display], Process, 'Use of Capacity' as Attribute,h_attr_id as attr_id,h_attribute_is_cost as attribute_is_cost,h_attribute_is_lever as attribute_is_lever,h_attribute_is_kpi as attribute_is_kpi, scenario_uoc as Value, '%' as Unit from pivoted
union all
select 'Scenario' as [Case], 'Headroom' as [Display], Process, 'Fill Color' as Attribute,h_attr_id as attr_id,h_attribute_is_cost as attribute_is_cost,h_attribute_is_lever as attribute_is_lever,h_attribute_is_kpi as attribute_is_kpi, scenario_color as Value, 'color' as Unit from pivoted

union all

select 'Base Case' as [Case], 'Current Tonnes' as [Display], Process, 'Tonnes' as Attribute,attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi, base_tonnes as Value, 'Mtpa' as Unit from pivoted
union all
select 'Base Case' as [Case], 'Current Tonnes' as [Display], Process, 'Use of Capacity' as Attribute,attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi, base_uoc as Value, '%' as Unit from pivoted
union all
select 'Base Case' as [Case], 'Current Tonnes' as [Display], Process, 'Fill Color' as Attribute,attr_id,attribute_is_cost,attribute_is_lever,attribute_is_kpi, base_color as Value, 'color' as Unit from pivoted

union all

select 'Base Case' as [Case], 'Headroom' as [Display], Process, 'Tonnes' as Attribute,h_attr_id as attr_id,h_attribute_is_cost as attribute_is_cost,h_attribute_is_lever as attribute_is_lever,h_attribute_is_kpi as attribute_is_kpi, case when base_tonnes > base_capacity then 0 else base_capacity - base_tonnes end as Value, 'Mtpa' as Unit from pivoted
union all
select 'Base Case' as [Case], 'Headroom' as [Display], Process, 'Use of Capacity' as Attribute,h_attr_id as attr_id,h_attribute_is_cost as attribute_is_cost,h_attribute_is_lever as attribute_is_lever,h_attribute_is_kpi as attribute_is_kpi, base_uoc as Value, '%' as Unit from pivoted
union all
select 'Base Case' as [Case], 'Headroom' as [Display], Process, 'Fill Color' as Attribute,h_attr_id as attr_id,h_attribute_is_cost as attribute_is_cost,h_attribute_is_lever as attribute_is_lever,h_attribute_is_kpi as attribute_is_kpi, base_color as Value, 'color' as Unit from pivoted

union all
select 'Scenario' as [Case], 'Current Tonnes' as [Display], Process as Process, 'Days Inventory' as Attribute, attr_id as attr_id,attribute_is_cost,attribute_is_lever ,attribute_is_kpi, [value] as Value, '#' as Unit from stock_data where attribute='Days Inventory'
union all
select 'Scenario' as [Case], 'Headroom' as [Display], Process as Process, 'Days Inventory' as Attribute, attr_id as attr_id,attribute_is_cost ,attribute_is_lever ,attribute_is_kpi, [value] as Value, '#' as Unit from stock_data where attribute='Days Inventory'
union all
select 'Base Case' as [Case], 'Current Tonnes' as [Display], Process as Process, 'Days Inventory' as Attribute, attr_id as attr_id,attribute_is_cost ,attribute_is_lever ,attribute_is_kpi, base_value as Value, '#' as Unit from stock_data where attribute='Days Inventory'
union all
select 'Base Case' as [Case], 'Headroom' as [Display], Process as Process, 'Days Inventory' as Attribute, attr_id as attr_id,attribute_is_cost,attribute_is_lever ,attribute_is_kpi, base_value as Value, '#' as Unit from stock_data where attribute='Days Inventory'

)

select * from result


end

go

create procedure [vdt].[sp_get_scenario_names]
@itt vdt.IdFilterTableType READONLY
AS 
BEGIN 
     SET NOCOUNT ON 

select dataset_id,dataset_name from vdt.dataset where dataset_id in (select id from @itt)

END 

go

create procedure [vdt].[sp_insert_audit]
       @audit_date datetime = null, 
       @category int = null, 
       @sub_category int = null,
	   @user_id int	                   
as 
begin 
     set nocount on 

     insert into vdt.[audit_entry]
          ( 
            audit_date,
            category,
            sub_category,
			[user_id]            
          ) 
     values 
          ( 
            @audit_date,
            @category,
            @sub_category,
			@user_id              
          )
end

go

CREATE procedure [vdt].[sp_get_attribute_by_id]
@dataset_id int, 
@user_id int,
@attributeIds vdt.IdFilterTableType READONLY

AS 
BEGIN 
     SET NOCOUNT ON 

	 declare @sec_level int;
	 set @sec_level = (select sec_level from vdt.[user] where [user_id] = @user_id);

select 
	  org_group, [function], activity, workstream, location, equipment_type, equipment, cost_type, product, attr_id, attribute, attribute_unit,
	  attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
	from vdt.data where dataset_id=@dataset_id
  AND (attr_id IN (select id from @attributeIds)) 
  AND  @sec_level>=  attribute_sec_level

END 	

GO

CREATE procedure [vdt].[sp_get_attributes_by_string]
@dataset_id int, 
@user_id int,
@search_string nvarchar(Max),
@search_option nvarchar(100)

AS 
BEGIN 
     SET NOCOUNT ON 

     declare @sec_level int;
     set @sec_level = (select sec_level from vdt.[user] where [user_id] = @user_id);
     IF @search_option = 'contains'
			select 
			  org_group, [function], activity, workstream, location, equipment_type, equipment, cost_type, product, attr_id, attribute, attribute_unit,
			  attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
			from vdt.data where dataset_id=@dataset_id
			  AND  
			      ((attribute_is_cost=1 and cost_type is not null and cost_type like '%'+@search_string+'%')
					 or (attribute_is_cost=1 and cost_type is null and attribute like '%'+@search_string+'%')
					 or (attribute_is_cost=0 and attribute like '%'+@search_string+'%'))
			  AND  (attribute_is_kpi = 0 and attribute_is_lever =1)    
			  AND  @sec_level>=  attribute_sec_level
	 ELSE IF @search_option = 'equals'
	       select 
			  org_group, [function], activity, workstream, location, equipment_type, equipment, cost_type, product, attr_id, attribute, attribute_unit,
			  attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
			from vdt.data where dataset_id=@dataset_id
			  AND 
			    ((attribute_is_cost=1 and cost_type is not null and cost_type = @search_string)
					 or (attribute_is_cost=1 and cost_type is null and attribute = @search_string)
					 or (attribute_is_cost=0 and attribute = @search_string))  
			  AND (attribute_is_kpi = 0 and attribute_is_lever =1)   
			  AND  @sec_level>=  attribute_sec_level
	 ELSE IF @search_option = 'not equal'
	       select 
			  org_group, [function], activity, workstream, location, equipment_type, equipment, cost_type, product, attr_id, attribute, attribute_unit,
			  attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
			from vdt.data where dataset_id=@dataset_id
			  AND  
			     ((attribute_is_cost=1 and cost_type is not null and cost_type != @search_string)
					 or (attribute_is_cost=1 and cost_type is null and attribute != @search_string)
					 or (attribute_is_cost=0 and attribute != @search_string))  
			  AND (attribute_is_kpi = 0 and attribute_is_lever =1)    
			  AND  @sec_level>=  attribute_sec_level
	 ELSE IF @search_option = 'not contain'
	       select 
			  org_group, [function], activity, workstream, location, equipment_type, equipment, cost_type, product, attr_id, attribute, attribute_unit,
			  attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi, value, base_value, override_value
			from vdt.data where dataset_id=@dataset_id
			  AND   ((attribute_is_cost=1 and cost_type is not null and cost_type not like '%'+@search_string+'%')
					 or (attribute_is_cost=1 and cost_type is null and attribute not like '%'+@search_string+'%')
					 or (attribute_is_cost=0 and attribute not like '%'+@search_string+'%'))
			  AND (attribute_is_kpi = 0 and attribute_is_lever =1) 
			  AND  @sec_level>=  attribute_sec_level
  
END
Go

GO

create procedure [vdt].[sp_is_super_user_group]
@group_names FilterTableType READONLY,
@model_id int
AS 
BEGIN 
     SET NOCOUNT ON 

select case when EXISTS (
select super_user_group_id from vdt.super_user_group super_user_group
inner join vdt.model model on model_id = @model_id
where (ad_name in (select item from @group_names)
and super_user_group.model_group_id = model.model_group_id)
)
then CAST(1 as bit) 
else CAST(0 as bit) end as is_super_user_group 

END 

go

create procedure [vdt].[sp_get_group_access_level]
@group_names FilterTableType READONLY,
@model_id int
AS 
BEGIN 
     SET NOCOUNT ON 

select max(security_level) as access_level from vdt.access_group  access_group
inner join vdt.model model on model_id = @model_id
where (ad_name in (select item from @group_names) and access_group.model_group_id = model.model_group_id )

END 

go

CREATE procedure [vdt].[sp_get_access_groups]
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT Distinct ad_name  FROM [vdt].[access_group]

END
go

Create procedure [vdt].[sp_get_model_groups]
@group_names FilterTableType READONLY,
@admin_user int 
AS 
BEGIN 
     SET NOCOUNT ON 
	declare @sec_level int,@ad_name nvarchar(200),@is_admin_user int, @user_id int;
	set @sec_level = (select sec_level from vdt.[user] where [user_id] = @user_id);
	set @ad_name = (select ad_name from vdt.[user] where [user_id] = @user_id);
	set @is_admin_user = (select is_admin_user from vdt.[user] where [user_id] = @user_id);

	IF @admin_user = 0
	BEGIN
	select DISTINCT mg.model_group_id,mg.name as model_group_name,m.model_id,m.[filename],m.[version] as model_version,mg.[instances]

	 from vdt.model_group as mg
	 inner join vdt.model as m on m.model_group_id = mg.model_group_id
	 inner join vdt.access_group as ag on m.model_group_id = ag.model_group_id
	 where m.is_active = 1 and (ag.ad_name  in (select item from @group_names)) order by mg.model_group_id ASC
	 -- @sec_level>= ag.security_level
	 END
	 ELSE
	  BEGIN
	     select DISTINCT mg.model_group_id,mg.name as model_group_name,m.model_id,m.[filename],m.[version] as model_version,mg.[instances]

		 from vdt.model_group as mg
		 inner join vdt.model as m on m.model_group_id = mg.model_group_id
		 inner join vdt.access_group as ag on m.model_group_id = ag.model_group_id
		 where m.is_active = 1  order by mg.model_group_id ASC
	  END

END
go

CREATE procedure [vdt].[sp_get_model_groups_by_id] 
@model_group_id int

AS 
BEGIN 
     SET NOCOUNT ON 

select mg.model_group_id,mg.name as model_group_name,mg.instances,
       m.model_id,m.filename,m.file_date,m.version,m.is_active,m.connection_string,
	   ag.access_group_id,ag.security_level,ag.ad_name as [access_group_ad_name],
	   sug.super_user_group_id,sug.ad_name as [super_user_ad_name]
       from vdt.model_group as mg
       inner join vdt.model as m on m.model_group_id = mg.model_group_id
	   inner join vdt.access_group as ag on ag.model_group_id = mg.model_group_id
	   inner join vdt.super_user_group as sug on sug.model_group_id= mg.model_group_id 
	   where (mg.model_group_id=@model_group_id AND m.is_active=1)

END 
go

CREATE procedure [vdt].[sp_insert_model]
   @model_group_name nvarchar(100),
   @no_of_instances int,
   @file_name nvarchar(256),
   @model_version int,
   @connection_string nvarchar(MAX),
   @access_group_names AccessGroupType READONLY,
   @super_user_group_names SuperUserAccessGroupType READONLY,
   @model_group_id int output,
   @model_id int output
AS 

BEGIN 
     SET NOCOUNT ON 

	 INSERT INTO [vdt].[model_group] ([name],[is_offline],[instances])
     VALUES (@model_group_name,0,@no_of_instances)

     select @model_group_id= CAST(scope_identity() AS int)

	 INSERT INTO [vdt].[model] ([filename],[file_date],[is_active],[version],[model_group_id],[connection_string])
			VALUES (@file_name,GETDATE(),1,@model_version,@model_group_id,@connection_string)

	select @model_id= CAST(scope_identity() AS int)

    INSERT INTO [vdt].[access_group] ([security_level],[ad_name],[model_group_id])
	select security_level,ad_name,@model_group_id from @access_group_names
     
	 INSERT INTO [vdt].[super_user_group] ([ad_name],[model_group_id])
     select ad_name,@model_group_id from @super_user_group_names
 select @model_group_id,@model_id;
END  

go
CREATE procedure [vdt].[sp_update_model]
   @model_group_id int,
   @model_id int,
   @model_group_name nvarchar(100),
   @no_of_instances int,
   @file_name nvarchar(256),
   @model_version int,
   @connection_string nvarchar(MAX),
   @access_group_names AccessGroupType READONLY,
   @super_user_group_names SuperUserAccessGroupType READONLY
AS 

BEGIN 
     SET NOCOUNT ON 
	 declare @model_version_from_db int,
	 @model_version_count int,
	 @rollback_filename nvarchar(256),
	 @rollback_file_date datetime;
	 set @model_version_from_db = (select [version] from vdt.model where model_id=@model_id);
	  set @rollback_filename = (select [filename] from vdt.model where model_id=@model_id);
	 set @rollback_file_date = (select [file_date] from vdt.model where model_id=@model_id);

	 UPDATE [vdt].[model_group] SET
		   [name] = @model_group_name,
		   [instances]= @no_of_instances
		  where model_group_id = @model_group_id

	      UPDATE [vdt].[model] SET [filename] = @file_name,[file_date] = GETDATE() ,
		    [is_active] = 1,[version] = @model_version,[model_group_id] = @model_group_id,
			  [connection_string] = @connection_string,[rollback_filename] = @rollback_filename,
			  [rollback_file_date] = @rollback_file_date,[last_rollback] = (@model_version - 1) 
		   where model_id= @model_id;

	 MERGE [access_group] AS T
		USING @access_group_names AS S
		ON (T.access_group_id = S.access_group_id AND T.model_group_id = @model_group_id) 
		WHEN NOT MATCHED BY TARGET 
			THEN INSERT ([security_level],[ad_name],[model_group_id]) Values (S.security_level,S.ad_name,@model_group_id)

		WHEN MATCHED 
			THEN UPDATE SET T.[security_level] = S.security_level,T.[ad_name] = S.ad_name
			     
		WHEN NOT MATCHED BY SOURCE AND T.[model_group_id] = @model_group_id
	 	 THEN DELETE ;
		
		MERGE [super_user_group] AS T
		USING @super_user_group_names AS S
		ON (T.super_user_group_id = S.[super_user_access_group_id] AND T.model_group_id = @model_group_id) 
		WHEN NOT MATCHED BY TARGET 
			THEN INSERT ([ad_name],[model_group_id]) values(S.ad_name,@model_group_id)

		 WHEN MATCHED 
			THEN UPDATE SET  T.[ad_name] = S.ad_name

		WHEN NOT MATCHED BY SOURCE AND T.[model_group_id] = @model_group_id
	 	 THEN DELETE ;

END  

go

CREATE procedure [vdt].[sp_upload_model] 
@model_group_id int,
@model_version int,
@file_name nvarchar(256)

AS 
BEGIN 
      declare @connection_string nvarchar(Max),
	 @rollback_filename nvarchar(256),
	 @rollback_file_date datetime;
	 
     SET NOCOUNT ON 

	  set @rollback_filename = (select [filename] from vdt.model where [model_group_id]=@model_group_id);
	  set @rollback_file_date = (select [file_date] from vdt.model where [model_group_id]=@model_group_id);
	   
	  UPDATE [vdt].[model] SET [filename] = @file_name,[file_date] = GETDATE() ,
			[is_active] = 1,[version] = @model_version,[rollback_filename] = @rollback_filename,
			[rollback_file_date] = @rollback_file_date,[last_rollback] = (@model_version - 1)
	   where [model_group_id] = @model_group_id
END 
go

CREATE procedure [vdt].[sp_rollback_to_previous_model] 
@model_group_id int,
@model_id int

AS 
BEGIN 
         declare @modelVersion int,@model_version_count int,
	 @filename nvarchar(256),
     @file_date datetime;
     SET NOCOUNT ON 
	 SET @model_version_count =(select count([version]) from vdt.model where model_group_id = @model_group_id)
	 SET @modelVersion =(select [version] from vdt.model where model_id = @model_id)
	  set @filename = (select [rollback_filename] from vdt.model where model_id=@model_id);
	 set @file_date = (select [rollback_file_date] from vdt.model where model_id=@model_id);
 UPDATE [vdt].[model] SET [version] = @modelVersion-1,[filename] = @filename,
		       file_date = @file_date,[rollback_filename] = null,[rollback_file_date] = null
		  where model_id = @model_id
END
go

CREATE procedure [vdt].[sp_get_previous_model_version_by_model_group_id] 
@model_group_id int
AS 
BEGIN 
			SELECT [last_rollback] as model_rollback_version,[rollback_filename] as model_rollback_filename,
		   FORMAT([rollback_file_date], 'yyyy-MM-dd HH:mm:ss') as model_rollback_file_date
		FROM vdt.model WHERE model_group_id = @model_group_id and [version] >  [last_rollback]
END

go

CREATE procedure [vdt].[sp_get_admin_model_groups]
AS 
BEGIN 
     SET NOCOUNT ON 

select mg.model_group_id,mg.name as model_group_name,m.model_id,m.[filename],m.[version] ,mg.[instances]
 from vdt.model_group as mg
 inner join vdt.model as m on m.model_group_id = mg.model_group_id
 where m.is_active = 1

END
go

create procedure [vdt].[sp_replicate_insert_update_user]
	   @user_id int,
       @login VARCHAR(15)= NULL, 
       @ad_name NVARCHAR(128)= NULL, 
       @pwd_hash NVARCHAR(64)= NULL,
	   @sec_level int,
	   @is_power_user  bit,
	   @is_admin_user bit                   
AS 
BEGIN 
     SET NOCOUNT ON 

	 IF EXISTS (SELECT 1 FROM vdt.[user] WHERE [user_id] = @user_id)
		BEGIN
 
			update vdt.[user]
			set is_power_user = @is_power_user, sec_level = @sec_level, is_admin_user = @is_admin_user
			where [user_id] = @user_id
			
		END

	ELSE

		BEGIN

			SET IDENTITY_INSERT [vdt].[user] ON

			 INSERT INTO vdt.[user]
				  ( 
					[user_id],
					[login],
					ad_name,
					pwd_hash,
					sec_level,
					is_power_user,
					is_admin_user            
				  ) 
			 VALUES 
				  ( 
					@user_id,
					@login,
					@ad_name,
					@pwd_hash,
					@sec_level,
					@is_power_user,
					@is_admin_user               
				  ) 

			SET IDENTITY_INSERT [vdt].[user] OFF

		END

END 

go

create procedure [vdt].[sp_get_model_connection_string]
@model_id int
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT  connection_string from vdt.model where model_id = @model_id

END 
go

create procedure [vdt].[sp_get_model_connection_strings]
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT model_id, connection_string from vdt.model

END

go 

create procedure [vdt].[sp_get_all_model_groups]
AS 
BEGIN 
     SET NOCOUNT ON 

select mg.model_group_id,mg.name as model_group_name,m.model_id,m.[filename],m.[version] as model_version,mg.[instances]
 from vdt.model_group as mg
 inner join vdt.model as m on m.model_group_id = mg.model_group_id
 where m.is_active = 1

END

go

create procedure [vdt].[sp_sync_insert_model]
   @model_group_name nvarchar(100),
   @no_of_instances int,
   @file_name nvarchar(256),
   @model_version int,
   @connection_string nvarchar(MAX),
   @model_id int 
AS 

BEGIN 
     SET NOCOUNT ON 
	 
	 SET IDENTITY_INSERT [vdt].[model] ON	
	
	declare @model_group_id int
	 INSERT INTO [vdt].[model_group] ([name],[is_offline],[instances])
     VALUES (@model_group_name,0,@no_of_instances)

	  select @model_group_id= CAST(scope_identity() AS int)

	 INSERT INTO [vdt].[model] ([model_id],[filename],[file_date],[is_active],[version],[model_group_id],[connection_string])
			VALUES (@model_id,@file_name,GETDATE(),1,@model_version,@model_group_id,@connection_string)

 select @model_group_id,@model_id;

  SET IDENTITY_INSERT [vdt].[model] OFF

END   


go

create procedure [vdt].[sp_sync_update_model]
   @model_group_name nvarchar(100),
   @no_of_instances int,
   @file_name nvarchar(256),
   @model_version int,
   @connection_string nvarchar(MAX),
   @model_id int
AS 

BEGIN 
     SET NOCOUNT ON 


	 update [vdt].[model] set [filename] = @file_name,[file_date] = GETDATE() ,[is_active] = 1,[version] =@model_version,[connection_string]=@connection_string
	 where model_id =@model_id


END  

go
