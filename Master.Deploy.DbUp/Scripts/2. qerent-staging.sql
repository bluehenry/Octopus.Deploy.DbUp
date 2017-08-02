
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
  
create index qerent_attr_ids_object_path_attr_name on [staging].[qerent_attr_ids] (model_id, object_path, attr_name) include (attr_id);

go

create procedure staging.sp_update_scenario_from_qerent (
  @dataset_id int,
  @data staging.QerentDataType readonly)
as
begin
  set nocount on

  /* This can happen after a dataset import */
  merge vdt.value T using (
    select @dataset_id as dataset_id, A.attr_id, D.value
	from @data D inner join staging.qerent_attr_ids A
	  on D.[Object Path]=A.object_path
	  and D.[Attribute]=A.attr_name) S
  on T.dataset_id=S.dataset_id and T.attr_id=S.attr_id
  when not matched by target then
    insert (dataset_id, attr_id, value) values (S.dataset_id, S.attr_id, S.value)
  when matched then
    update set T.value=S.value;
  
  update vdt.dataset set last_update=CURRENT_TIMESTAMP
  where dataset_id=@dataset_id;
end

go

create view [staging].[qerent_import] as
select V.dataset_id, Q.object_path, Q.attr_name, coalesce(C.new_value, V.value) as value, 0 as is_override
from vdt.value V inner join vdt.attribute A on V.attr_id=A.attr_id
inner join vdt.dataset D on V.dataset_id=D.dataset_id
inner join staging.qerent_attr_ids Q on A.attr_id=Q.attr_id --and D.model_id=Q.model_id
left join vdt.scenario S on S.view_dataset_id=V.dataset_id
left join vdt.scenario_change C on S.scenario_id=C.scenario_id and C.attr_id=A.attr_id
where A.is_calculated=0
union all
select S.view_dataset_id, Q.object_path, Q.attr_name, V.new_value, 1 as is_override
from vdt.scenario_change V inner join vdt.scenario S on V.scenario_id=S.scenario_id
inner join staging.qerent_attr_ids Q on V.attr_id=Q.attr_id
inner join vdt.attribute A on V.attr_id=A.attr_id
where A.is_calculated=1

go

create type staging.[QerentCategoryType] as table (
  qerent_category_id int,
  grouping_name nvarchar(50),
  category_path nvarchar(2000));

go

create type staging.[QerentAttrCategoryMapType] as table (
  qerent_attr_id int,
  qerent_category_id int);

go

CREATE TYPE [staging].[QerentDependencyType] AS TABLE (
    [QerentAttrId] INT,
	[DependsOnQerentAttrId] INT
)
go

create procedure [report].[sp_update_attribute_cache] as
begin
  set nocount on
  -- SITE MAY CUSTOMIZE HERE
end
go

create procedure [staging].[sp_import_qerent_model_structure] (
    @db_ver int,
    @model_id int,
    @filename nvarchar(256),
	@data staging.QerentStructureType readonly,
	@deps staging.QerentDependencyType readonly,
	@categories staging.QerentCategoryType readonly,
	@attr_category_map staging.QerentAttrCategoryMapType readonly,
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

  -- create any groupings that don't already exist
  insert into vdt.[grouping] (grouping_name, is_filter)
  select distinct C.grouping_name, 1 from @categories C
  where not exists (select * from vdt.[grouping] G where G.grouping_name=C.grouping_name);
  
  -- split the category name strings into rows (the delimiter for levels is double-tilde "~~")
  with cat_levels (grp_id, qid, tmp_id, parent_id, lev, name, tail) as (
    select G.[grouping_id], qerent_category_id, 100*qerent_category_id as tmp_id, null, 1,
      cast(left(category_path, patindex('%~~%', category_path + '~~')-1) as nvarchar),
      stuff(category_path, 1, patindex('%~~%', category_path+'~~')+1, '')+'~~'
    from @categories C inner join vdt.[grouping] G on G.[grouping_name]=C.grouping_name
    union all
    select grp_id, qid, tmp_id+1, tmp_id, lev + 1,
      cast(left(tail, patindex('%~~%', tail)-1) as nvarchar),
      stuff(tail, 1, patindex('%~~%', tail)+1, '')
    from cat_levels X where tail <> '~~' and tail <> ''
  )
  
  -- preserve this in a temp table because we're going to need to do a level-by-level merge with the existing data
  -- in the vdt.category table
  select grp_id, case when tail='~~' or tail='' then qid else null end as qid, tmp_id, parent_id, lev, name, 0 as cat_id
  into #cat_levels from cat_levels;

  -- result will be a map from qerent_category_id to db_category_id. NB qerent_category_id need not refer to a leaf!
  declare @cat_id_map table (qerent_category_id int, db_category_id int);
  
  declare @maxlevel int = (select max(lev) from #cat_levels);
  declare @lev int = 1
  
  -- level-by-level iteration through the category tree
  while @lev <= @maxlevel
  begin
    -- add any categorys at this level that don't already exist - NB matching on parent_id as well as name and grouping (same name can occur multiple times with
	-- different parent categories).
    insert into vdt.category (category_name, parent_category_id, [grouping_id])
    select distinct name, parent_id, grp_id from #cat_levels C where lev=@lev
    and not exists (select * from vdt.category E where E.category_name=C.name and coalesce(E.parent_category_id,-1)=coalesce(C.parent_id,-1) and E.grouping_id=C.grp_id)
  
    -- read back database category_ids for this level (they should all exist now)
    update #cat_levels set cat_id=E.category_id
    from vdt.category E where E.category_name=#cat_levels.name and coalesce(E.parent_category_id,-1)=coalesce(#cat_levels.parent_id,-1) and E.grouping_id=#cat_levels.grp_id
  
    -- add the mapping for any qerent_category_ids that reference this level
    insert into @cat_id_map (qerent_category_id, db_category_id)
    select qid, cat_id from #cat_levels where lev=@lev and qid is not null
  
    -- we are about to set the tmp_id of this level to the correct database category_id values, but first we
	-- must update their children so that they will refer to the correct parent
    update child set parent_id=parent.cat_id
    from #cat_levels child
    inner join #cat_levels parent on parent.tmp_id=child.parent_id
    where child.lev=@lev+1;
  
    -- now we can delete any redundant rows (note that a parent may appear multiple times in the original list as it is referenced by different children)
    with minids (grp_id, tmp_id, parent_id, name) as (
      select grp_id, min(tmp_id), parent_id, name from #cat_levels where lev=@lev group by grp_id, parent_id, name)
    delete D from #cat_levels D inner join minids I on I.name=D.name and coalesce(D.parent_id,-1)=coalesce(I.parent_id,-1) and I.grp_id=D.grp_id
    where D.tmp_id <> I.tmp_id
  
    -- finally, set tmp_id to the db category_id for this level
    update #cat_levels set tmp_id=cat_id where lev=@lev
  
    set @lev = @lev + 1
  end

  -- At this point we have all the groupings and categories in the database and @cat_id_map is good to map the qerent category ids that
  -- are referred to in the attribute data to the appropriate db category_id.

  -- Add any new attributes from the model and update flags on existing ones
  declare @NewAttributes table (action nvarchar(20), attr_id int, object_path nvarchar(256), qerent_attr_id int, attr_name nvarchar(100));

  merge vdt.attribute T using (
    select A.attr_id, D.[QerentAttrId], D.[Object Path], D.[Attribute],
	    coalesce(D.[DisplayUnit], '#') as DisplayUnit,
		coalesce(D.[SecLevel], 1) as SecLevel,
	    case substring(D.[Type], 1, 1) when 'C' then 1 else 0 end as is_cost,
        case substring(D.[Type], 2, 1) when 'L' then 1 else 0 end as is_lever,
        case substring(D.[Type], 2, 1) when 'C' then 1 else 0 end as is_calculated,
        case substring(D.[Type], 3, 1) when 'A' then 1 else 0 end as is_aggregate,
        case substring(D.[Type], 4, 1) when 'K' then 1 else 0 end as is_kpi
    from @data D left join staging.qerent_attr_ids A
	  on D.[Object Path]=A.object_path and D.[Attribute]=A.attr_name and @model_id=A.model_id) S
  on T.attr_id=S.attr_id
  when not matched by target then
    insert (attr_name, unit, sec_level, is_cost, is_lever, is_calculated, is_aggregate, is_kpi)
	values (S.Attribute, S.DisplayUnit, S.SecLevel, S.is_cost,  S.is_lever, S.is_calculated,  S.is_aggregate, S.is_kpi)
  when matched then
    update set T.unit=S.DisplayUnit, T.sec_level=S.SecLevel, T.is_cost=S.is_cost, T.is_lever=S.is_lever,
	  T.is_calculated=S.is_calculated, T.is_aggregate=S.is_aggregate, T.is_kpi=S.is_kpi
  output $action, inserted.attr_id, S.[Object Path], S.[QerentAttrId], S.[Attribute] into @NewAttributes;
  
  insert into staging.qerent_attr_ids (model_id, attr_id, object_path, qerent_attr_id, attr_name)
  select @model_id, attr_id, object_path, qerent_attr_id, attr_name from @NewAttributes
  where action='INSERT';

  -- Postcondition: staging.qerent_attr_ids is up-to-date.

  -- Update attribute categories
  delete from vdt.attribute_category where attr_id in (select attr_id from @cat_id_map);
  insert into vdt.attribute_category (attr_id, grouping_id, category_id)
  select A.attr_id, C.grouping_id, C.category_id
  from staging.qerent_attr_ids A
  inner join @attr_category_map M on A.qerent_attr_id=M.qerent_attr_id
  inner join @cat_id_map M2 on M.qerent_category_id=M2.qerent_category_id
  inner join vdt.category C on M2.db_category_id=C.category_id

  -- Overwrite the dependency map for any attributes that were listed in @data
  -- NB we are relying on the script ensuring that attributes are in the @deps map if and only if they're in the @data table
  delete from vdt.attribute_dependency where attr_id in (select A.attr_id
  from staging.qerent_attr_ids A inner join @data D on A.object_path=D.[Object Path] and A.attr_name=D.Attribute and model_id = @model_id);

  insert into vdt.attribute_dependency (attr_id, depends_on_attr_id,model_id)
  select X.attr_id, Y.attr_id,@model_id
  from @deps D
  inner join staging.qerent_attr_ids X on D.QerentAttrId=X.qerent_attr_id and X.model_id = @model_id
  inner join staging.qerent_attr_ids Y on D.DependsOnQerentAttrId=Y.qerent_attr_id and Y.model_id = @model_id
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
	inner join @data Q on I.[object_path] = Q.[Object Path] and I.[attr_name] = Q.[Attribute]
  end
  
  -- NB the Qerent Attr Id for the top of tree is not the same as the database attr_id...
  declare @top_of_tree_attr_id int = (select attr_id from staging.qerent_attr_ids where qerent_attr_id=@top_of_tree_qattr_id and model_id = @model_id);

  update vdt.model set
    top_of_tree_attr_id = @top_of_tree_attr_id,
	[filename] = @filename
  where model_id = @model_id

  exec [report].sp_update_attribute_cache
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


CREATE NONCLUSTERED INDEX [_dta_index_qerent_attr_ids_c_50_1799677459__K2_K3_K4_9987] ON [staging].[qerent_attr_ids]
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

create type staging.vdt_node_type AS TABLE(
	nodeId int not null,
	attributeId int not null,
	name nvarchar(100) not null,
	link nvarchar(100)  null
);
GO

create type staging.vdt_structure_type AS TABLE(
	name nvarchar(100) not null
);
GO

create type staging.vdt_edge_type as table(
	parentId int not null,
	childId int not null)
GO


create procedure [staging].[sp_get_attribute_id_map_to_qerent]
  @model_filename nvarchar(256)
as
select object_path + '[' + attr_name + ']' as full_path, I.attr_id, '' as link
from staging.qerent_attr_ids I
--inner join vdt.model M on I.model_id=M.model_id
--where (M.filename=@model_filename or M.filename='_debug' and left(@model_filename, 1)='_')


GO


