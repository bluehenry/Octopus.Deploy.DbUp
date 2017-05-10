--USE *database_name*

/*
ColumnChart output format
-------------------------
series: legend entry for the bar
group: x-axis title for the bar
component: vertical stack element for the bar
stack_order: numerical priority of the component in the stack, lower numbers are at the bottom
fill_color: colour for the bar, HTML notation e.g. #ffaacc
fill_pattern: TBD, hatching/tiling pattern to use
value: numeric, height of the bar, non-negative
*/

create procedure vdt.sp_get_site_reports
  @site nvarchar(100),
  @dataset_id int,
  @user_id int
as
begin
  set nocount on;

  declare @reports table (
    title nvarchar(50),
	sproc nvarchar(50),
	args nvarchar(100),
	renderer nvarchar(50),
	value_format nvarchar(50));

  insert into @reports (title, sproc, args, renderer, value_format) values ('WAIO KPI', 'vdt.sp_get_system_kpis', '@dataset_id,@user_id', 'SimpleTable', '');
  if @site <> 'WAIO'
  begin
    insert into @reports (title, sproc, args, renderer, value_format) values (@site + ' Volume KPI', 'vdt.sp_get_site_volume_kpis', '@site_name,@dataset_id,@user_id', 'SimpleTable', '');
    insert into @reports (title, sproc, args, renderer, value_format) values (@site + ' Cost KPI', 'vdt.sp_get_site_cost_kpis', '@site_name,@dataset_id,@user_id', 'SimpleTable', '');
  end
  else
  begin
    insert into @reports (title, sproc, args, renderer, value_format) values ('OOR Tonnes', 'vdt.sp_get_mining_oor', '@dataset_id,@user_id', 'SimpleTable', '');
    insert into @reports (title, sproc, args, renderer, value_format) values ('WAIO Costs', 'vdt.sp_get_system_total_cost', '@dataset_id,@user_id', 'ColumnChart', 'A$M');
    insert into @reports (title, sproc, args, renderer, value_format) values ('WAIO Cost Changes', 'vdt.sp_get_system_cost_waterfall', '@dataset_id,@user_id', 'ColumnChart', '$/t');
    insert into @reports (title, sproc, args, renderer, value_format) values ('System Capacity', 'vdt.sp_get_system_tonnes', '@dataset_id,@user_id', 'ColumnChart', 'Mtpa');
    insert into @reports (title, sproc, args, renderer, value_format) values ('GHG Emissions', 'vdt.sp_get_system_ghg_emissions', '@dataset_id,@user_id', 'ColumnChart', 't');
  end
  insert into @reports (title, sproc, args, renderer, value_format) values ('Incremental OOR Tonnes', 'vdt.sp_get_mining_tonnes_waterfall', '@dataset_id,@user_id', 'ColumnChart', 'Mtpa');
  if @site not in ('Rail', 'Port', 'WAIO')
  begin
    insert into @reports (title, sproc, args, renderer, value_format) values ('Mine Capacity (Ore Tonnes)', 'vdt.sp_get_capacity_chart', '@site_name,@dataset_id,@user_id', 'ColumnChart', 'Mtpa');
    insert into @reports (title, sproc, args, renderer, value_format) values ('System Capacity', 'vdt.sp_get_system_tonnes', '@dataset_id,@user_id', 'ColumnChart', 'Mtpa');
	insert into @reports (title, sproc, args, renderer, value_format) values ('Process Costs', 'vdt.sp_get_mine_total_cost_chart', '@site_name,@dataset_id,@user_id', 'ColumnChart', 'A$M');
	insert into @reports (title, sproc, args, renderer, value_format) values ('Cost Changes ($/t OOR)', 'vdt.sp_get_mine_cost_waterfall', '@site_name,@dataset_id,@user_id', 'ColumnChart', '$/t');
    insert into @reports (title, sproc, args, renderer, value_format) values ('GHG Emissions', 'vdt.sp_get_mine_ghg_emissions', '@site_name,@dataset_id,@user_id', 'ColumnChart', 't');
	insert into @reports (title, sproc, args, renderer, value_format) values ('Rehandle Cost', 'vdt.sp_get_mine_rehandle_cost_per_tonne', '@site_name,@dataset_id,@user_id', 'SimpleTable', '$/t');
	insert into @reports (title, sproc, args, renderer, value_format) values ('Rehandle Cost', 'vdt.sp_get_mine_rehandle_cost_total', '@site_name,@dataset_id,@user_id', 'SimpleTable', 'A$M');	
	insert into @reports (title, sproc, args, renderer, value_format) values ('Process Efficiency', 'vdt.sp_get_mine_process_efficiencies', '@site_name,@dataset_id,@user_id', 'SimpleTable', '');	
  end
  if @site = 'Rail'
  begin
    insert into @reports (title, sproc, args, renderer, value_format) values ('System Capacity', 'vdt.sp_get_system_tonnes', '@dataset_id,@user_id', 'ColumnChart', 'Mtpa');
    insert into @reports (title, sproc, args, renderer, value_format) values ('Rail Costs', 'vdt.sp_get_rail_total_cost', '@dataset_id,@user_id', 'ColumnChart', 'A$M');
    insert into @reports (title, sproc, args, renderer, value_format) values ('Rail Cost Changes', 'vdt.sp_get_rail_cost_waterfall', '@dataset_id,@user_id', 'ColumnChart', '$/t');
  end
  if @site = 'Port'
  begin
    insert into @reports (title, sproc, args, renderer, value_format) values ('Port Capacity', 'vdt.sp_get_port_capacity_chart', '@dataset_id,@user_id', 'ColumnChart', 'Mtpa');
    insert into @reports (title, sproc, args, renderer, value_format) values ('System Capacity', 'vdt.sp_get_system_tonnes', '@dataset_id,@user_id', 'ColumnChart', 'Mtpa');
    insert into @reports (title, sproc, args, renderer, value_format) values ('Port Costs', 'vdt.sp_get_port_total_cost', '@dataset_id,@user_id', 'ColumnChart', 'A$M');
    insert into @reports (title, sproc, args, renderer, value_format) values ('Port Cost Changes', 'vdt.sp_get_port_cost_waterfall', '@dataset_id,@user_id', 'ColumnChart', '$/t');
  end
  insert into @reports (title, sproc, args, renderer, value_format) values ('Site margins', 'vdt.sp_get_system_site_margins', '@dataset_id,@user_id', 'SimpleTable', '');
  insert into @reports (title, sproc, args, renderer, value_format) values ('Product margins', 'vdt.sp_get_system_product_margins', '@dataset_id,@user_id', 'SimpleTable', '');
  select * from @reports;
end
go


create procedure [vdt].[sp_get_mining_tonnes_waterfall]
  @dataset_id int,
  @user_id int
as
begin
  set nocount on;

  declare @basecase_name nvarchar(100);
  declare @scenario_name nvarchar(100);
  
  select @basecase_name=B.dataset_name, @scenario_name=V.dataset_name
  from vdt.dataset V inner join vdt.scenario S on S.view_dataset_id=V.dataset_id
  inner join vdt.dataset B on S.base_dataset_id=B.dataset_id
  where V.dataset_id=@dataset_id;

  set @basecase_name = 'Base';
  set @scenario_name = 'Scenario';
  
  with oor as (
  select row_number() over (order by (value - base_value)) num, location, value as scenario_value, base_value
  from vdt.data where attribute='OOR' and org_group='Mines' and dataset_id=@dataset_id and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level)
  ,totals as (
  select 'Base Case' as series, sum(base_value) as total from oor
  union all
  select 'Scenario' as series, sum(scenario_value) as total from oor)
  
  ,waterfall as (
  select cast(0 as int) as num, cast('Blue' as nvarchar) as col, cast(0 as float) as base, total as cum, total as height from totals where series='Base Case'
  union all
  select
    cast(R.num as int),
    cast(case when R.scenario_value < R.base_value then 'Red' else 'Green' end as nvarchar) as col,
    case when R.scenario_value < R.base_value then (R.scenario_value - R.base_value) + P.cum else P.cum end as base,
    P.cum + (R.scenario_value - R.base_value) as cum,
    case when R.scenario_value < R.base_value then R.base_value - R.scenario_value else R.scenario_value - R.base_value end as height
  from oor R inner join waterfall P on P.num = R.num-1)
  
  
  ,pivoted as (
  select @basecase_name as [group], 0 as [Transparent], total as [Total], 0 as [Increase], 0 as [Decrease]
  from totals where series='Base Case'
  union all
  select location as [group], base as [Transparent], 0 as [Total], case when col='Green' then height else 0 end as [Increase], case when col='Red' then height else 0 end as [Decrease]
  from waterfall W inner join oor L on W.num=L.num
  union all
  select @scenario_name as [group], 0 as [Transparent], total as [Total], 0 as [Increase], 0 as [Decrease]
  from totals where series='Scenario')
  
  select 'Change in tonnes' as series, [group], 'Transparent' as [component], [Transparent] as value, 'rgba(0,0,0,0)' as fill_color, '' as fill_pattern, 1 as stack_order from pivoted
  union all
  select 'Change in tonnes' as series, [group], 'Total' as [component], [Total] as value, '#0000c0' as fill_color, '' as fill_pattern, 2 as stack_order from pivoted
  union all
  select 'Change in tonnes' as series, [group], 'Increase' as [component], [Increase] as value, '#008800' as fill_color, '' as fill_pattern, 3 as stack_order from pivoted
  union all
  select 'Change in tonnes' as series, [group], 'Decrease' as [component], [Decrease] as value, '#880000' as fill_color, '' as fill_pattern, 4 as stack_order from pivoted
end
go

create procedure [vdt].[sp_get_capacity_chart]
	@site_name nvarchar(100),
	@dataset_id int,
    @user_id int
as
begin
set nocount on;

with data as (
select case when Location='Whaleback' and activity='OFH' then 'CD+Bene' else Activity end as [Activity],
  attribute, value, base_value
from vdt.data where attribute in ('Value Chain Tonnes', 'Internal Capacity', 'Stacked VCT')
  and location=@site_name and activity is not null and dataset_id=@dataset_id
)
,headroom as (
select T.Activity,
  case when C.value is null or C.value < T.value then 0 else C.value - T.value end as value,
  case when C.base_value is null or C.base_value < T.base_value then 0 else C.base_value - T.base_value end as base_value
from data T left join data C on T.Activity=C.Activity and C.Attribute='Internal Capacity'
where T.attribute='Value Chain Tonnes'
)
,polite as (
select activity, case attribute when 'Stacked VCT' then '' else attribute end as attribute, value, base_value
from data where attribute <> 'Internal Capacity'
union all
select activity, 'Headroom' as attribute, value, base_value from headroom
)
,consolidated as (
select 'Base Case' as series, activity as [group], attribute as component, base_value as value,
  case attribute when '' then 'transparent' when 'Value Chain Tonnes' then 'black' else 'rgb(216,222,231)' end as fill_color,
  case attribute when '' then 'empty' when 'Value Chain Tonnes' then 'solid' else 'top_dash' end as fill_pattern,
  case attribute when '' then 1 when 'Value Chain Tonnes' then 2 else 3 end as stack_order
from polite
union all
select 'Scenario' as series, activity as [group], attribute as component, value as value,
  case attribute when '' then 'transparent' when 'Value Chain Tonnes' then 'orange' else 'rgb(216,222,231)' end as fill_color,
  case attribute when '' then 'empty' when 'Value Chain Tonnes' then 'solid' else 'top_dash' end as fill_pattern,
  case attribute when '' then 1 when 'Value Chain Tonnes' then 2 else 3 end as stack_order
from polite
)
select * from consolidated
order by series, case [group]
  when 'Drill' then 1
  when 'Blast' then 2
  when 'Load' then 3
  when 'Haul' then 4
  when 'Beneficiation' then 5
  when 'OFH' then 6
  when 'CD+Bene' then 6
  when 'OFR' then 7
  when 'TLO' then 8
  else 7 end, stack_order
end
  

GO

create procedure [vdt].[sp_get_mine_total_cost_chart]
  @site_name nvarchar(100),
  @dataset_id int,
  @user_id int
as
begin
  declare @basecase_name nvarchar(100);
  declare @scenario_name nvarchar(100);
  
  select @basecase_name=coalesce(B.dataset_name, V.dataset_name), @scenario_name=V.dataset_name
  from vdt.dataset V left join vdt.scenario S on S.view_dataset_id=V.dataset_id
  left join vdt.dataset B on S.base_dataset_id=B.dataset_id
  where V.dataset_id=@dataset_id;

  set @basecase_name = 'Base Case';
  set @scenario_name = 'Scenario';

  with totals as (
    select [function] as obj, attribute as attr, value, base_value
    from vdt.data
    where attribute in ('Fixed cost', 'Variable cost')
    and activity is null
    and org_group='Mines' and location=@site_name
	and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
    and dataset_id=@dataset_id)
    
  ,lineitems as (
	select *
    from vdt.data
    where attribute in ('Fixed cost', 'Variable cost')
    and cost_type is null and equipment is null and (activity is not null or [function]='Overheads')
	and (activity <> 'Load' and activity <> 'Haul') and (equipment_type='Plant' or equipment_type is null)
	and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
    and dataset_id=@dataset_id and org_group='Mines' and location=@site_name)
  
  ,subtotals as (
	  select [function] as obj, attribute as attr, sum(value) as value, sum(base_value) as base_value
	  from lineitems group by [function], attribute)

  ,sna as (
  select T.obj, T.attr, T.value - S.value as value, T.base_value - S.base_value as base_value
  from subtotals S inner join totals T on S.obj=T.obj and S.attr=T.attr
  where T.obj <> 'Overheads')

  ,pivot_data as (
    select
      coalesce(activity, 'Mine Overheads') as [group],
      attribute as component,
      value as value,
	  base_value
    from lineitems
    union all
    select
	case S.obj
	  when 'Mining' then 'Mine Svc & Ovh'
	  when 'Processing' then 'Proc Svc & Ovh'
	end as [group],
  	S.attr as component,
  	value as value,
	base_value
    from sna S)

  ,data as (
    select @basecase_name as series, [group], [component], [base_value] as value,
	  case when [component]='Fixed cost' then 1 else 2 end as stack_order,
	  case when [component]='Fixed cost' then '#000' else '#aaa' end as fill_color, '' as fill_pattern
	from pivot_data
	union all
    select @scenario_name as series, [group], [component], [value] as value,
	  case when [component]='Fixed cost' then 1 else 2 end as stack_order,
	  case when [component]='Fixed cost' then '#ffa500' else '#ffcb6a' end as fill_color, '' as fill_pattern
	from pivot_data)

  ,grouped as (
	select series, case [group]
	  when 'Drill' then 'Drill & Blast'
	  when 'Blast' then 'Drill & Blast'
	  when 'Load' then 'Load & Haul'
	  when 'Haul' then 'Load & Haul'
	  when 'OFH' then 'Processing'
	  when 'OFR' then 'Processing'
	  when 'TLO' then 'Processing'
	  when 'Proc Svc & Ovh' then 'Processing'
	  else [group]
	end as [group],
	component, value, stack_order, fill_color, fill_pattern
	from data)

  select series, [group], [component], sum(value) as value, stack_order, fill_color, fill_pattern
  from grouped group by series, [group], component, stack_order, fill_color, fill_pattern
  order by
    case when [series]=@basecase_name then 1 else 2 end,
    case [group]
      when 'Drill & Blast' then 1
       when 'Load & Haul' then 2
       when 'Mine Svc & Ovh' then 3
       when 'Beneficiation' then 4
       when 'Processing' then 5
       else 6
    end
end
go

create procedure vdt.sp_get_mine_ghg_emissions (
  @site_name nvarchar(100),
  @dataset_id int,
  @user_id int)
as
begin
set nocount on;
with pivot_data as (
  select coalesce([activity], 'Processing') as activity, attribute, value, base_value
  from vdt.data where attribute in ('CO₂e Diesel', 'CO₂e Electricity')
  and org_group='Mines' and location=@site_name
  and (activity is not null or attribute='CO₂e Electricity')
  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
  and dataset_id=@dataset_id
)
, result as (
  select 'Base Case' as series, activity as [group], attribute as [component], [base_value] as value,
    case when attribute='CO₂e Diesel' then 1 else 2 end as stack_order,
    case when attribute='CO₂e Diesel' then '#000' else '#aaa' end as fill_color, '' as fill_pattern
  from pivot_data
  union all
  select 'Scenario' as series, activity as [group], attribute as [component], [value] as value,
    case when attribute='CO₂e Diesel' then 1 else 2 end as stack_order,
    case when attribute='CO₂e Diesel' then '#ffa500' else '#ffcb6a' end as fill_color, '' as fill_pattern
  from pivot_data
)
select * from result
order by case [group]
  when 'Drill' then 1
  when 'Blast' then 2
  when 'Load' then 3
  when 'Haul' then 4
  else 5
end
end
go

create procedure [vdt].[sp_get_system_kpis]
  @dataset_id int,
  @user_id int
as
begin
  set nocount on;

  select
    [attribute] as KPI,
    case when attribute='EBITDA Improvement' then null when base_value=0 then 0 else value/base_value-1 end as [% Change:{0:2%}],
    value as [Scenario:{0:4,1000000s}],
    base_value as [Baseline:{0:4,1000000s}]
  from vdt.data where org_group='WAIO' and attribute in ('EBITDA', 'EBITDA Improvement', 'CO₂e Emissions')
  and dataset_id=@dataset_id
  AND (select sec_level from vdt.[user] where [user_id] = @user_id) >=  attribute_sec_level;
end
go


create procedure [vdt].[sp_get_mine_cost_waterfall]
  @site_name nvarchar(100),
  @dataset_id int,
  @user_id int
as
begin
  declare @basecase_name nvarchar(100);
  declare @scenario_name nvarchar(100);
  
  select @basecase_name=coalesce(B.dataset_name, V.dataset_name), @scenario_name=V.dataset_name
  from vdt.dataset V left join vdt.scenario S on S.view_dataset_id=V.dataset_id
  left join vdt.dataset B on S.base_dataset_id=B.dataset_id
  where V.dataset_id=@dataset_id;
  
  declare @base_tons float;
  declare @scen_tons float;
  select @scen_tons=value, @base_tons=base_value from vdt.data where org_group='Mines' and location=@site_name
   and [function] is null and attribute='OOR'
   and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
   and dataset_id=@dataset_id;

  if @base_tons = 0 set @base_tons = 1;
  if @scen_tons = 0 set @scen_tons = 1; -- charts won't make sense anyway, so avoid an error message...

  set @basecase_name = 'Base Case';
  set @scenario_name = 'Scenario';

  with totals as (
    select [function] as obj, attribute as attr, value, base_value
    from vdt.data
    where attribute in ('Fixed cost', 'Variable cost')
    and activity is null
    and org_group='Mines' and location=@site_name
	and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
    and dataset_id=@dataset_id)
    
  ,lineitems as (
	select *
    from vdt.data
    where attribute in ('Fixed cost', 'Variable cost')
    and cost_type is null and equipment is null and (activity is not null or [function]='Overheads')
	and (activity <> 'Load' and activity <> 'Haul') and (equipment_type='Plant' or equipment_type is null)
	and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
    and dataset_id=@dataset_id and org_group='Mines' and location=@site_name)
  
  ,subtotals as (
	  select [function] as obj, attribute as attr, sum(value) as value, sum(base_value) as base_value
	  from lineitems group by [function], attribute)

  ,sna as (
  select T.obj, T.attr, T.value - S.value as value, T.base_value - S.base_value as base_value
  from subtotals S inner join totals T on S.obj=T.obj and S.attr=T.attr
  where T.obj <> 'Overheads')

  ,pivot_data as (
    select
      coalesce(activity, 'Mine Overheads') as [group],
      attribute as component,
      value as value,
	  base_value
    from lineitems
    union all
    select
	case S.obj
	  when 'Mining' then 'Mine Svc/Ovh'
	  when 'Processing' then 'Proc Svc/Ovh'
	end as [group],
  	S.attr as component,
  	value as value,
	base_value
    from sna S)

  ,grouped as (
	select case [group]
	  when 'Drill' then 'D&B'
	  when 'Blast' then 'D&B'
	  when 'Load' then 'L&H'
	  when 'Haul' then 'L&H'
	  when 'Load & Haul' then 'L&H'
	  when 'OFH' then 'Proc.'
	  when 'OFR' then 'Proc.'
	  when 'TLO' then 'Proc.'
	  when 'Proc Svc/Ovh' then 'Proc.'
	  when 'Mine Overheads' then 'Mine Ovh.'
	  when 'Beneficiation' then 'Bene.'
	  else [group]
	end as [group],
	value - base_value as delta
	from pivot_data)

  ,agg as (
	select [group], sum([delta]) / @base_tons as [delta] from grouped group by [group]
	union all
	select 'Vol. Dil.' as [group], (sum(value)/@scen_tons - sum(value)/@base_tons) as [delta] from totals where obj is null)
  
  ,withidx as (
    select row_number() over (order by case [group]
         when 'D&B' then 1
         when 'L&H' then 2
         when 'Mine Svc/Ovh' then 3
         when 'Bene.' then 4
         when 'Proc.' then 5
		 when 'Vol. Dil.' then 999
         else 6
       end) as num, [group], [delta]
	from agg
	)

  ,stacked as (
    select cast(0 as int) as num, cast('Base' as nvarchar) as [group], cast(0 as float) as base, sum(base_value)/@base_tons as height, sum(base_value)/@base_tons as cum
	from totals where obj is null
	union all
	select cast(R.num as int), cast(R.[group] as nvarchar) as [group],
	  case when delta < 0 then P.cum + delta else P.cum end as base,
	  abs(delta) as height,
	  P.cum + delta as cum
	from withidx R inner join stacked P on P.num = R.num-1)

  ,pivoted as (
    select [num], [group],
      case when [num]=0 then height else 0 end as total,
	  base as transparent, 
	  case when num=0 then 0 when base=cum then height else 0 end as decrease, 
	  case when num=0 then 0 when base=cum then 0 else height end as increase
    from stacked
    union all
	select 999 as num, 'Scenario' as [group], sum(value)/@scen_tons as total, 0 as transparent, 0 as decrease, 0 as increase
	from totals where obj is null)

  select 'Change in tonnes' as series, [group], 'Total' as [component], total as value, '#0000c0' as fill_color, '' as fill_pattern, 1 as stack_order from pivoted
  union all
  select 'Change in tonnes' as series, [group], 'Transparent' as [component], transparent as value, 'rgba(0,0,0,0)' as fill_color, '' as fill_pattern, 2 as stack_order from pivoted
  union all
  select 'Change in tonnes' as series, [group], 'Increase' as [component], increase as value, '#880000' as fill_color, '' as fill_pattern, 3 as stack_order from pivoted
  union all
  select 'Change in tonnes' as series, [group], 'Decrease' as [component], decrease as value, '#008800' as fill_color, '' as fill_pattern, 4 as stack_order from pivoted
end
go


create procedure [vdt].[sp_get_site_cost_kpis]
  @site_name nvarchar(100),
  @dataset_id int,
  @user_id int
as
select
  [attribute] as KPI,
  attribute_unit as [Unit],
  case when base_value=0 then 0 else value/base_value-1 end as [% Change:{0:2%}],
  value as [Scenario:_format],
  base_value as [Baseline:_format],
  [format] as _format
from vdt.data
left join vdt.unit_format on data.attribute_unit=unit_format.unit
where [function] is null and attribute_is_kpi<>0 and attribute_is_cost=1
and dataset_id=@dataset_id and (org_group='Mines' and location=@site_name or org_group=@site_name)
and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
go

create procedure [vdt].[sp_get_site_volume_kpis]
  @site_name nvarchar(100),
  @dataset_id int,
  @user_id int
as
select
  [attribute] as KPI,
  attribute_unit as [Unit],
  case when base_value=0 then 0 else value/base_value-1 end as [% Change:{0:2%}],
  value as [Scenario:_format],
  base_value as [Baseline:_format],
  [format] as _format
from vdt.data
left join vdt.unit_format on data.attribute_unit=unit_format.unit
where activity is null and attribute_is_kpi<>0 and attribute_is_cost=0
AND (select sec_level from vdt.[user] where [user_id] = @user_id) >=  attribute_sec_level
and dataset_id=@dataset_id  and (org_group='Mines' and location=@site_name or org_group=@site_name)
GO


create procedure [vdt].[sp_get_mine_rehandle_cost_total]
  @site_name nvarchar(100),
  @dataset_id int,
  @user_id int
as
select equipment, attribute_unit as [Unit],
  case when base_value=0 then 0 else value/base_value-1 end as [% Change:{0:2%}],
  value as [Scenario:_format],
  base_value as [Baseline:_format],
  [format] as _format
from vdt.data left join vdt.unit_format on data.attribute_unit=unit_format.unit
where attribute='Rehandle cost' and (base_value<>0 or value <> 0)
  and dataset_id=@dataset_id and location=@site_name
  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
go


create procedure [vdt].[sp_get_mine_rehandle_cost_per_tonne]
  @site_name nvarchar(100),
  @dataset_id int,
  @user_id int
as
select equipment, attribute_unit as [Unit],
  case when base_value=0 then 0 else value/base_value-1 end as [% Change:{0:2%}],
  value as [Scenario:_format],
  base_value as [Baseline:_format],
  [format] as _format
from vdt.data left join vdt.unit_format on data.attribute_unit=unit_format.unit
where attribute='Rehandle cost per tonne' and (base_value<>0 or value <> 0)
  and dataset_id=@dataset_id and location=@site_name
  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
go

create procedure [vdt].[sp_get_mine_process_efficiencies]
  @site_name nvarchar(100),
  @dataset_id int,
  @user_id int
as
select
  coalesce([activity], [function]) as Activity,
  attribute as [Measure],
  case when base_value=0 then 0 else value/base_value-1 end as [% Change:{0:2%}],
  value as [Scenario:_format],
  base_value as [Baseline:_format],
  [format] as _format
from vdt.data  left join vdt.unit_format on data.attribute_unit=unit_format.unit
where equipment is null and attribute in ('Cost per meter', 'Cost per tonnes blasted', 'Cost per PM', 'Cost per OOR')
  and ([function]='Processing' or activity is not null)
  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
and dataset_id=@dataset_id and org_group='Mines' and location=@site_name;
with best as (
  select activity, [function], attribute, min(base_value) as best
  from vdt.data where equipment is null and attribute in ('Cost per meter', 'Cost per tonnes blasted', 'Cost per PM', 'Cost per OOR')
    and ([function]='Processing' or activity is not null)
  group by activity, [function], attribute
)
select
  coalesce(data.[activity], data.[function]) as Activity,
  data.attribute as [Measure],
  case when base_value=0 then 0 else value/base_value-1 end as [% Change:{0:2%}],
  value as [Scenario:_format],
  base_value as [Baseline:_format],
  best as [Best:_format],
  [format] as _format
from vdt.data  left join vdt.unit_format on data.attribute_unit=unit_format.unit
left join best on coalesce(data.activity, data.[function])=coalesce(best.activity, best.[function]) and data.attribute=best.[attribute]
where equipment is null and data.attribute in ('Cost per meter', 'Cost per tonnes blasted', 'Cost per PM', 'Cost per OOR')
  and (data.[function]='Processing' or data.activity is not null)
  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
and dataset_id=@dataset_id and org_group='Mines' and location=@site_name
go

create procedure [vdt].[sp_get_mining_oor]
  @dataset_id int,
  @user_id int
as
select location as [Mine], attribute_unit as [Unit],
  case when base_value=0 then 0 else value/base_value-1 end as [% Change:{0:2%}],
  value as [Scenario:{0:4,1000000s}],
  base_value as [Baseline:{0:4,1000000s}]
from vdt.data
where attribute='OOR' and org_group='Mines' and activity is null
and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
and dataset_id=@dataset_id
order by value desc
go


create procedure [vdt].[sp_get_system_total_cost]
  @dataset_id int,
  @user_id int
as
with costs as (
  select * from vdt.data where attribute_is_aggregate=0 and attribute in ('Fixed cost', 'Variable cost')
  and dataset_id=@dataset_id
  AND (select sec_level from vdt.[user] where [user_id] = @user_id) >=  attribute_sec_level)
  --select * from costs where org_group='WAIO' and [function]='Other'
,summary as (
select org_group as [group], attribute, sum(value) as scenario, sum(base_value) as basecase from costs where org_group<>'WAIO' group by org_group, attribute
union all
select case [function] when 'Logistic & Infrastructure' then 'L&I' else [function] end as [group],
  attribute, sum(value) as scenario, sum(base_value) as basecase from costs where org_group='WAIO' group by [function], attribute
)

,ordering as (
select row_number() over (order by sum(scenario) desc) as ordering, [group] from summary group by [group])

,result as (
select
  ordering,
  'Base Case' as series,
  summary.[group],
  attribute as component,
  case attribute when 'Fixed cost' then 1 else 2 end as stack_order,
  case attribute when 'Fixed cost' then 'black' else '#aaa' end as fill_color,
  '' as fill_pattern,
  basecase as value
from summary inner join ordering on summary.[group]=ordering.[group]
union all
select
  ordering,
  'Scenario' as series,
  summary.[group],
  attribute as component,
  case attribute when 'Fixed cost' then 1 else 2 end as stack_order,
  case attribute when 'Fixed cost' then 'orange' else 'rgb(255,203,106)' end as fill_color,
  '' as fill_pattern,
  scenario as value
from summary inner join ordering on summary.[group]=ordering.[group])

select series, [group], [component], [stack_order], [fill_color], [fill_pattern], value
from result order by ordering, value desc

GO

create procedure vdt.sp_get_system_ghg_emissions (
  @dataset_id int,
  @user_id int)
as
begin
set nocount on;
with pivot_data as (
  select coalesce([location], [org_group]) as [source], attribute, value, base_value
  from vdt.data where attribute in ('CO₂e Diesel', 'CO₂e Electricity')
  and [Function] is null and ([org_group] <> 'Mines' or location is not null)
  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
  and dataset_id=@dataset_id
)
, result as (
  select 'Base Case' as series, [source] as [group], attribute as [component], [base_value] as value,
    case when attribute='CO₂e Diesel' then 1 else 2 end as stack_order,
    case when attribute='CO₂e Diesel' then '#000' else '#aaa' end as fill_color, '' as fill_pattern
  from pivot_data
  union all
  select 'Scenario' as series, [source] as [group], attribute as [component], [value] as value,
    case when attribute='CO₂e Diesel' then 1 else 2 end as stack_order,
    case when attribute='CO₂e Diesel' then '#ffa500' else '#ffcb6a' end as fill_color, '' as fill_pattern
  from pivot_data
)
select * from result
order by case [group]
  when 'Area C' then 1
  when 'Yandi' then 2
  when 'Whaleback' then 3
  when 'Eastern Ridge' then 4
  when 'Jimblebar' then 5
  when 'Rail' then 6
  when 'Port' then 7
  else 8
end
end
go

create procedure [vdt].[sp_get_system_cost_waterfall]
  @dataset_id int,
  @user_id int
as

declare @base_tons float;
declare @scen_tons float;

select @base_tons=base_value, @scen_tons=value from vdt.data
where attribute='Value Chain Tonnes' and org_group='Port' 
and dataset_id=@dataset_id
AND (select sec_level from vdt.[user] where [user_id] = @user_id) >=  attribute_sec_level;

if @base_tons = 0 set @base_tons = 1;
if @scen_tons = 0 set @scen_tons = 1;

with costs as (
  select * from vdt.data where attribute_is_aggregate=0 and attribute in ('Fixed cost', 'Variable cost')
  and dataset_id=@dataset_id
  AND (select sec_level from vdt.[user] where [user_id] = @user_id) >=  attribute_sec_level)
  --select * from costs where org_group='WAIO' and [function]='Other'

,summary as (
select org_group as [group], sum(value) as scenario, sum(base_value) as basecase from costs where org_group<>'WAIO' group by org_group
union all
select case [function] when 'Logistic & Infrastructure' then 'L&I' else [function] end as [group],
  sum(value) as scenario, sum(base_value) as basecase from costs where org_group='WAIO' group by [function]
)

,ordering as (
select row_number() over (order by scenario desc) as ordering, [group], (scenario - basecase) / @base_tons as delta from summary
union all
select (select count(*)+1 from summary) as ordering, 'Vol. Dil.' as [group], (sum(value)/@scen_tons - sum(value)/@base_tons) as [delta] from costs)

,waterfall as (
select cast(0 as int) as num, cast('Base' as nvarchar) as [group], cast(0 as float) as base, sum(base_value) / @base_tons as height, sum(base_value) / @base_tons as cum from costs
union all
select cast(N.ordering as int) as num, cast(N.[group] as nvarchar), case when delta < 0 then delta + P.cum else P.cum end as base, abs(delta) as height, P.cum + delta as cum
from ordering N inner join waterfall P on N.ordering=P.num + 1
)

,pivoted as (
select [num], [group],
  case when [num]=0 then height else 0 end as total,
  base as transparent, 
  case when num=0 then 0 when base=cum then height else 0 end as decrease, 
  case when num=0 then 0 when base=cum then 0 else height end as increase
from waterfall
union all
select 999 as num, 'Scenario' as [group], sum(value)/@scen_tons as total, 0 as transparent, 0 as decrease, 0 as increase from costs)

select 'Change in unit cost' as series, [group], 'Total' as [component], total as value, '#0000c0' as fill_color, '' as fill_pattern, 1 as stack_order from pivoted
union all
select 'Change in unit cost' as series, [group], 'Transparent' as [component], transparent as value, 'rgba(0,0,0,0)' as fill_color, '' as fill_pattern, 2 as stack_order from pivoted
union all
select 'Change in unit cost' as series, [group], 'Increase' as [component], increase as value, '#880000' as fill_color, '' as fill_pattern, 3 as stack_order from pivoted
union all
select 'Change in unit cost' as series, [group], 'Decrease' as [component], decrease as value, '#008800' as fill_color, '' as fill_pattern, 4 as stack_order from pivoted

GO

create procedure [vdt].[sp_get_system_tonnes]
  @dataset_id int,
  @user_id int
as

with rawrawdata as (
select org_group, attribute,
  case when attribute='Value Chain Stock' then 0 else value end as value,
  case when attribute='Value Chain Stock' then 0 else base_value end as base_value
from vdt.data
where attribute in ('Value Chain Tonnes', 'Value Chain Stock', 'Headroom', 'Internal Capacity') and [function] is null
  and location is null
  and dataset_id=@dataset_id
  AND (select sec_level from vdt.[user] where [user_id] = @user_id) >=  attribute_sec_level
)
,rawdata as (
select * from rawrawdata where attribute <> 'Internal Capacity'
)
,capacity as (
  select org_group, sum(base_value) as base_capacity, sum(value) as capacity
  from rawrawdata where attribute in ('Internal Capacity', 'Value Chain Stock') group by org_group
)
,stockplustonnes as (
  select org_group, sum(base_value) as base_spt, sum(value) as spt
  from rawrawdata where attribute not in ('Headroom', 'Internal Capacity') group by org_group
)
,data as (
select D.org_group, D.attribute,
  case when D.attribute='Headroom' then
    case when S.base_spt > C.base_capacity then 0 else C.base_capacity - S.base_spt end
  else D.base_value end as base_value,
  case when D.attribute='Headroom' then
    case when S.spt > C.capacity then 0 else C.capacity - S.spt end
  else D.value end as value from rawdata D
  left join capacity C on D.org_group=C.org_group
  left join stockplustonnes S on D.org_group=S.org_group
)
,series as (
select 'Base Case' as series, org_group as [group], attribute as [component], base_value as value,
  case attribute when 'Headroom' then 3 when 'Value Chain Stock' then 2 else 1 end as stack_order,
  case attribute when 'Headroom' then 'rgb(216,222,231)' when 'Value Chain Tonnes' then 'black' else '#aaa' end as fill_color,
  '' as fill_pattern
from data
union all
select 'Scenario' as series, org_group as [group], attribute as [component], value as value,
  case attribute when 'Headroom' then 3 when 'Value Chain Stock' then 2 else 1 end as stack_order,
  case attribute when 'Headroom' then 'rgb(216,222,231)' when 'Value Chain Tonnes' then 'orange' else 'rgb(255,203,106)' end as fill_color,
  '' as fill_pattern
from data
)
select * from series
order by case [group]
  when 'Mines' then 1
  when 'Rail' then 2
  when 'Port' then 3
  else 4
end
go


create procedure [vdt].[sp_get_rail_total_cost]
  @dataset_id int,
  @user_id int
as

with costs as (
  select * from vdt.data where attribute_is_aggregate=0 and attribute in ('Fixed cost', 'Variable cost')
  and org_group='Rail'
  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
  and dataset_id=@dataset_id)

,summary as (
select [function] as [group], attribute, sum(value) as scenario, sum(base_value) as basecase from costs group by [function], attribute
)

,ordering as (
select row_number() over (order by sum(scenario) desc) as ordering, [group] from summary group by [group])

,result as (
select
  ordering,
  'Base Case' as series,
  summary.[group],
  attribute as component,
  case attribute when 'Fixed cost' then 1 else 2 end as stack_order,
  case attribute when 'Fixed cost' then 'black' else '#aaa' end as fill_color,
  '' as fill_pattern,
  basecase as value
from summary inner join ordering on summary.[group]=ordering.[group]
union all
select
  ordering,
  'Scenario' as series,
  summary.[group],
  attribute as component,
  case attribute when 'Fixed cost' then 1 else 2 end as stack_order,
  case attribute when 'Fixed cost' then 'orange' else 'rgb(255,203,106)' end as fill_color,
  '' as fill_pattern,
  scenario as value
from summary inner join ordering on summary.[group]=ordering.[group])

select series, [group], [component], [stack_order], [fill_color], [fill_pattern], value
from result order by ordering, value desc
go

create procedure [vdt].[sp_get_rail_cost_waterfall]
  @dataset_id int,
  @user_id int
as

declare @base_tons float;
declare @scen_tons float;

select @base_tons=base_value, @scen_tons=value from vdt.data
where attribute='Value Chain Tonnes'
and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
and org_group='Rail' and dataset_id=@dataset_id;

if @base_tons = 0 set @base_tons = 1;
if @scen_tons = 0 set @scen_tons = 1;

with costs as (
  select * from vdt.data where attribute_is_aggregate=0 and attribute in ('Fixed cost', 'Variable cost')
  and org_group='Rail'
  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
  and dataset_id=@dataset_id)
,summary as (
select [function] as [group], sum(value) as scenario, sum(base_value) as basecase from costs group by [function]
)

,ordering as (
select row_number() over (order by scenario desc) as ordering, [group], (scenario - basecase) / @base_tons as delta from summary
union all
select (select count(*)+1 from summary) as ordering, 'Vol. Dil.' as [group], (sum(value)/@scen_tons - sum(value)/@base_tons) as [delta] from costs)

,waterfall as (
select cast(0 as int) as num, cast('Base' as nvarchar) as [group], cast(0 as float) as base, sum(base_value) / @base_tons as height, sum(base_value) / @base_tons as cum from costs
union all
select cast(N.ordering as int) as num, cast(N.[group] as nvarchar), case when delta < 0 then delta + P.cum else P.cum end as base, abs(delta) as height, P.cum + delta as cum
from ordering N inner join waterfall P on N.ordering=P.num + 1
)

,pivoted as (
select [num], [group],
  case when [num]=0 then height else 0 end as total,
  base as transparent, 
  case when num=0 then 0 when base=cum then height else 0 end as decrease, 
  case when num=0 then 0 when base=cum then 0 else height end as increase
from waterfall
union all
select 999 as num, 'Scenario' as [group], sum(value)/@scen_tons as total, 0 as transparent, 0 as decrease, 0 as increase from costs)

select 'Change in unit cost' as series, [group], 'Total' as [component], total as value, '#0000c0' as fill_color, '' as fill_pattern, 1 as stack_order from pivoted
union all
select 'Change in unit cost' as series, [group], 'Transparent' as [component], transparent as value, 'rgba(0,0,0,0)' as fill_color, '' as fill_pattern, 2 as stack_order from pivoted
union all
select 'Change in unit cost' as series, [group], 'Increase' as [component], increase as value, '#880000' as fill_color, '' as fill_pattern, 3 as stack_order from pivoted
union all
select 'Change in unit cost' as series, [group], 'Decrease' as [component], decrease as value, '#008800' as fill_color, '' as fill_pattern, 4 as stack_order from pivoted

go

create procedure [vdt].[sp_get_port_capacity_chart]
	@dataset_id int,
	@user_id int
AS 
BEGIN 
  SET NOCOUNT ON;
  with rawdata as (
    select coalesce(location, 'Car Dumpers') as [process], attribute,
	case when attribute='Value Chain Stock' then 0 else value end as value,
	case when attribute='Value Chain Stock' then 0 else base_value end as base_value
    from vdt.data where attribute in ('Value Chain Tonnes', 'Value Chain Stock', 'Headroom', 'Internal Capacity')
    and  org_group='Port' and [function] is not null
	and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
    and dataset_id=@dataset_id
  )
  ,data as (
    select * from rawdata where attribute <> 'Internal Capacity'
  )
  ,capacity as (
  select process, sum(value) as capacity
  from rawdata where attribute in ('Value Chain Stock', 'Internal Capacity') group by process
  )
  ,production as (
    select process, sum(value) as value, sum(base_value) as base_value
    from data where attribute <> 'Headroom' group by process
  )
  ,points as (
    select * from data where attribute <> 'Headroom'
    union all
    select P.process, 'Capacity Delta' as attribute, C.capacity - P.value as value, C.capacity - P.base_value as base_value
    from production P inner join capacity C on P.process=C.process
  )
  ,result as (
    select 'Base Case' as series, process as [group], attribute as [component], base_value as value,
      case attribute when 'Value Chain Tonnes' then 1 when 'Value Chain Stock' then 2 else 3 end as stack_order,
      case attribute when 'Capacity Delta' then 'rgb(216,222,231)' when 'Value Chain Tonnes' then 'black' else '#aaa' end as fill_color,
      case attribute when 'Capacity Delta' then 'top_dash' else 'solid' end as fill_pattern
    from points
    union all
    select 'Scenario' as series, process as [group], attribute as [component], value as value,
      case attribute when 'Value Chain Tonnes' then 1 when 'Value Chain Stock' then 2 else 3 end as stack_order,
      case attribute when 'Capacity Delta' then 'rgb(216,222,231)' when 'Value Chain Tonnes' then 'orange' else 'rgb(255,203,106)' end as fill_color,
      case attribute when 'Capacity Delta' then 'top_dash' else 'solid' end as fill_pattern
    from points
  )
  select * from result
  order by series, case [group] when 'Car Dumpers' then 1 when 'Shiploaders' then 3 else 2 end
END
go

create procedure [vdt].[sp_get_port_total_cost]
  @dataset_id int,
  @user_id int
as

with costs as (
  select * from vdt.data where attribute_is_aggregate=0 and attribute in ('Fixed cost', 'Variable cost')
  and org_group='Port'
  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
  and dataset_id=@dataset_id)

,summary as (
select [function] as [group], attribute, sum(value) as scenario, sum(base_value) as basecase from costs group by [function], attribute
)

,ordering as (
select row_number() over (order by sum(scenario) desc) as ordering, [group] from summary group by [group])

,result as (
select
  ordering,
  'Base Case' as series,
  summary.[group],
  attribute as component,
  case attribute when 'Fixed cost' then 1 else 2 end as stack_order,
  case attribute when 'Fixed cost' then 'black' else '#aaa' end as fill_color,
  '' as fill_pattern,
  basecase as value
from summary inner join ordering on summary.[group]=ordering.[group]
union all
select
  ordering,
  'Scenario' as series,
  summary.[group],
  attribute as component,
  case attribute when 'Fixed cost' then 1 else 2 end as stack_order,
  case attribute when 'Fixed cost' then 'orange' else 'rgb(255,203,106)' end as fill_color,
  '' as fill_pattern,
  scenario as value
from summary inner join ordering on summary.[group]=ordering.[group])

select series, [group], [component], [stack_order], [fill_color], [fill_pattern], value
from result order by ordering, value desc
go

create procedure [vdt].[sp_get_port_cost_waterfall]
  @dataset_id int,
  @user_id int
as

declare @base_tons float;
declare @scen_tons float;

select @base_tons=base_value, @scen_tons=value from vdt.data
where attribute='Value Chain Tonnes' and org_group='Port' 
and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
and dataset_id=@dataset_id;

if @base_tons = 0 set @base_tons = 1;
if @scen_tons = 0 set @scen_tons = 1;

with costs as (
  select * from vdt.data where attribute_is_aggregate=0 and attribute in ('Fixed cost', 'Variable cost')
  and org_group='Port'
  and (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  attribute_sec_level
  and dataset_id=@dataset_id)
,summary as (
select [function] as [group], sum(value) as scenario, sum(base_value) as basecase from costs group by [function]
)

,ordering as (
select row_number() over (order by scenario desc) as ordering, [group], (scenario - basecase) / @base_tons as delta from summary
union all
select (select count(*)+1 from summary) as ordering, 'Vol. Dil.' as [group], (sum(value)/@scen_tons - sum(value)/@base_tons) as [delta] from costs)

,waterfall as (
select cast(0 as int) as num, cast('Base' as nvarchar) as [group], cast(0 as float) as base, sum(base_value) / @base_tons as height, sum(base_value) / @base_tons as cum from costs
union all
select cast(N.ordering as int) as num, cast(N.[group] as nvarchar), case when delta < 0 then delta + P.cum else P.cum end as base, abs(delta) as height, P.cum + delta as cum
from ordering N inner join waterfall P on N.ordering=P.num + 1
)

,pivoted as (
select [num], [group],
  case when [num]=0 then height else 0 end as total,
  base as transparent, 
  case when num=0 then 0 when base=cum then height else 0 end as decrease, 
  case when num=0 then 0 when base=cum then 0 else height end as increase
from waterfall
union all
select 999 as num, 'Scenario' as [group], sum(value)/@scen_tons as total, 0 as transparent, 0 as decrease, 0 as increase from costs)

select 'Change in unit cost' as series, [group], 'Total' as [component], total as value, '#0000c0' as fill_color, '' as fill_pattern, 1 as stack_order from pivoted
union all
select 'Change in unit cost' as series, [group], 'Transparent' as [component], transparent as value, 'rgba(0,0,0,0)' as fill_color, '' as fill_pattern, 2 as stack_order from pivoted
union all
select 'Change in unit cost' as series, [group], 'Increase' as [component], increase as value, '#880000' as fill_color, '' as fill_pattern, 3 as stack_order from pivoted
union all
select 'Change in unit cost' as series, [group], 'Decrease' as [component], decrease as value, '#008800' as fill_color, '' as fill_pattern, 4 as stack_order from pivoted

go

create procedure vdt.sp_get_system_site_margins
  @dataset_id int,
  @user_id int
as
select
  [attribute] as [Site],
  attribute_unit as [Unit],
  case when base_value=0 then 0 else value/base_value-1 end as [% Change:{0:2%}],
  value as [Scenario:_format],
  base_value as [Baseline:_format],
  [format] as _format
from vdt.data
left join vdt.unit_format on data.attribute_unit=unit_format.unit
where [org_group]='WAIO' and [function] like '% Margin%' and location is null and equipment is null and [activity] is null
AND (select sec_level from vdt.[user] where [user_id] = @user_id) >=  attribute_sec_level
and dataset_id=@dataset_id

go

create procedure vdt.sp_get_system_product_margins
  @dataset_id int,
  @user_id int
as
select
  [activity] as Product,
  [attribute] as Measure,
  attribute_unit as [Unit],
  case when base_value=0 then 0 else value/base_value-1 end as [% Change:{0:2%}],
  value as [Scenario:_format],
  base_value as [Baseline:_format],
  [format] as _format
from vdt.data
left join vdt.unit_format on data.attribute_unit=unit_format.unit
where [org_group]='WAIO' and [function]='Product Margin' and location is null and equipment is null
and attribute like '%Margin%'
AND (select sec_level from vdt.[user] where [user_id] = @user_id) >=  attribute_sec_level
and dataset_id=@dataset_id
go



