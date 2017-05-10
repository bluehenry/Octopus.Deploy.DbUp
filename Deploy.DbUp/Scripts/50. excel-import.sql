--USE *database_name*

CREATE TYPE [staging].[ExcelImportType] AS TABLE(
	[Sheet] [nvarchar](100) NULL,
	[Org_Group] [nvarchar](100) NULL,
	[Location] [nvarchar](100) NULL,
	[Function] [nvarchar](100) NULL,
	[Activity] [nvarchar](100) NULL,
	[Workstream] [nvarchar](100) NULL,
	[Equipment_Type] [nvarchar](100) NULL,
	[Equipment] [nvarchar](100) NULL,
	[Cost_Type] [nvarchar](100) NULL,
	[Product] [nvarchar](100) NULL,
	[Attribute] [nvarchar](100) NULL,
	[Unit] [nvarchar](100) NULL,
	[Cost?] [nvarchar](100) NULL,
	[Value] [float] NULL
)
GO


create procedure [staging].[sp_import_excel]
  @dataset_name nvarchar(200),
  @model_id int,
  @user_id int,
  @description nvarchar(1000),
  @category nvarchar(100),
  @data staging.ExcelImportType readonly
as
begin
  set nocount on;

  declare @results table (severity nvarchar(20), [message] nvarchar(200));
  declare @shouldCopyData bit = 0
  -- severity is one of: info, warning, error
  insert into @results (severity, [message]) values ('info', 'Starting data import');

/***********************SUMMARY************************** 
Temp tables that exist are the following:
• #Final_Calc_Dataset - The input sheet
• #MiningCostDrivers - Majority of the cost drivers for mining
• #MiningCostRates;   
• #retrieve_attribute_id;    
• #final_attribute;   
• #fulljoin;   
• #ProductionTime - calcs to convert the hours into percentages for production data 
• #CostRates  - Includes the cost rate calc
• #ActiveRakesCalc  
• #HaulAdjustments - Includes the backcals for haul adjustments.
• #LoadAdjustments - Includes the backcals for load adjustments.
• #PortCostDrivers - Includes the port back calculations
*/

select * into #Final_Calc_Dataset from @data;

-- set blanks to null
update #Final_Calc_Dataset set Org_Group = null where Org_Group = '' ;
update #Final_Calc_Dataset set Location = null where Location = '' ;
update #Final_Calc_Dataset set [Function] = null where [Function] = '' ;
update #Final_Calc_Dataset set Activity = null where Activity = '' ;
update #Final_Calc_Dataset set Equipment_Type = null where Equipment_Type = '' ;
update #Final_Calc_Dataset set Equipment = null where Equipment = '' ;
update #Final_Calc_Dataset set Workstream = null where Workstream = '' ;
update #Final_Calc_Dataset set Cost_Type = null where Cost_Type = '' ;
update #Final_Calc_Dataset set Product = null where Product = '' ;
update #Final_Calc_Dataset set Attribute = null where Attribute = '' ;

 --Table updates to conform to the attribute fact table and back calculations
update #Final_Calc_Dataset set Equipment = 'Drill Stock', Equipment_Type='Stockpile' where Equipment = 'Drill Inventory'  --- DKT
update #Final_Calc_Dataset set attribute = 'Tonnes blasted' where attribute = 'Tonnes of ore blasted' ;
update #Final_Calc_Dataset set Equipment_Type = 'Loader' where Equipment_Type = 'Loaders' ;
update #Final_Calc_Dataset set Equipment_Type = 'Excavator' where Equipment_Type = 'Excavators' ;
update #Final_Calc_Dataset set Equipment_Type = 'Truck' where Equipment_Type = 'Trucks' ;
update #Final_Calc_Dataset set Attribute = 'Closing stock' where  Attribute = 'Closing balance' and org_group = 'Port' ;
update #Final_Calc_Dataset set Attribute = 'L/F split' where  Attribute = 'L/F split ratio' ;
update #Final_Calc_Dataset set Attribute = 'Total movement tonnes' where	Attribute = 'Total movement'	and Activity is not null 
																		and Workstream is not null and Equipment_Type is not null 
																		and Equipment is not null ;
update #Final_Calc_Dataset set Attribute = 'Target Tonnes' where Attribute = 'Throughput'; 
update #Final_Calc_Dataset set Attribute = 'Average no. of drills' where Attribute = 'Average no of drills'; 
update #Final_Calc_Dataset set Attribute = 'Starved/blocked' where Attribute = 'Starved/block time'; 
update #Final_Calc_Dataset set Attribute = 'Unscheduled process downtime excl. starve/block' where Attribute = 'Unscheduled operating loss time (exclude starve/block time)'; 
update #Final_Calc_Dataset set Equipment_Type = 'CFR Ships' Where attribute = 'CFR ratio'
update #Final_Calc_Dataset set Equipment_Type = 'FOB Ships' Where attribute = 'FOB ratio'


-------------------######################################################################################################################-----------------------	
-------------------######################################################################################################################-----------------------
-------------------######################################################################################################################-----------------------
----------------------------------------------------------------------------Mining------------------------------------------------------------------------------

--=====================================================================INITIAL CHECKS===========================================================================

-- check if tonnes of ore blasted has missing values
insert into @results (severity, [message])
select 'warning', Location + ' tonnes blasted is zero or blank' from #Final_Calc_Dataset where Attribute='Tonnes blasted' and (Value is null or Value = 0)


-- check if drill stock has missing values
insert into @results (severity, [message])
select 'warning', Location + ' drill stock is zero or blank' from #Final_Calc_Dataset where Equipment = 'Drill Stock' and Attribute='Closing balance' and (Value is null or Value = 0)
;

--=====================================================================Calculations===========================================================================
With DrillDensity as (
			select distinct  C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Density'  as 'Attribute', c.Unit, c.[Cost?] 
				,case when pb.[Value] = 0 then '0' else (pa.[Value]) / pb.[Value] end as  [Value]
				FROM #Final_Calc_Dataset C
				inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.activity,'') = coalesce(pa.activity,'') 
				inner join #Final_Calc_Dataset pb on coalesce(c.Activity, '') = coalesce(pb.Activity,'') and coalesce(c.org_group ,'') = coalesce(pb.org_group ,'') and coalesce(c.location  ,'') = coalesce(pb.Location  ,'') and coalesce(c.[function],'') = coalesce(pb.[function],'') and coalesce(c.equipment,'') = coalesce(pb.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pb.equipment_type,'') and coalesce(c.activity,'') = coalesce(pb.activity,'')
				where c.Activity = 'Drill' and pa.Attribute = 'Tonnes drilled' and pb.Attribute = 'Volume drilled' and c.Unit = 'tpa')
			-- drill inventory opening balance
	,DrillInventoryOpeningBal as		
			(select distinct  C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], 'Drill Stock' as 'Equipment', c.Cost_Type, c.Product, 'Opening Balance'  as 'Attribute', c.Unit, c.[Cost?] 
				,c.Value+ pb.Value - (select sum(pa.Value))  as  [Value]--, c.Value ggsg, pb.value sdsfs, pa.value sdsa
				FROM #Final_Calc_Dataset C
				inner join #Final_Calc_Dataset pa on  coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') 
				inner join #Final_Calc_Dataset pb on  coalesce(c.org_group ,'') = coalesce(pb.org_group ,'') and coalesce(c.location  ,'') = coalesce(pb.Location  ,'') and coalesce(c.[function],'') = coalesce(pb.[function],'') 
				where c.Equipment = 'Drill Stock'  and pb.attribute='Tonnes blasted' and pa.attribute= 'Tonnes Drilled' and pa.Equipment like 'Drill Production%'
				group by C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], c.Cost_Type, c.Product, c.Unit, c.[Cost?],pa.Value, pb.Value, c.Value)
	,DisplacedVolume as 
			-- Displaced Volume
			(select distinct  C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Displaced Volume'  as 'Attribute', c.Unit, c.[Cost?] 
				,case when c.value*pb.value = 0 then 0 else pa.[Value]/(c.[Value]*pb.[Value]) end as  [Value]
				FROM #Final_Calc_Dataset C
				inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.activity,'') = coalesce(pa.activity,'') 
				inner join #Final_Calc_Dataset pb on coalesce(c.Activity, '') = coalesce(pb.Activity,'') and coalesce(c.org_group ,'') = coalesce(pb.org_group ,'') and coalesce(c.location  ,'') = coalesce(pb.Location  ,'') and coalesce(c.[function],'') = coalesce(pb.[function],'') and coalesce(c.equipment,'') = coalesce(pb.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pb.equipment_type,'') and coalesce(c.activity,'') = coalesce(pb.activity,'')
				where pa.attribute='Volume drilled' and pb.attribute='Meters drilled per rig' and c.Attribute = 'Average no. of drills')
			-- cost explosive per kg
	,CostPerExplosive as		
			( select distinct  C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Cost explosive per kg'  as 'Attribute', c.Unit, c.[Cost?] 
				,case when (pa.[Value]*pb.[Value]) = 0 then 0 else c.[Value]/(pa.[Value]*pb.[Value]) end as  [Value]
				--, c.value as llsk, pa.value sdjfm, pb.value sjjfs 
				FROM #Final_Calc_Dataset C
				inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.activity,'') = coalesce(pa.activity,'') 
				inner join #Final_Calc_Dataset pb on coalesce(c.Activity, '') = coalesce(pb.Activity,'') and coalesce(c.org_group ,'') = coalesce(pb.org_group ,'') and coalesce(c.location  ,'') = coalesce(pb.Location  ,'') and coalesce(c.[function],'') = coalesce(pb.[function],'') and coalesce(c.equipment,'') = coalesce(pb.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pb.equipment_type,'') and coalesce(c.activity,'') = coalesce(pb.activity,'')
				where pa.activity='Blast' and pa.attribute='Powder factor' and pb.attribute='Tonnes blasted' and c.cost_type = 'Consumables')
	,DieselPricePerLitreHaul as (
			-- Equipment Price per Litre, check values being imported - Haul
			select distinct  
			C.[Sheet], c.[Org_Group], C.Location, C.[function], 'Haul' as 'Activity', 'Operations' as 'Workstream', C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Diesel price per litre'  as 'Attribute', c.Unit, c.[Cost?]
					,case when (pa.[Value]=0 or pa.[Value] is NULL) then 1 else (c.[Value]/ pa.[Value]) end as  [Value]
					FROM #Final_Calc_Dataset c
					left join #Final_Calc_Dataset pa on pa.attribute='Litres consumed' and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'')  and coalesce(c.equipment,'') = coalesce(pa.equipment,'')
					where c.org_group = 'Mines' and  c.Cost_Type = 'Diesel' and c.Activity = 'Haul')
	,DieselPricePerLitreLoad as (
			-- Equipment Price per Litre, check values being imported - Load
			select distinct  
			C.[Sheet], c.[Org_Group], C.Location, C.[function], 'Load' as 'Activity', 'Operations' as 'Workstream', C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Diesel price per litre'  as 'Attribute', c.Unit, c.[Cost?]
					,case when (pa.[Value]=0 or pa.[Value] is NULL) then 1 else (c.[Value] / pa.[Value]) end as  [Value]
					FROM #Final_Calc_Dataset c
					left join #Final_Calc_Dataset pa on pa.attribute='Litres consumed' and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'')  and coalesce(c.equipment,'') = coalesce(pa.equipment,'')
					where c.org_group = 'Mines' and  c.Cost_Type = 'Diesel' and c.Activity = 'Load')
				-- Burn Rate, only applicable to load
	,BurnRateLitresGiven as (
			select distinct  C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, 'Diesel' as Cost_Type, c.Product, 'Burn Rate'  as 'Attribute', c.Unit, c.[Cost?]					
					,case when pa.[Value] in ('0', NULL)  or pb.[Value] in ('0', NULL) 
					then c.[Value] else (c.[Value]/(pa.[Value]/pb.[Value])) end  as  [Value]
					FROM #Final_Calc_Dataset c
					inner join #Final_Calc_Dataset pa on coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'')  and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.activity,'') = coalesce(pa.activity,'')
					inner join #Final_Calc_Dataset pb on coalesce(c.org_group ,'') = coalesce(pb.org_group ,'') and coalesce(c.location  ,'') = coalesce(pb.Location  ,'') and coalesce(c.[function],'') = coalesce(pb.[function],'')  and coalesce(c.equipment,'') = coalesce(pb.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pb.equipment_type,'') and coalesce(c.activity,'') = coalesce(pb.activity,'')
					where c.attribute='Litres consumed' and  pa.attribute='Total movement tonnes' and pb.attribute = 'Net rate' and c.Value <> 0)
	,BurnRateNoLitres as (
		select distinct  C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, 'Diesel' as Cost_Type, c.Product, 'Burn Rate'  as 'Attribute', c.Unit, c.[Cost?]					
					,c.Value
					FROM #Final_Calc_Dataset c
					left join #Final_Calc_Dataset pa on pa.attribute = 'Litres consumed' and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'')  and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.activity,'') = coalesce(pa.activity,'')
					where c.attribute='Variable cost' and c.cost_type='Diesel' and c.Activity in ('Load', 'Haul') and  (pa.attribute is null or pa.Value=0))
	,BurnRate as (
	  select * from BurnRateLitresGiven union all select * from BurnRateNoLitres)
	,RhTonnesperc as (
			-- rehandle tonnes %
			select distinct  c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'RH tonnes %'  as 'Attribute', c.Unit, c.[Cost?]  
					,case when pa.[Value] in ('0', NULL)  
					then 0 else (c.[Value]/pa.[Value]) end  as  [Value]
					FROM #Final_Calc_Dataset c
					inner join #Final_Calc_Dataset pa on coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'')  and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.activity,'') = coalesce(pa.activity,'')
					where c.attribute='Rehandle movement' and  pa.attribute='Total movement tonnes'
			 )  

				 Select a.* into #MiningCostDrivers from (  
				select * from DrillDensity union all
				--select * from DrillInventoryOpeningBal union all
				select * from DisplacedVolume union all
				select * from CostPerExplosive union all
				select * from DieselPricePerLitreHaul union all
				select * from DieselPricePerLitreLoad union all
				select * from BurnRate union all
				select * from RhTonnesperc ) a;

--select * from #MiningCostDrivers
-- Check drill stock sizes, if density is greater than a particular value
-- investigate cost per kg explosives
-- if the sum of drill stock is larger than x% of total tonnes then flag
insert into @results (severity, [message])
select 'warning', Location + ' cost explosive per kg calculated to be less than $0.01' from #MiningCostDrivers where Attribute='Cost explosive per kg' and Value < 0.01
;

with FleetsWithDieselBurnRate as (
select Location, Activity, Equipment_Type, Equipment from #Final_Calc_Dataset
where activity in ('Load', 'Haul') and Cost_Type='Diesel' and Attribute='Variable Cost' and (Value is null or Value <> 0)
)
insert into @results (severity, [message])
select 'warning', F.Location + ' ' + F.Equipment + ' diesel burn rate/price not calculated - check tonnes'
from FleetsWithDieselBurnRate F left join #MiningCostDrivers C on F.Location=C.Location and F.Activity=C.Activity
and F.Equipment_Type=C.Equipment_Type and F.Equipment=C.Equipment and C.Attribute = 'Diesel price per litre'
where (C.Value is null or C.Value = 0)
;
--Net rate calcs & Checks
-- OEE BackCalcs


with CalendarTime as (
			-- Calendar Time hours
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Calendar time'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value as Value
							from #Final_Calc_Dataset c
							where c.Attribute = 'Calendar time' )

	, CalenderTimeNotAggregate as (
				-- Calendar Time not aggregated
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Calendar time Not Aggregate'  as 'Attribute', c.Unit, c.[Cost?]
							, case when ca.Value = 0 then 0 else c.Value/ca.Value end as Value
							from CalendarTime c
							inner join #Final_Calc_Dataset ca on c.Activity = ca.Activity and c.Equipment = ca.Equipment
							where c.Attribute = 'Calendar time' and ca.Attribute = 'Average number of units'
									) 

	,StandbyTime as (
			-- Standby Time
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Standby time'  as 'Attribute', c.Unit, c.[Cost?]
						, c.Value as Value
			from #Final_Calc_Dataset c
			where c.Attribute = 'Standby time' )

	,ScheduledEquipDowntime as (
			-- scheduled equipment downtime
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Scheduled equipment downtime raw'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value as Value
							from #Final_Calc_Dataset c
							where c.Attribute = 'Scheduled equipment downtime' )-- select * from ScheduledEquipDowntime

	,UnscheduledEquipDowntime as (
			-- unscheduled equipment downtime
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Unscheduled equipment downtime raw'  as 'Attribute', c.Unit, c.[Cost?]
						, c.Value as Value
						from #Final_Calc_Dataset c
						where c.Attribute = 'Unscheduled equipment downtime' )

	,ScheduledProcessDowntime as (
			-- Scheduled Process Downtime
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Scheduled process downtime raw'  as 'Attribute', c.Unit, c.[Cost?]
						, c.Value as Value
						from #Final_Calc_Dataset c
						where c.Attribute = 'Scheduled process downtime' )
--  Unscheduled process downtime excl. starve/block
	,UnscheduledProcessDowntime_exclsb as (
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Unscheduled process downtime excl. starve/block raw'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value as Value
							from #Final_Calc_Dataset c
							where c.Attribute in ('Unscheduled process downtime', 'Unscheduled process downtime excl. starve/block')) 
-- Starve Block time
	,Starve_Block as (
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Starved/blocked time raw'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value as Value
							from #Final_Calc_Dataset c
							where c.Attribute in ('Starved/blocked', 'Starved/blocked time')) --select * from Starve_Block
-- Production Time
	,ProductionTime as ( select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Production Time'  as 'Attribute', c.Unit, c.[Cost?]
									,ISNULL(c.Value,0) - (ISNULL(ca.Value,0) + ISNULL(cb.Value,0) + ISNULL(cc.Value,0) + ISNULL(cd.Value,0) +  ISNULL(ce.Value,0) + ISNULL(cf.Value,0))  as Value
									--,c.Value ss,ca.Value sdas,cb.Value dsad,cc.Value fsaa,cd.Value fsa ,ce.Value fssa ,cf.Value faa
						from #Final_Calc_Dataset c
						left join StandbyTime							ca on coalesce(c.org_group ,'') = coalesce(ca.org_group ,'') and coalesce(c.location  ,'') = coalesce(ca.Location  ,'') and coalesce(c.[function],'') = coalesce(ca.[function],'')  and coalesce(c.equipment,'') = coalesce(ca.equipment,'') and coalesce(c.equipment_type,'') = coalesce(ca.equipment_type,'') and coalesce(c.activity,'') = coalesce(ca.activity,'') and coalesce(c.workstream,'') = coalesce(ca.workstream,'')
						left join ScheduledEquipDowntime				cb on coalesce(c.org_group ,'') = coalesce(cb.org_group ,'') and coalesce(c.location  ,'') = coalesce(cb.Location  ,'') and coalesce(c.[function],'') = coalesce(cb.[function],'')  and coalesce(c.equipment,'') = coalesce(cb.equipment,'') and coalesce(c.equipment_type,'') = coalesce(cb.equipment_type,'') and coalesce(c.activity,'') = coalesce(cb.activity,'') and coalesce(c.workstream,'') = coalesce(cb.workstream,'')
						left join UnscheduledEquipDowntime				cc on coalesce(c.org_group ,'') = coalesce(cc.org_group ,'') and coalesce(c.location  ,'') = coalesce(cc.Location  ,'') and coalesce(c.[function],'') = coalesce(cc.[function],'')  and coalesce(c.equipment,'') = coalesce(cc.equipment,'') and coalesce(c.equipment_type,'') = coalesce(cc.equipment_type,'') and coalesce(c.activity,'') = coalesce(cc.activity,'') and coalesce(c.workstream,'') = coalesce(cc.workstream,'')
						left join ScheduledProcessDowntime				cd on coalesce(c.org_group ,'') = coalesce(cd.org_group ,'') and coalesce(c.location  ,'') = coalesce(cd.Location  ,'') and coalesce(c.[function],'') = coalesce(cd.[function],'')  and coalesce(c.equipment,'') = coalesce(cd.equipment,'') and coalesce(c.equipment_type,'') = coalesce(cd.equipment_type,'') and coalesce(c.activity,'') = coalesce(cd.activity,'') and coalesce(c.workstream,'') = coalesce(cd.workstream,'')
						left join UnscheduledProcessDowntime_exclsb		ce on coalesce(c.org_group ,'') = coalesce(ce.org_group ,'') and coalesce(c.location  ,'') = coalesce(ce.Location  ,'') and coalesce(c.[function],'') = coalesce(ce.[function],'')  and coalesce(c.equipment,'') = coalesce(ce.equipment,'') and coalesce(c.equipment_type,'') = coalesce(ce.equipment_type,'') and coalesce(c.activity,'') = coalesce(ce.activity,'') and coalesce(c.workstream,'') = coalesce(ce.workstream,'')
						left join Starve_Block							cf on coalesce(c.org_group ,'') = coalesce(cf.org_group ,'') and coalesce(c.location  ,'') = coalesce(cf.Location  ,'') and coalesce(c.[function],'') = coalesce(cf.[function],'')  and coalesce(c.equipment,'') = coalesce(cf.equipment,'') and coalesce(c.equipment_type,'') = coalesce(cf.equipment_type,'') and coalesce(c.activity,'') = coalesce(cf.activity,'') and coalesce(c.workstream,'') = coalesce(cf.workstream,'')
						where c.Attribute = 'Calendar time') --select * from ProductionTime
--LeverCalculation 
	,UnscheduledEquipDowntimeBc as (
			-- unscheduled equipment downtime
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Unscheduled equipment downtime'  as 'Attribute','%' as 'Unit', c.[Cost?]
						, case when (c.Value + ca.Value) = 0 then 0 else c.Value / (c.Value + ca.Value) end as Value
						from #Final_Calc_Dataset c
						inner join ProductionTime ca on coalesce(c.org_group ,'') = coalesce(ca.org_group ,'') and coalesce(c.location  ,'') = coalesce(ca.Location  ,'') and coalesce(c.[function],'') = coalesce(ca.[function],'')  and coalesce(c.equipment,'') = coalesce(ca.equipment,'') and coalesce(c.equipment_type,'') = coalesce(ca.equipment_type,'') and coalesce(c.activity,'') = coalesce(ca.activity,'') and coalesce(c.workstream,'') = coalesce(ca.workstream,'')
						where c.Attribute = 'Unscheduled equipment downtime' )  

	,ScheduledProcessDowntimeBc as (
			-- Scheduled Process Downtime
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Scheduled process downtime'  as 'Attribute','%' as 'Unit', c.[Cost?]
						, case when (c.Value + ca.Value) = 0 then 0 else c.Value / (c.Value + ca.Value) end as Value
						from #Final_Calc_Dataset c
						inner join ProductionTime ca on coalesce(c.org_group ,'') = coalesce(ca.org_group ,'') and coalesce(c.location  ,'') = coalesce(ca.Location  ,'') and coalesce(c.[function],'') = coalesce(ca.[function],'')  and coalesce(c.equipment,'') = coalesce(ca.equipment,'') and coalesce(c.equipment_type,'') = coalesce(ca.equipment_type,'') and coalesce(c.activity,'') = coalesce(ca.activity,'') and coalesce(c.workstream,'') = coalesce(ca.workstream,'')
						where c.Attribute = 'Scheduled process downtime' ) --select * from ScheduledProcessDowntimeBc

--  Unscheduled process downtime excl. starve/block
	,UnscheduledProcessDowntime_exclsbBc as (
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Unscheduled process downtime excl. starve/block'  as 'Attribute', '%' as 'Unit', c.[Cost?]
						, case when (c.Value + ca.Value) = 0 then 0 else c.Value / (c.Value + ca.Value) end as Value
						from #Final_Calc_Dataset c
						inner join ProductionTime ca on coalesce(c.org_group ,'') = coalesce(ca.org_group ,'') and coalesce(c.location  ,'') = coalesce(ca.Location  ,'') and coalesce(c.[function],'') = coalesce(ca.[function],'')  and coalesce(c.equipment,'') = coalesce(ca.equipment,'') and coalesce(c.equipment_type,'') = coalesce(ca.equipment_type,'') and coalesce(c.activity,'') = coalesce(ca.activity,'') and coalesce(c.workstream,'') = coalesce(ca.workstream,'')
						where c.Attribute = 'Unscheduled process downtime excl. starve/block') --select * from UnscheduledProcessDowntime_exclsbBc
	,CalculatedNetRate as ( 
			select distinct c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Calculated Net Rate'  as 'Attribute', 't/hr' as 'Unit', c.[Cost?]
					, case when ca.Value = 0 then 0 else c.value / ca.Value end as Value
					from #Final_Calc_Dataset c
					inner join ProductionTime ca on coalesce(c.org_group ,'') = coalesce(ca.org_group ,'') and coalesce(c.location  ,'') = coalesce(ca.Location  ,'') and coalesce(c.[function],'') = coalesce(ca.[function],'')  and coalesce(c.equipment,'') = coalesce(ca.equipment,'') and coalesce(c.equipment_type,'') = coalesce(ca.equipment_type,'') and coalesce(c.activity,'') = coalesce(ca.activity,'') and coalesce(c.workstream,'') = coalesce(ca.workstream,'')
					where c.Attribute = 'Total movement tonnes')
				
				
			select a.* into #ProductionTime from (
				select * from UnscheduledEquipDowntimeBc			union all
				select * from CalendarTime							union all
				select * from StandbyTime							union all
				select * from ScheduledEquipDowntime				union all
				select * from UnscheduledEquipDowntime				union all
				select * from ScheduledProcessDowntime				union all
				select * from UnscheduledProcessDowntime_exclsb		union all
				select * from ScheduledProcessDowntimeBc			union all
				select * from UnscheduledProcessDowntime_exclsbBc	union all
				select * from CalenderTimeNotAggregate				union all
				select * from CalculatedNetRate						union all
				select * from ProductionTime
				) a;

-- if a mismatch here - investigate
--Select count(*) from #Final_Calc_Dataset  where attribute not in ('Fixed Cost', 'Variable Cost') and attribute = 'Calendar Time';
--Select * from #ProductionTime where Attribute = 'Calendar Time';
-- check if calendar time is greater than a particular value
--select * from #ProductionTime;

insert into @results (severity, [message])
select 'error', Location + ' ' + Equipment + ' production time less than zero'
from #ProductionTime where Attribute='Production Time' and Value < 0

--- Checks to ensure net rate matches total movement 
-- Check this again - You need the raw net rate to check otherwise it makes no sense!!
insert into @results (severity, [message])
select case when V.variance > 0.2 then 'error' else 'warning' end,
  V.Location + ' ' + V.Equipment + ' variance between total movement and net rate x production time is ' + cast(cast(V.variance*100 as int) as nvarchar) + '%'
from (select a.Location, a.Activity, a.Equipment_Type, a.Equipment, a.Value as 'TM', b.value as 'NR', c.Value as 'PT',
  case
    when a.Value=b.Value*c.Value*d.Value then 0
	when a.Value <> b.Value*c.Value*d.Value and (a.Value = 0 or b.Value*c.Value*d.Value=0) then 1
	else (b.Value*c.Value*d.Value)/ a.Value - 1
  end as variance
  from #Final_Calc_Dataset a
  	inner join #Final_Calc_Dataset b on a.Location=b.Location and a.Activity=b.Activity and a.Equipment_Type=b.Equipment_Type and a.Equipment = b.Equipment
  	inner join #ProductionTime c on a.Location=c.Location and a.Activity=c.Activity and a.Equipment_Type=c.Equipment_Type and a.Equipment = c.Equipment
  	inner join #Final_Calc_Dataset d on a.Location=b.Location and a.Activity=d.Activity and a.Equipment_Type=d.Equipment_Type and a.Equipment = d.Equipment
  	where a.Attribute = 'Total movement tonnes' and b.Attribute = 'Net Rate' and c.Attribute = 'Production Time' and d.Attribute='Average number of units') V
  where V.variance > 0.05
  	;

-- Back-calculate RH rate, using either (a) given value, (b) RH tonnes / RH hours, or (c) assuming the same net rate as PM
with LoadHaul_RH_rate_given as (
  Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'RH rate'  as 'Attribute', c.Unit, c.[Cost?]
    , c.Value
  from #Final_Calc_Dataset c where C.Activity in ('Load', 'Haul') and c.Attribute='RH rate')

, LoadHaul_RH_rate_ratio as (
  Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'RH rate'  as 'Attribute', c.Unit, c.[Cost?]
    , case when pa.Value = 0 then 0 else c.Value / pa.Value end as Value
  from #Final_Calc_Dataset c
  inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
  where C.Activity in ('Load', 'Haul') and c.Attribute='Rehandle movement'
    and pa.Activity in ('Load', 'Haul') and pa.Attribute='Rehandle hours')

, LoadHaul_RH_rate_consol as (
  Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'RH rate'  as 'Attribute', c.Unit, c.[Cost?]
    , coalesce(pa.Value, pb.Value, c.Value) as Value
  from #Final_Calc_Dataset c
  left join LoadHaul_RH_rate_given pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
  left join LoadHaul_RH_rate_ratio pb on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
  where C.Activity in ('Load', 'Haul') and c.Attribute='Net rate'
    and (pa.Value is null or pa.Activity in ('Load', 'Haul') and pa.Attribute='RH rate')
    and (pb.Value is null or pb.Activity in ('Load', 'Haul') and pa.Attribute='RH rate'))

select * into #LoadHaulRHRate from LoadHaul_RH_rate_consol;

--select * from #LoadHaulRHRate

-- Load adjustments
with TotalProductiveMovementLoad as ( 
				Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Total Productive Movement'  as 'Attribute', c.Unit, c.[Cost?] 
				, sum(distinct c.value) - sum( distinct pa.value) as Value
					from #Final_Calc_Dataset c 
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					where c.Attribute = 'Total Movement tonnes' and pa.Attribute = 'Rehandle movement' and c.Activity = 'Load'
					group by C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, c.Unit, c.[Cost?] 
					 ) --select * from TotalProductiveMovementLoad
					 --done
	
	,TotalRehandleMovementLoad as ( 
				Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Total Rehandle Movement'  as 'Attribute', c.Unit, c.[Cost?]
					,  sum(distinct c.value)  as Value
					from #Final_Calc_Dataset c 
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					where  c.Activity = 'Load'  and c.Attribute = 'Rehandle movement' 
					group by C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, c.Unit, c.[Cost?] 
					) --select * from TotalRehandleMovementLoad
					--done
	
	,ReportProductiveMovement as (
						Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, c.Cost_Type, c.Product, 'Report Productive Movement'  as 'Attribute', c.Unit, c.[Cost?]
								,   sum(distinct c.value)  as Value
				from #Final_Calc_Dataset c 
				inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
				where  c.Attribute = 'Productive movement' and c.Activity is null
				group by C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, c.Cost_Type, c.Product, c.Unit, c.[Cost?] ) --select * from ReportProductiveMovement
				--done	
	,ReportRehandleMovement as (
				select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, c.Cost_Type, c.Product, 'Report Rehandle Movement'  as 'Attribute', c.Unit, c.[Cost?]
								, c.value - pa.value as Value
					from #Final_Calc_Dataset c 
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					where  c.Attribute = 'Total Movement' and pa.Attribute = 'Productive movement'
					)-- select * from ReportRehandleMovement 
					-- done
	
	,RateAdjLoad as (
				Select C.[Sheet], c.[Org_Group], C.Location, 'Adjusted Rate factor' as Attribute
					,case when sum(c.Value) = 0 then 0 else sum(distinct pa.value)/sum (distinct c.Value) end as Value
					--,c.Value sfsf, pa.Value sfffss
					from TotalProductiveMovementLoad c, ReportProductiveMovement pa 
					where c.Location = pa.Location
					group by  C.Location, c.Sheet, c.Org_Group
					)-- select * from RateAdjLoad  
					--done
	
	,RHTonnespercLoad as (
				Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Rehandle Tonnes Movement Ratio'  as 'Attribute', c.Unit, c.[Cost?]
					, case when c.Value = 0 then 0 else pa.Value / c.Value end as Value--, pa.Value ssdsd, c.Value gddga
					from  #Final_Calc_Dataset c
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					where c.Location = pa.Location and c.Attribute = 'Total movement tonnes' and pa.Attribute = 'Rehandle movement' and c.Activity = 'Load'
					
					) --select * from RHTonnespercLoad

	,RHhoursLoad as (
				Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Rehandle Hours'  as 'Attribute', c.Unit, c.[Cost?]
					, case when c.Value = 0 then 0 else pa.Value / c.Value end as Value
					from  #LoadHaulRHRate c
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					where c.Location = pa.Location and c.Attribute = 'RH rate' and pa.Attribute = 'Rehandle movement' and c.Activity = 'Load'
				) --select * from RHhoursLoad

	,ProductionHrs as ( -- Total Operating time
				Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Production Time'  as 'Attribute', c.Unit, c.[Cost?]
					, case when pa.Value = 0 then 0 else (c.Value) / (pa.Value) end as Value
					from  #Final_Calc_Dataset c
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
				--	inner join #Final_Calc_Dataset pb on coalesce(c.Activity, '') = coalesce(pb.Activity,'') and coalesce(c.org_group ,'') = coalesce(pb.org_group ,'') and coalesce(c.location  ,'') = coalesce(pb.Location  ,'') and coalesce(c.[function],'') = coalesce(pb.[function],'') and coalesce(c.equipment,'') = coalesce(pb.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pb.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pb.cost_type,'') and coalesce(c.workstream,'') = coalesce(pb.workstream,'') 
					where c.Location = pa.Location and c.Attribute = 'Total movement tonnes' and pa.Attribute = 'Net rate' --and c.Activity = 'Load' and pb.Attribute = 'Average number of units' and pb.Activity = 'Load'
						) --select * from ProductionHrs

	,PMRateLoad as ( 
				select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'PM rate'  as 'Attribute', c.Unit, c.[Cost?]
					, case when pa.Value - pb.Value = 0 then 0 else c.Value/(pa.Value - pb.Value) end as Value
					from TotalProductiveMovementLoad c
					inner join ProductionHrs pa  on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					inner join RHhoursLoad pb on		coalesce(c.Activity, '') = coalesce(pb.Activity,'') and coalesce(c.org_group ,'') = coalesce(pb.org_group ,'') and coalesce(c.location  ,'') = coalesce(pb.Location  ,'') and coalesce(c.[function],'') = coalesce(pb.[function],'') and coalesce(c.equipment,'') = coalesce(pb.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pb.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pb.cost_type,'') and coalesce(c.workstream,'') = coalesce(pb.workstream,'')
					where c.Attribute = 'Total Productive Movement' and pa.Attribute = 'Production Time' and pb.Attribute = 'Rehandle Hours')
					--select * from PMRateLoad
					-- Done
	,RHRateLoad as (Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Rehandle Rate %'  as 'Attribute', c.Unit, c.[Cost?]
						, c.Value as Value
					from #LoadHaulRHRate c
					where c.Activity = 'Load' and c.Attribute = 'RH rate')  --select * from RHRateLoad
  
	-- backcalcs starting
	,PMRateAdj_LoadBc as ( select distinct C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Adjusted PM rate'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from PMRateLoad c, RateAdjLoad b 
							where c.Location = b.Location and b.Org_Group = c.Org_Group )  -- select * from PMRateAdj_LoadBc
	
	,RHRateAdj_LoadBc as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Adjusted RH rate'  as 'Attribute', c.Unit, c.[Cost?]
								, c.Value*b.Value as Value
								from RHRateLoad c, RateAdjLoad b
								where c.Location = b.Location)  --select * from RHRateAdj_LoadBc
	
	,ProductiveTonnes_LoadBc as ( select distinct C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Adjusted Productive Tonnes'  as 'Attribute', c.Unit, c.[Cost?]
								, c.Value*(b.Value -d.Value) as Value
									from PMRateAdj_LoadBc c
									inner join ProductionHrs b on coalesce(c.Activity, '') = coalesce(b.Activity,'') and coalesce(c.org_group ,'') = coalesce(b.org_group ,'') and coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.[function],'') = coalesce(b.[function],'') and coalesce(c.equipment,'') = coalesce(b.equipment,'') and coalesce(c.equipment_type,'') = coalesce(b.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(b.cost_type,'') and coalesce(c.workstream,'') = coalesce(b.workstream,'')
									inner join RHhoursLoad d on coalesce(c.Activity, '') = coalesce(d.Activity,'') and coalesce(c.org_group ,'') = coalesce(d.org_group ,'') and coalesce(c.location  ,'') = coalesce(d.Location  ,'') and coalesce(c.[function],'') = coalesce(d.[function],'') and coalesce(c.equipment,'') = coalesce(d.equipment,'') and coalesce(c.equipment_type,'') = coalesce(d.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(d.cost_type,'') and coalesce(c.workstream,'') = coalesce(d.workstream,'')
									where c.Location = b.Location and b.Attribute = 'Production Time' and c.Attribute = 'Adjusted PM rate' and d.Attribute = 'Rehandle Hours') --select * from ProductiveTonnes_LoadBc
	
	,RehandleTonnes_LoadBc as ( select distinct C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Adjusted Rehandle Tonnes'  as 'Attribute', c.Unit, c.[Cost?]
									, c.Value*b.Value as Value
									from RHRateAdj_LoadBc c
									inner join RHhoursLoad b on coalesce(c.Activity, '') = coalesce(b.Activity,'') and coalesce(c.org_group ,'') = coalesce(b.org_group ,'') and coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.[function],'') = coalesce(b.[function],'') and coalesce(c.equipment,'') = coalesce(b.equipment,'') and coalesce(c.equipment_type,'') = coalesce(b.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(b.cost_type,'') and coalesce(c.workstream,'') = coalesce(b.workstream,'')
									where c.location = b.location) --select * from RehandleTonnes_LoadBc
	
	,TotalRHTonnes_LoadBc as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Adjusted Total RH Tonnes'  as 'Attribute', c.Unit, c.[Cost?]
								, c.Value*b.Value as Value
								from RHRateAdj_LoadBc c
								inner join RHhoursLoad b on coalesce(c.Activity, '') = coalesce(b.Activity,'') and coalesce(c.org_group ,'') = coalesce(b.org_group ,'') and coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.[function],'') = coalesce(b.[function],'') and coalesce(c.equipment,'') = coalesce(b.equipment,'') and coalesce(c.equipment_type,'') = coalesce(b.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(b.cost_type,'') and coalesce(c.workstream,'') = coalesce(b.workstream,'')
								) --select * from TotalRHTonnes_LoadBc
	, TotalRHPerMineLoad as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, NULL as [Equipment_type], 'NULL' As equipment, c.Cost_Type, c.Product, 'Adjusted Total RH Tonnes'  as 'Attribute', c.Unit, c.[Cost?]
								, sum(c.Value*b.Value) as Value
								from RHRateAdj_LoadBc c
								inner join RHhoursLoad b on coalesce(c.Activity, '') = coalesce(b.Activity,'') and coalesce(c.org_group ,'') = coalesce(b.org_group ,'') and coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.[function],'') = coalesce(b.[function],'') and coalesce(c.equipment,'') = coalesce(b.equipment,'') and coalesce(c.equipment_type,'') = coalesce(b.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(b.cost_type,'') and coalesce(c.workstream,'') = coalesce(b.workstream,'')
								group by C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, c.Cost_Type, c.Product, c.Unit, c.[Cost?]
								
								) -- select * from TotalRHPerMine

	,TargetTonnes_Bc as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Target Tonnes'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value + b.Value as Value
							from ProductiveTonnes_LoadBc c
							inner join RehandleTonnes_LoadBc b on coalesce(c.Activity, '') = coalesce(b.Activity,'') and coalesce(c.org_group ,'') = coalesce(b.org_group ,'') and coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.[function],'') = coalesce(b.[function],'') and coalesce(c.equipment,'') = coalesce(b.equipment,'') and coalesce(c.equipment_type,'') = coalesce(b.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(b.cost_type,'') and coalesce(c.workstream,'') = coalesce(b.workstream,'')
							where c.Location = b.Location)  --select * from TargetTonnes_Bc
	

	/*Additional rates to be added to both load & haul*/

	,Load_AdjDesignRate as (select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Design Rate'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from #Final_Calc_Dataset c
							inner join RateAdjLoad b on  coalesce(c.location  ,'') = coalesce(b.Location  ,'')
							where c.Location = b.Location and c.Attribute = 'Design Rate' and c.Activity = 'Load') --select * from Load_AdjDesignRate

	,Load_AdjNetRate as (select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Net Rate'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from #Final_Calc_Dataset c
							inner join RateAdjLoad b on  coalesce(c.location  ,'') = coalesce(b.Location  ,'')
							where c.Location = b.Location and c.Attribute = 'Net Rate' and c.Activity = 'Load') --select * from Load_AdjDesignRate

	,Adj_TM as (select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Total movement tonnes'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from #Final_Calc_Dataset c
							inner join RateAdjLoad b on  coalesce(c.location  ,'') = coalesce(b.Location  ,'')
							where c.Location = b.Location and c.Attribute = 'Total Movement tonnes' and c.Activity = 'Load') --select * from Adj_TM where Location = 'Whaleback'

	,Adj_RM as (select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Rehandle movement'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from #Final_Calc_Dataset c
							inner join RateAdjLoad b on  coalesce(c.location  ,'') = coalesce(b.Location  ,'')
							where c.Location = b.Location and c.Attribute = 'Rehandle movement' and c.Activity = 'Load') --select * from Adj_RM

	,Adj_RR as (select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'RH rate'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from #LoadHaulRHRate c
							inner join RateAdjLoad b on  coalesce(c.location  ,'') = coalesce(b.Location  ,'')
							where c.Location = b.Location and c.Attribute = 'RH rate' and c.Activity = 'Load') --select * from Adj_RM

	,Rehandle_Other as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], b.Activity, 'Operations' as Workstream, NULL as equipment_type, NULL as equipment, NULL as Cost_Type, NULL as Product, 'Rehandle Other'  as 'Attribute', c.Unit, c.[Cost?] 
							,c.Value - b.Value as Value
							from  (select sum(value) as value, Activity, Location as Location from Adj_RM where activity = 'Load' group by Location, activity ) b
							inner join ReportRehandleMovement c on coalesce(c.location  ,'') = coalesce(b.Location  ,'') 
							where c.Location = b.Location 
					)  --select * from Rehandle_Other


			select a.* into #LoadAdjustments from (
			select * from Load_AdjDesignRate union all
			select * from Load_AdjNetRate union all
			select * from Adj_TM union all
			select * from Adj_RM union all
			select * from Adj_RR union all
			select * from Rehandle_Other) a;

-- Mismatch - Investigate
--Select count(distinct Equipment)*5 from #Final_Calc_Dataset where activity = 'Load' and attribute not in ('Fixed Cost', 'Variable Cost')
-- ensure that the total productive movement has values
--Select * from #LoadAdjustments where equipment = 'WB LBHR 996 EXC'

-- Haul adjustments
with TotalProductiveMovementHaul as ( 
				Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Total Productive Movement'  as 'Attribute', c.Unit, c.[Cost?] 
				, sum(distinct c.value) - sum( distinct pa.value) as Value
					from #Final_Calc_Dataset c 
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					where c.Attribute = 'Total Movement tonnes' and pa.Attribute = 'Rehandle movement' and c.Activity = 'Haul'
					group by C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, c.Unit, c.[Cost?] 
					 ) --select * from TotalProductiveMovementHaul
					 --done
	
	,TotalRehandleMovementHaul as ( 
				Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Total Rehandle Movement'  as 'Attribute', c.Unit, c.[Cost?]
					,  sum(distinct c.value)  as Value
					from #Final_Calc_Dataset c 
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					where  c.Activity = 'Haul'  and c.Attribute = 'Rehandle movement' 
					group by C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, c.Unit, c.[Cost?] 
					) --select * from TotalRehandleMovementHaul
					--done
	
	,ReportProductiveMovement as (
						Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, c.Cost_Type, c.Product, 'Report Productive Movement'  as 'Attribute', c.Unit, c.[Cost?]
								,   sum(distinct c.value)  as Value
				from #Final_Calc_Dataset c 
				inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
				where  c.Attribute = 'Productive movement' and c.Activity is null
				group by C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, c.Cost_Type, c.Product, c.Unit, c.[Cost?] ) --select * from ReportProductiveMovement
				--done	
	,ReportRehandleMovement as (
				select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, c.Cost_Type, c.Product, 'Report Rehandle Movement'  as 'Attribute', c.Unit, c.[Cost?]
								, c.value - pa.value as Value
					from #Final_Calc_Dataset c 
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					where  c.Attribute = 'Total Movement' and pa.Attribute = 'Productive movement'
					)-- select * from ReportRehandleMovement 
					-- done
	
	,RateAdjHaul as (
				Select C.[Sheet], c.[Org_Group], C.Location, 'Adjusted Productive Movement Rate factor' as Attribute
					,case when sum(c.Value) = 0 then 0 else sum(distinct pa.value)/sum (distinct c.Value) end as Value
					--,c.Value sfsf, pa.Value sfffss
					from TotalProductiveMovementHaul c, ReportProductiveMovement pa 
					where c.Location = pa.Location
					group by  C.Location, c.Sheet, c.Org_Group
					) --select * from RateAdjPMHaul  
					--done
	
	,RHTonnespercHaul as (
				Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Rehandle Tonnes Movement Ratio'  as 'Attribute', c.Unit, c.[Cost?]
					, case when c.Value = 0 then 0 else pa.Value / c.Value end as Value--, pa.Value ssdsd, c.Value gddga
					from  #Final_Calc_Dataset c
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					where c.Location = pa.Location and c.Attribute = 'Total movement tonnes' and pa.Attribute = 'Rehandle movement' and c.Activity = 'Haul'
					
					) --select * from RHTonnespercHaul

	,RHhoursHaul as (
				Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Rehandle Hours'  as 'Attribute', c.Unit, c.[Cost?]
					, case when c.Value = 0 then 0 else pa.Value / c.Value end as Value
					from  #LoadHaulRHRate c
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					where c.Location = pa.Location and c.Attribute = 'RH rate' and pa.Attribute = 'Rehandle movement' and c.Activity = 'Haul'
				) --select * from RHhoursHaul

	,ProductionHrs as ( -- Total Operating time
				Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Production Time'  as 'Attribute', c.Unit, c.[Cost?]
					, case when pa.Value = 0 then 0 else (c.Value) / (pa.Value) end as Value
					from  #Final_Calc_Dataset c
					inner join #Final_Calc_Dataset pa on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
				--	inner join #Final_Calc_Dataset pb on coalesce(c.Activity, '') = coalesce(pb.Activity,'') and coalesce(c.org_group ,'') = coalesce(pb.org_group ,'') and coalesce(c.location  ,'') = coalesce(pb.Location  ,'') and coalesce(c.[function],'') = coalesce(pb.[function],'') and coalesce(c.equipment,'') = coalesce(pb.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pb.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pb.cost_type,'') and coalesce(c.workstream,'') = coalesce(pb.workstream,'') 
					where c.Location = pa.Location and c.Attribute = 'Total movement tonnes' and pa.Attribute = 'Net rate' --and c.Activity = 'Haul' and pb.Attribute = 'Average number of units' and pb.Activity = 'Haul'
						) --select * from ProductionHrs

	,PMRateHaul as ( 
				select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'PM rate'  as 'Attribute', c.Unit, c.[Cost?]
					, case when pa.Value - pb.Value = 0 then 0 else c.Value/(pa.Value - pb.Value) end as Value
					from TotalProductiveMovementHaul c
					inner join ProductionHrs pa  on coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'') and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pa.cost_type,'') and coalesce(c.workstream,'') = coalesce(pa.workstream,'') 
					inner join RHhoursHaul pb on		coalesce(c.Activity, '') = coalesce(pb.Activity,'') and coalesce(c.org_group ,'') = coalesce(pb.org_group ,'') and coalesce(c.location  ,'') = coalesce(pb.Location  ,'') and coalesce(c.[function],'') = coalesce(pb.[function],'') and coalesce(c.equipment,'') = coalesce(pb.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pb.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(pb.cost_type,'') and coalesce(c.workstream,'') = coalesce(pb.workstream,'')
					where c.Attribute = 'Total Productive Movement' and pa.Attribute = 'Production Time' and pb.Attribute = 'Rehandle Hours')
					--select * from PMRateHaul
					-- Done
	,RHRateHaul as (Select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Rehandle Rate %'  as 'Attribute', c.Unit, c.[Cost?]
						, c.Value as Value
					from #LoadHaulRHRate c
					where c.Activity = 'Haul' and c.Attribute = 'RH rate')  --select * from RHRateHaul
  
	-- backcalcs starting
	,PMRateAdj_HaulBc as ( select distinct C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Adjusted PM rate'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from PMRateHaul c, RateAdjHaul b 
							where c.Location = b.Location and b.Org_Group = c.Org_Group )  -- select * from PMRateAdj_HaulBc
	
	,RHRateAdj_HaulBc as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Adjusted RH rate'  as 'Attribute', c.Unit, c.[Cost?]
								, c.Value*b.Value as Value
								from RHRateHaul c, RateAdjHaul b
								where c.Location = b.Location)  --select * from RHRateAdj_HaulBc
	
	,ProductiveTonnes_HaulBc as ( select distinct C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Adjusted Productive Tonnes'  as 'Attribute', c.Unit, c.[Cost?]
								, c.Value*(b.Value -d.Value) as Value
									from PMRateAdj_HaulBc c
									inner join ProductionHrs b on coalesce(c.Activity, '') = coalesce(b.Activity,'') and coalesce(c.org_group ,'') = coalesce(b.org_group ,'') and coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.[function],'') = coalesce(b.[function],'') and coalesce(c.equipment,'') = coalesce(b.equipment,'') and coalesce(c.equipment_type,'') = coalesce(b.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(b.cost_type,'') and coalesce(c.workstream,'') = coalesce(b.workstream,'')
									inner join RHhoursHaul d on coalesce(c.Activity, '') = coalesce(d.Activity,'') and coalesce(c.org_group ,'') = coalesce(d.org_group ,'') and coalesce(c.location  ,'') = coalesce(d.Location  ,'') and coalesce(c.[function],'') = coalesce(d.[function],'') and coalesce(c.equipment,'') = coalesce(d.equipment,'') and coalesce(c.equipment_type,'') = coalesce(d.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(d.cost_type,'') and coalesce(c.workstream,'') = coalesce(d.workstream,'')
									where c.Location = b.Location and b.Attribute = 'Production Time' and c.Attribute = 'Adjusted PM rate' and d.Attribute = 'Rehandle Hours') --select * from ProductiveTonnes_HaulBc
	
	,RehandleTonnes_HaulBc as ( select distinct C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Adjusted Rehandle Tonnes'  as 'Attribute', c.Unit, c.[Cost?]
									, c.Value*b.Value as Value
									from RHRateAdj_HaulBc c
									inner join RHhoursHaul b on coalesce(c.Activity, '') = coalesce(b.Activity,'') and coalesce(c.org_group ,'') = coalesce(b.org_group ,'') and coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.[function],'') = coalesce(b.[function],'') and coalesce(c.equipment,'') = coalesce(b.equipment,'') and coalesce(c.equipment_type,'') = coalesce(b.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(b.cost_type,'') and coalesce(c.workstream,'') = coalesce(b.workstream,'')
									where c.location = b.location) --select * from RehandleTonnes_HaulBc
	
	,TotalRHTonnes_HaulBc as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Adjusted Total RH Tonnes'  as 'Attribute', c.Unit, c.[Cost?]
								, c.Value*b.Value as Value
								from RHRateAdj_HaulBc c
								inner join RHhoursHaul b on coalesce(c.Activity, '') = coalesce(b.Activity,'') and coalesce(c.org_group ,'') = coalesce(b.org_group ,'') and coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.[function],'') = coalesce(b.[function],'') and coalesce(c.equipment,'') = coalesce(b.equipment,'') and coalesce(c.equipment_type,'') = coalesce(b.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(b.cost_type,'') and coalesce(c.workstream,'') = coalesce(b.workstream,'')
								) --select * from TotalRHTonnes_HaulBc
	, TotalRHPerMineHaul as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, NULL as [Equipment_type], 'NULL' As equipment, c.Cost_Type, c.Product, 'Adjusted Total RH Tonnes'  as 'Attribute', c.Unit, c.[Cost?]
								, sum(c.Value*b.Value) as Value
								from RHRateAdj_HaulBc c
								inner join RHhoursHaul b on coalesce(c.Activity, '') = coalesce(b.Activity,'') and coalesce(c.org_group ,'') = coalesce(b.org_group ,'') and coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.[function],'') = coalesce(b.[function],'') and coalesce(c.equipment,'') = coalesce(b.equipment,'') and coalesce(c.equipment_type,'') = coalesce(b.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(b.cost_type,'') and coalesce(c.workstream,'') = coalesce(b.workstream,'')
								group by C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, c.Cost_Type, c.Product, c.Unit, c.[Cost?]
								
								) -- select * from TotalRHPerMine

	,TargetTonnes_Bc as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Target Tonnes'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value + b.Value as Value
							from ProductiveTonnes_HaulBc c
							inner join RehandleTonnes_HaulBc b on coalesce(c.Activity, '') = coalesce(b.Activity,'') and coalesce(c.org_group ,'') = coalesce(b.org_group ,'') and coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.[function],'') = coalesce(b.[function],'') and coalesce(c.equipment,'') = coalesce(b.equipment,'') and coalesce(c.equipment_type,'') = coalesce(b.equipment_type,'') and coalesce(c.cost_type,'') = coalesce(b.cost_type,'') and coalesce(c.workstream,'') = coalesce(b.workstream,'')
							where c.Location = b.Location)  --select * from TargetTonnes_Bc
	
	
	/*Additional rates to be added to both load & haul*/
	,Haul_AdjDesignRate as (select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type , c.Product , 'Design Rate'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from #Final_Calc_Dataset c
							inner join RateAdjHaul b on  coalesce(c.location  ,'') = coalesce(b.Location  ,'')
							where c.Location = b.Location and c.Attribute = 'Design Rate' and c.Activity = 'Haul') --select * from Haul_AdjDesignRate

	,Haul_AdjNetRate as (select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type , c.Product , 'Net Rate'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from #Final_Calc_Dataset c
							inner join RateAdjHaul b on  coalesce(c.location  ,'') = coalesce(b.Location  ,'')
							where c.Location = b.Location and c.Attribute = 'Net Rate' and c.Activity = 'Haul') --select * from Haul_AdjDesignRate

	,Adj_TM as (select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Total movement tonnes'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from #Final_Calc_Dataset c
							inner join RateAdjHaul b on  coalesce(c.location  ,'') = coalesce(b.Location  ,'')
							where c.Location = b.Location and c.Attribute = 'Total Movement tonnes' and c.Activity = 'Haul') --select * from Adj_TM where Location = 'Whaleback'

	,Adj_RM as (select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product , 'Rehandle movement'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from #Final_Calc_Dataset c
							inner join RateAdjHaul b on  coalesce(c.location  ,'') = coalesce(b.Location  ,'')
							where c.Location = b.Location and c.Attribute = 'Rehandle movement' and c.Activity = 'Haul') --select * from Adj_RM

	,Adj_RR as (select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product , 'RH rate'  as 'Attribute', c.Unit, c.[Cost?]
							, c.Value*b.Value as Value
							from #LoadHaulRHRate c
							inner join RateAdjHaul b on  coalesce(c.location  ,'') = coalesce(b.Location  ,'')
							where c.Location = b.Location and c.Attribute = 'RH rate' and c.Activity = 'Haul') --select * from Adj_RM

	,Rehandle_Other as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], b.Activity, 'Operations' as Workstream, NULL as equipment_type, NULL as equipment, NULL as Cost_Type, NULL as Product, 'Rehandle Other'  as 'Attribute', c.Unit, c.[Cost?] 
							,c.Value - b.Value as Value
							from  (select sum(value) as value, Activity, Location as Location from Adj_RM where activity = 'Haul' group by Location, activity ) b
							inner join ReportRehandleMovement c on coalesce(c.location  ,'') = coalesce(b.Location  ,'') 
							where c.Location = b.Location 
					)  --select * from Rehandle_Other


			select a.* into #HaulAdjustments from (
			select * from Haul_AdjDesignRate	union all
			select * from Haul_AdjNetRate union all
			select * from Adj_TM union all
			select * from Adj_RM union all
			select * from Adj_RR union all
			select * from Rehandle_Other) a;

-- Patch the data set with the load/haul calcs
delete from #Final_Calc_Dataset where Activity in ('Load', 'Haul') and Attribute in ('Design rate', 'Net rate', 'Total movement', 'Total movement tonnes', 'Rehandle movement', 'RH rate', 'Rehandle hours')
insert into #Final_Calc_Dataset select * from #LoadAdjustments
insert into #Final_Calc_Dataset select * from #HaulAdjustments

-- Mismatch - Investigate
--Select count(distinct Equipment)*5 from #Final_Calc_Dataset where activity = 'Haul' and attribute not in ('Fixed Cost', 'Variable Cost')
--Select * from #HaulAdjustments			

insert into @results (severity, [message])
select 'error', Location + ' ' + Equipment + ' calculated target tonnes < 0'
from #HaulAdjustments where Attribute='Target Tonnes' and Value < 0
;

-- Calculate mining stockpile opening balances from closing balances
with ClosingBalance as (
select * from #Final_Calc_Dataset where [Function]='Mining' and Attribute='Closing balance')

, MiningActivity as (
select Location, coalesce(Activity, 'LoadHaul') as Activity, sum(Value) as Value from #Final_Calc_Dataset
where Activity in ('Drill', 'Blast') and Attribute in ('Tonnes Drilled', 'Tonnes Blasted') or [Function]='Mining' and Activity is null and Attribute='Productive Movement'
group by Location, Activity)

, OpeningBalances as (
select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, 'Stockpile' as Equipment_type, C.equipment, C.Cost_Type, C.Product, 'Opening balance' as 'Attribute', c.Unit, c.[Cost?]
  , case when C.Value - I.Value + O.Value < 0 then 0 else C.Value - I.Value + O.Value end as Value
from ClosingBalance C
inner join MiningActivity I on C.Location=I.Location
inner join MiningActivity O on C.Location=O.Location
where (C.Equipment='Drill stock' and I.Activity = 'Drill' and O.Activity='Blast')
  or (C.Equipment='Blast inventory' and I.Activity = 'Blast' and O.Activity='LoadHaul')
)

insert into #Final_Calc_Dataset select * from OpeningBalances;

-- Processing stocks are a little harder because of the hub model

with ClosingBalance as (
select * from #Final_Calc_Dataset where [Function]='Processing' and Attribute='Closing balance')

, LumpFines as (
select A.Location, A.Value * B.Value as Fines, A.Value * (1 - B.Value) as Lump from #Final_Calc_Dataset A, #Final_Calc_Dataset B
where A.Equipment='Bene Plant' and A.Attribute in ('Throughput', 'Target tonnes')
  and A.Equipment='Bene Plant' and B.Attribute='Bypass ratio')

, Throughput as (
select Location, [Function], [Activity], [Equipment], [Value]
from  #Final_Calc_Dataset
where Attribute in ('Throughput', 'Target tonnes') and [Function]='Processing' and Equipment <> 'Bene Plant'
union all
select H.Location, H.[Function], H.[Activity], H.[Equipment], H.[Value] / (1 + S.[Value])
from  #Final_Calc_Dataset H
inner join #Final_Calc_Dataset S on H.Location=S.Location
where H.[Function]='Mining' and H.Attribute='Productive Movement' and H.Activity is null
  and S.[Function]='Mining' and S.Attribute='Stripping Ratio'
union all
select Location, 'Processing' as [Function], 'Beneficiation' as [Activity], 'Bene Plant Lump' as Equipment, Lump as value from LumpFines
union all
select Location, 'Processing' as [Function], 'Beneficiation' as [Activity], 'Bene Plant Fines' as Equipment, Fines as value from LumpFines
)

, Routing as (
select Location,
  case
    when [Location]='Whaleback' and Equipment in ('CR2', 'Crusher 9', 'OHP 5') then 'Pre-Crushing Stockpile'
	when [Location]='Whaleback' and Equipment='OHP 4' then 'OFH Stockpile'
	when [Location]='Whaleback' and Equipment='Car Dumper' then null -- 'Orebody'
	when [Equipment] in ('OB24 TLO', 'OB18 TLO') then 'OFH Stockpile'
	when Activity='TLO' then 'OFR Stockpile'
	when Activity in ('OFR', 'OFH') then 'Pre-Crushing Stockpile'
	else null
  end as [Source],
  case
    when [Function]='Mining' then 'Pre-Crushing Stockpile'
	when Location='Whaleback' and Equipment in ('CR2', 'Car Dumper', 'Bene Plant Lump') then 'OFH Stockpile'
	when Location='Whaleback' and Equipment in ('Crusher 9', 'OHP 5', 'OHP 4', 'Bene Plant Fines') then 'OFR Stockpile'
	--when Activity='TLO' and Equipment in ('OB18 TLO', 'OB24 TLO') then 'Orebody'
	when Activity='OFH' then 'OFH Stockpile'
	when Activity='OFR' then 'OFR Stockpile'
  end as Destination, Activity, Equipment
, Value
from Throughput
)

, Outflow as (
select Location, [Source], sum(Value) as Outflow
from Routing group by Location, [Source]
)

, Inflow as (
select Location, [Destination], sum(Value) as Inflow
from Routing group by Location, [Destination]
)

, OpeningBalances as (
select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.Equipment_type, C.equipment, C.Cost_Type, C.Product, 'Opening balance' as 'Attribute', c.Unit, c.[Cost?]
 , C.Value + coalesce(O.Outflow, 0) - coalesce(I.Inflow, 0) as Value
from ClosingBalance C
left join Inflow I on C.Location=I.Location and C.Equipment=I.[Destination]
left join Outflow O on C.Location=O.Location and C.Equipment=O.[Source]
)

insert into #Final_Calc_Dataset select * from OpeningBalances;

-------------------######################################################################################################################-----------------------	
-------------------######################################################################################################################-----------------------
-------------------######################################################################################################################-----------------------
--------------------------------------------------Rail----------------------------------------------------------------------------------------------------------

declare @daysinyear int;

select @daysinyear=Value from #Final_Calc_Dataset where Attribute='Days in year';
if @daysinyear is null set @daysinyear=365;

--Re rail recon calculations
With RailInvTransit as (select distinct  NULL as [Sheet], NULL as [Org_Group], null as Location, null as [function], null as Activity, null as Workstream, null as [Equipment_type], null as equipment, null as Cost_Type, null as Product, 'Rail inventory transit' as Attribute, null as  Unit, null as [Cost?]
							 , sum(distinct b.value) - sum(distinct a.Value) as Value
							from #Final_Calc_Dataset a , #Final_Calc_Dataset b
						where a.Activity = 'TLO' and a.Attribute = 'Target tonnes' and a.equipment not in ('OB24 TLO','OB18 TLO')
						and b.Org_Group = 'Port' and b.Activity = 'Inflow' and b.Attribute = 'Target Tonnes') --select * from RailInvTransit

	,CurrentActiveRakes as (
					select distinct C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, c.Attribute, c.Unit, c.[Cost?] , c.Value as Value
						from #Final_Calc_Dataset c
						where c.[Function] = 'Mainline' and c.Attribute = 'Active Rakes')

	, TotalOOR as (
					select C.[Sheet], c.[Org_Group], NULL as Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment, c.Cost_Type, c.Product, 'Total OOR' as Attribute, c.Unit, c.[Cost?] 
							, sum(c.Value) as Value
						from #Final_Calc_Dataset c
						where c.Activity = 'TLO' and c.Attribute = 'Target Tonnes' and c.Equipment not in ('OB24 TLO','OB18 TLO')
						group by C.[Sheet], c.[Org_Group],  C.[function], C.Activity, C.Workstream, C.[Equipment_type], c.Cost_Type, c.Product, c.Attribute, c.Unit, c.[Cost?] ) --select * from TotalOOR

	--Throughput will be 'target tonnes'
	, OORbyLocation as (
					select C.[Sheet], c.[Org_Group], c.Location as Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment, c.Cost_Type, c.Product, c.Attribute, c.Unit, c.[Cost?] 
							, sum(c.Value) as Value
						from #Final_Calc_Dataset c
						where c.Activity = 'TLO' and c.Attribute = 'Target Tonnes' and c.Equipment not in ('OB24 TLO','OB18 TLO')
						group by C.[Sheet],c.Location, c.[Org_Group],  C.[function], C.Activity, C.Workstream, C.[Equipment_type], c.Cost_Type, c.Product, c.Attribute, c.Unit, c.[Cost?] ) --select * from OORbyLocation

	, OORbyLocationAndEquip as (
				select C.[Sheet], c.[Org_Group], c.Location as Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment, c.Cost_Type, c.Product, c.Attribute, c.Unit, c.[Cost?] 
						, sum(c.Value) as Value
					from #Final_Calc_Dataset c
					where c.Activity = 'TLO' and c.Attribute = 'Target Tonnes' and c.Equipment not in ('OB24 TLO','OB18 TLO')
					group by C.[Sheet],c.Location, c.[Org_Group],  C.[function], C.Activity, C.Workstream, C.[Equipment_type], c.Cost_Type, c.Product, c.Attribute, c.Unit, c.[Cost?], c.Equipment ) --select * from OORbyLocationAndEquip

	, TimeAtMine as (
					select C.[Sheet], c.[Org_Group], c.Location, 'Shuttle' as 'Function', C.Activity, C.Workstream, C.[Equipment_type], c.Equipment, c.Cost_Type, c.Product, 'Time at mine' as 'Attribute', 'hr' as 'unit', c.[Cost?] 
					,sum(c.value) as Value 
						from #Final_Calc_Dataset c
						where c.Attribute = 'queue to load' or c.Attribute = 'Pre Load' or c.Attribute = 'Train Load' or c.Attribute = 'Post Load'
								or c.Attribute = 'mine to mine queue' or c.Attribute = 'mine 1 pre load' or c.Attribute = 'mine 1 train load'
								or c.Attribute = 'mine 1 to mine 2' or c.Attribute = 'mine 2 train load' or c.Attribute = 'mine 2 post load'
						group by C.[Sheet], c.Location, c.[Org_Group], C.Activity, C.Workstream, C.[Equipment_type],c.Equipment, c.Cost_Type, c.Product, c.[Cost?] )

	, TotalPortInflow as (
					select C.[Sheet], c.[Org_Group], c.Location, 'Shuttle' as 'Function', C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment, c.Cost_Type, c.Product, 'Port Ore' as 'Attribute', 'hr' as 'unit', c.[Cost?] 
							, sum(c.value) as Value 
					from #Final_Calc_Dataset c
						where c.Org_Group = 'Port' and c.Activity = 'Inflow' and c.Attribute = 'Target Tonnes'
						group by C.[Sheet], c.[Org_Group], c.Location,  C.Activity, C.Workstream, C.[Equipment_type], c.Cost_Type, c.Product, c.[Cost?] 
						)

	, NoOfOreCars as (
						select C.[Sheet], c.[Org_Group], c.Location, 'Shuttle' as 'Function', C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment, c.Cost_Type, c.Product, 'No. of Ore Cars' as 'Attribute', 'hr' as 'unit', c.[Cost?] 
						, c.Value as Value
							from #Final_Calc_Dataset c
							where c.org_group = 'Rail' and c.attribute in ('No of ore cars', 'No. of ore cars') and c.[function] = 'mainline'
						) 

	, OreCarTonnage as (
						select C.[Sheet], c.[Org_Group], c.Location,'Shuttle' as 'Function', C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment, c.Cost_Type, c.Product, 'Ore car tonnage' as 'Attribute', 'hr' as 'unit', c.[Cost?] 
						, c.Value as Value
							from #Final_Calc_Dataset c
							where c.org_group = 'Rail' and c.attribute in ('Ore car tonnage') and c.[function] = 'mainline'
						) 

	, RakeTonnes as (
						select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment, c.Cost_Type, c.Product, 'Rake tonnes' as 'Attribute', 'hr' as 'unit', c.[Cost?] 
						, c.Value*ca.Value as Value
							from OreCarTonnage c
							inner join NoOfOreCars ca on coalesce(c.Activity, '') = coalesce(ca.Activity,'') and coalesce(c.org_group ,'') = coalesce(ca.org_group ,'') and coalesce(c.[function]  ,'') = coalesce(ca.[function]  ,'') 
								
						) 

	, DepPerDay as ( --match the max departures per day to this
						select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment, c.Cost_Type, c.Product, 'Departures per Day' as 'Attribute', 'hr' as 'unit', c.[Cost?] 
						, case when ca.Value = 0 then 0 else c.Value/ca.Value/@daysinyear end as Value
							from OORbyLocation c
							inner join RakeTonnes ca on coalesce(c.Location, '') = coalesce(ca.Location,'') 
								
						) --select * from DepPerDay

	, ToFromJunction as (select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment, c.Cost_Type, c.Product, 'To/From Junction' as 'Attribute', 'hr' as 'unit', c.[Cost?] 
								, sum(c.Value) as Value
								from #Final_Calc_Dataset c
									where c.org_group = 'Rail' and  c.attribute = 'Junction to mine queue' or c.attribute = 'Mine queue to junction'
								group by C.[Sheet], c.[Org_Group], c.Location, C.Activity, C.Workstream, C.[Equipment_type], c.Cost_Type, c.Product, c.[Cost?], c.[Function] ) 

	, CarDumperWeight as (	
						select  C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, 'CD Weight' as 'Attribute', 'hr' as 'unit', c.[Cost?] 
								,case when sum(b.Value) = 0 then 0 else sum(c.Value)/sum(b.Value) end as Value
								from #Final_Calc_Dataset c, (select sum(value) as Value from #Final_Calc_Dataset  where Equipment in ('CD1', 'CD2','CD3','CD4','CD5') and Attribute = 'Target Tonnes') b
									where c.org_group = 'Port' and c.Equipment in ('CD1', 'CD2','CD3','CD4','CD5') and c.Attribute = 'Target Tonnes'
								group by C.[Sheet], c.[Org_Group], c.Location, C.Activity, C.Workstream, C.[Equipment_type], c.Equipment, c.Cost_Type, c.Product, c.[Cost?],c.[Function] )
								--select * from CarDumperWeight
									
	, CarDumperCycle as (
						select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, 'CD Cycle' as 'Attribute', 'hr' as 'unit', c.[Cost?] 
								, sum(c.Value) as Value
								from #Final_Calc_Dataset c
									where  c.Attribute in ('Queue to Dump', 'Dump')
								group by C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, c.[Cost?] 
							) --select * from CarDumperCycle

	, CDCycleContribution as (
						select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, 'CD Cycle Contribution' as 'Attribute', 'hr' as 'unit', c.[Cost?] 
								, sum(c.Value)*sum(ca.Value) as Value
								from CarDumperCycle c
								inner join CarDumperWeight ca on coalesce(c.equipment, '') =  coalesce(ca.equipment, '')
								group by C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, c.[Cost?] 
							) --select * from CycleContribution

	, NelsonFuncicaneWeight as (
						select C.[Sheet], c.[Org_Group],c.location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment , c.Cost_Type, c.Product, 'NF Weight' as 'Attribute', '%' as 'unit', c.[Cost?] 
								,sum(c.Value) as Value 
								from
								(
										select  a.[Sheet], a.[Org_Group], a.[Function], a.Activity, a.Workstream, a.[Equipment_type], a.Equipment , a.Cost_Type, a.Product, a.[Cost?]
											,case when a.Equipment in ('CD1','CD2', 'CD3') then 'Nelson Point' else 'Finucane Island' end as Location, 'CD Weight' as Reference
											,case when sum(b.Value) = 0 then 0 else  sum(a.Value)/sum(b.Value) end as Value
											from #Final_Calc_Dataset a, (select sum(value) as Value from #Final_Calc_Dataset  where Equipment in ('CD1', 'CD2','CD3','CD4','CD5') and Attribute = 'Target Tonnes') b
											where a.org_group = 'Port' and  a.Equipment in ('CD1', 'CD2','CD3','CD4','CD5') and a.Attribute = 'Target Tonnes'
											group by a.Equipment, a.[Sheet], a.[Org_Group], a.Location, a.[Function], a.Activity, a.Workstream, a.[Equipment_type], a.Equipment , a.Cost_Type, a.Product, a.[Cost?])  c 
									group by  C.[Sheet], c.[Org_Group],c.location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type] , c.Cost_Type, c.Product,  c.[Cost?] 
									)
									--select * from NelsonFuncicaneWeight
	, NFCycle as (
					select C.[Sheet], c.[Org_Group],c.location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment , c.Cost_Type, c.Product, 'NF Cycle' as 'Attribute', '%' as 'unit', c.[Cost?] 
							,  sum(c.Value) as Value
							from #Final_Calc_Dataset c
								where   c.Attribute in ( 'Port to dumper queue' ,'Post dump')
								group by C.[Sheet], c.[Org_Group],c.location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type] , c.Cost_Type, c.Product, c.[Cost?] ) --select * from NFCycle
								--select * from #Final_Calc_Dataset where attribute like '%dump%'
	, NFCycleContribution as ( 
								select C.[Sheet], c.[Org_Group],c.location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment , c.Cost_Type, c.Product, 'NFCycle Contribution' as 'Attribute', '%' as 'unit', c.[Cost?] 
								,  sum(c.Value)*sum(ca.Value) as Value
								from NFCycle c
								inner join NelsonFuncicaneWeight ca on ca.Location = c.Location
								group by C.[Sheet], c.[Org_Group],c.location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Cost_Type, c.Product, c.[Cost?] 
									) --select * from NFCycleContribution

				, TimeAtPort as ( select c.value + ca.value as value
								from (select sum(value) as value from NFCycleContribution) c, (select sum(value) as value from  CDCycleContribution) ca
										) --select * from TimeAtPort
					
				, RestOfCycle as ( select c.value+ca.value as value
						from 
							(select sum(value) as value from #Final_Calc_Dataset where Attribute in ('Empty travel to junction','Loaded travel from junction')) c,
							TimeAtPort ca) --select * from RestOfCycle
					--select * from #Final_Calc_Dataset where attribute like '%loaded%'
				, TotalDepPerDay as ( select sum(value) as Value from DepPerDay)
					
				, CycleTime as (select C.[Sheet], c.[Org_Group], cb.Location, 'Shuttle' as 'Function', C.Activity, C.Workstream, C.[Equipment_type], c.Equipment, c.Cost_Type, c.Product, 'Cycle Time' as 'Attribute', 'hr' as 'unit', c.[Cost?]
									,c.value + cb.Value + ca.value as value
 									from ToFromJunction c,
									(select case when a.Location in ('Whaleback','Jimblebar','Eastern Ridge') then 'east' else 'west' end as Region, a.Location,a.Value from TimeAtMine a) cb
									, RestOfCycle ca
									where c.Location = cb.Region) 	
														
	,AvgRakeTons as (
						select C.[Sheet], c.[Org_Group], NULL as location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment, c.Cost_Type, c.Product, 'Avg Rake Tonnes' as 'Attribute', 't' as 'unit', c.[Cost?]
						,case when cb.Value = 0 then 0 else sum(c.Value*ca.value)/cb.Value end as value
						from TotalDepPerDay cb, RakeTonnes c
						inner join DepPerDay ca on c.Location = ca.Location
						group by C.[Sheet], c.[Org_Group], c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Cost_Type, c.Product, c.[Cost?], cb.Value
						) --select * from AvgRakeTons

	,AvgTimeAtMine as (
						select C.[Sheet], c.[Org_Group], NULL as location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment, c.Cost_Type, c.Product, 'Avg Time at Mine' as 'Attribute', 't' as 'unit', c.[Cost?]
								,case when cb.Value = 0 then 0 else sum(c.Value*ca.value)/cb.Value end as value
								from TotalDepPerDay cb, TimeAtMine c
								inner join DepPerDay ca on c.Location = ca.Location
								group by C.[Sheet], c.[Org_Group], c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Cost_Type, c.Product, c.[Cost?], cb.Value
							) --select * from AvgTimeAtMine	

	,AvgToFromJunction as (		
							select Ca.[Sheet], ca.[Org_Group], NULL as location, ca.[Function], Ca.Activity, Ca.Workstream, Ca.[Equipment_type], NULL as Equipment, ca.Cost_Type, null as Product, 'Avg To From Junction' as 'Attribute', 't' as 'unit', ca.[Cost?]
								,case when c.Value = 0 then 0 else sum(cb.Value*ca.value)/c.Value end as value
								from TotalDepPerDay c, ToFromJunction ca
										inner join (select case when a.Location in ('Whaleback','Jimblebar','Eastern Ridge') then 'east' else 'west' end as Region, a.Location,a.Value from DepPerDay a) cb
										on ca.Location = cb.Region
								group by Ca.[Sheet], ca.[Org_Group], ca.[Function], Ca.Activity, Ca.Workstream, Ca.[Equipment_type], ca.Cost_Type, ca.[Cost?],c.Value
						) 
									
	,AvgRestOfCycle as ( 
							select ca.[Sheet], ca.[Org_Group], NULL as location, ca.[Function], Ca.Activity, Ca.Workstream, Ca.[Equipment_type], NULL as Equipment, ca.Cost_Type, null as Product, 'Avg Rest of Cycle' as 'Attribute', 't' as 'unit', ca.[Cost?]
								, case when cb.Value = 0 then 0 else  sum(c.Value*ca.value)/cb.Value end as value, c.value lsds, cb.Value dsss
								from TotalDepPerDay cb, RestOfCycle c, DepPerDay ca
								group by Ca.[Sheet], ca.[Org_Group], ca.[Function], Ca.Activity, Ca.Workstream, Ca.[Equipment_type], ca.Cost_Type, ca.[Cost?], cb.Value
											,c.value, ca.value) 
	,AvgTotalCycle as ( 
							select ca.[Sheet], ca.[Org_Group], NULL as location, ca.[Function], Ca.Activity, Ca.Workstream, Ca.[Equipment_type], NULL as Equipment, ca.Cost_Type, null as Product, 'Avg Total of Cycle' as 'Attribute', 't' as 'unit', ca.[Cost?]
								, case when cb.Value = 0 then 0 else sum(c.Value*ca.value)/cb.Value end as value
								from TotalDepPerDay cb, CycleTime c, DepPerDay ca
								where ca.Location = c.Location
								group by Ca.[Sheet], ca.[Org_Group], ca.[Function], Ca.Activity, Ca.Workstream, Ca.[Equipment_type], ca.Cost_Type, ca.[Cost?], cb.Value) 
								--select * from AvgTotalCycle

	,NumberActiveRakes as (	select	c.[Sheet], 'Rail' as Org_group, NULL as location, 'Mainline' as [function], NULL as Activity, NULL as Workstream,  NULL as Equipment_Type, NULL as Equipment, c.Cost_Type, null as Product, 'Active Rakes' as 'Attribute', 't' as 'unit', c.[Cost?]
								--select 'Rail' as Org_group, 'Mainline' as [function], 'Active Rakes' as Reference
								, ((a.value /*+ d.Value/(@daysinyear*b.value)*/)*(c.value/24)) as value
								from AvgTotalCycle c, RailInvTransit d, AvgRakeTons b, TotalDepPerDay a) --select * from NumberActiveRakes
								
	,LoadTimePerTrain as ( select ca.[Sheet], ca.[Org_Group],  ca.location, ca.[Function], Ca.Activity, Ca.Workstream, Ca.[Equipment_type],ca.Equipment, ca.Cost_Type, ca.Product, ca.Attribute as 'Attribute', 'h' as 'unit', ca.[Cost?]
								, ca.Value--, sum(c.Value*ca.value)/cb.Value as value
								from #Final_Calc_Dataset ca
								where Attribute in ( 'Train load', 'Mine 1 Train Load', 'Mine 2 Train Load')
								) --select * from LoadTimePerTrain

	,TotalDepTrains as ( select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], NULL as Equipment, c.Cost_Type, c.Product, 'Total Departure Trains' as 'Attribute', '#' as 'unit', c.[Cost?] 
						, c.Value * @daysinyear/2 as Value
							from DepPerDay c) --select * from TotalDepTrains

	, TotalLoadHours as (select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], b.Equipment, c.Cost_Type, c.Product, 'Total Load Hours' as 'Attribute', 'h' as 'unit', c.[Cost?] 
							, c.Value * ca.Value as Value
								from LoadTimePerTrain c 
								inner join TotalDepTrains ca on c.Location = ca.Location 
								inner join  
								(select distinct b.Location, c.Equipment as oc, c.Attribute
										, case	
											when c.Attribute = 'Mine 1 Train Load' then 'TLO1'
											when c.Attribute = 'Mine 2 Train Load' then 'TLO2'
											when c.Attribute = 'Train load' and c.Location = 'Eastern Ridge' then 'OB25 TLO'
											when c.Attribute = 'Train load' and c.Location = 'Whaleback' then 'TLO'
											when c.Attribute = 'Train load' and c.Location = 'Jimblebar' then 'JMB TLO'
											when c.Attribute = 'Train load' and c.Location = 'Area C' then 'TLO'
											else '' end as Equipment
												from #Final_Calc_Dataset b  
												inner join LoadTimePerTrain c	on b.Location = c.Location)
									 d on c.Location = d.Location  and d.Attribute = c.Attribute
								inner join #Final_Calc_Dataset b on coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.workstream  ,'') = coalesce(b.workstream  ,'') and d.Equipment = b.Equipment
								where b.Activity = 'TLO' and b.Attribute = 'Target Tonnes' and b.Equipment not in ('OB24 TLO','OB18 TLO')
							) --select * from TotalLoadHours

	, EffectiveLoadingHours as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], b.equipment, c.Cost_Type, c.Product, 'Effective Loading Hours'  as 'Attribute', c.Unit, c.[Cost?] 
							,case when b.Value = 0 then 0 else c.value/b.Value end as Value
							from OORbyLocationAndEquip c
							inner join #Final_Calc_Dataset b on  coalesce(c.org_group ,'') = coalesce(b.org_group , '') and coalesce(c.location  ,'') = coalesce(b.Location  ,'') and coalesce(c.cost_type,'') = coalesce(b.cost_type,'') and coalesce(c.equipment,'') = coalesce(b.equipment,'') 
							where b.Attribute = 'Net rate' and b.Activity = 'TLO' and b.Equipment not in ('OB24 TLO','OB18 TLO') ) --select * from EffectiveLoadingHours
	
	, TLODowntimeLoading as (	select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Effective Loading Hours'  as 'Attribute', c.Unit, c.[Cost?] 
										,c.value - b.Value as Value, c.Value shdh, b.Value shhsbd
										from TotalLoadHours c
										inner join EffectiveLoadingHours b on c.Location = b.Location and c.Equipment = b.Equipment) --select * from TLODowntimeLoading
	
	, TotalTLOdowntimeExcSchMain as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Total TLO downtime exc sch. maint.'  as 'Attribute', c.Unit, c.[Cost?] 
										,sum(c.value) as Value
										from #ProductionTime c 
										where c.Attribute in ('Scheduled process downtime raw','Unscheduled equipment downtime raw','Unscheduled process downtime excl. starve/block raw')
										and c.Activity = 'TLO' and c.Equipment not in ('OB24 TLO','OB18 TLO')
										group by C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, c.Unit, c.[Cost?] )

	, DTImpactingTrains as ( select C.[Sheet], c.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Downtime Impacting Trains                     '  as 'Attribute', '%' as unit, c.[Cost?] 
									, case when b.Value = 0 then 0 else c.Value / b.Value end as Value
									from TLODowntimeLoading c
									inner join TotalTLOdowntimeExcSchMain b on c.Equipment = b.Equipment  and c.Location = b.Location 			 
								)

	, DumpTime as			(select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, 'Dump Time' as 'Attribute', 'hr' as 'unit', c.[Cost?] 
								, sum(c.Value) as Value
									from #Final_Calc_Dataset c
										where  c.Attribute in ( 'Dump')
									group by C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, c.[Cost?])

	, CarDumperTonnes as ( select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, c.attribute, c.unit, c.[Cost?] 
								, c.Value as Value
									from #Final_Calc_Dataset c
										where  c.Attribute in ( 'Target Tonnes') and c.equipment in ('CD1','CD2','CD3','CD4','CD5')				
									)

	, CarDumperNetRate as ( select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, c.attribute, c.unit, c.[Cost?] 
								, c.Value as Value
									from #Final_Calc_Dataset c
										where  c.Attribute in ( 'Net Rate') and c.equipment in ('CD1','CD2','CD3','CD4','CD5')
									)

	, CarDumperProductionTime as ( select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, c.Attribute, c.unit, c.[Cost?] 
									, c.Value as Value
									from #ProductionTime c
									where   c.equipment in ('CD1','CD2','CD3','CD4','CD5') and c.attribute = 'Production Time'
									)

	, CarDumperUnscheduledEqp as ( select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, c.Attribute, c.unit, c.[Cost?] 
								, c.Value as Value
									from #ProductionTime c
									where   c.equipment in ('CD1','CD2','CD3','CD4','CD5') and c.attribute = 'Unscheduled equipment downtime raw')

	, CarDumperScheduledProcess as ( select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, c.Attribute, c.unit, c.[Cost?] 
								, c.Value as Value
									from #ProductionTime c
									where   c.equipment in ('CD1','CD2','CD3','CD4','CD5') and c.attribute = 'Scheduled process downtime raw')

	, CarDumperUnschedProcess as ( select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, c.Attribute, c.unit, c.[Cost?] 
								, c.Value as Value
									from #ProductionTime c
									where   c.equipment in ('CD1','CD2','CD3','CD4','CD5') and c.attribute = 'Unscheduled process downtime excl. starve/block raw'
									) --select * from CarDumperUnschedProcess
						--	select * from #ProductionTime
	, HoursOfDumpDownTime as ( select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, 'Hours of Dump Downtime CD' as 'Attribute', c.unit, c.[Cost?] 
								, case when cc.value*ca.Value-cb.Value = 0 then 0 else c.Value/cc.value*ca.Value-cb.Value end as Value
									from CarDumperTonnes c
									inner join DumpTime					ca on c.equipment = ca.equipment
									inner join CarDumperProductionTime	cb on c.equipment = cb.equipment
									, AvgRakeTons				cc 
									) --select * from HoursOfDumpDownTime

	, DowntimeImpactingTrainsCD as ( select C.[Sheet], c.[Org_Group], c.Location, c.[Function], C.Activity, C.Workstream, C.[Equipment_type], c.Equipment , c.Cost_Type, c.Product, 'Hours of Downtime CD' as 'Attribute', c.unit, c.[Cost?] 
								, case when (ca.Value + cb.Value + cc.Value) = 0 then 0 else c.Value/(ca.Value + cb.Value + cc.Value) end as Value
								--, c.Value wwds, ca.Value sdfa, cb.Value sdsdfa, cc.Value sdajjff
									from HoursOfDumpDownTime c
									inner join CarDumperUnscheduledEqp ca on c.equipment = ca.equipment
									inner join CarDumperUnschedProcess	cb on c.equipment = cb.equipment
									inner join CarDumperScheduledProcess cc on c.equipment = cc.equipment
									) --select * from DowntimeImpactingTrainsCD
	, MaximumRakes as (
					select top 1 NULL as [Sheet], 'Rail' as [Org_Group], NULL as Location, 'Shuttle' as [function], 'Rail' as Activity, 'Operations' as Workstream, NULL as [Equipment_type], NULL as Equipment, NULL as Cost_Type, null as Product, 'Maximum allowable active rakes' as Attribute, NULL as Unit, NULL as [Cost?] 
							, 6 as Value
						)
					select a.* into #ActiveRakesCalc from 
					(
						select * from NumberActiveRakes union all
						select * from DTImpactingTrains union all
						select * from RailInvTransit    union all
						--select * from MaximumTonnes     union all
						--select * from MaximumRakes		union all
						select * from TotalOOR			union all
						select * from DowntimeImpactingTrainsCD
						) a
						
 if (select abs(b.value/a.Value) as value from #ActiveRakesCalc a,#ActiveRakesCalc b where a.Attribute = 'Total OOR' and  b.Attribute = 'Rail inventory transit') > 0.1
 insert into @results (severity, [message]) values ('warning', 'Mainline calculated rail inventory transit > 10% of total OOR')

 declare @ar_calc float = (select value from #ActiveRakesCalc where Attribute='Active Rakes')
 declare @ar_var float = (select abs(@ar_calc / b.Value - 1) from #Final_Calc_Dataset b where b.Attribute = 'Active Rakes' and b.[function] = 'Mainline');
if @ar_var >= 0.01
insert into @results (severity, [message]) select
  case when @ar_var > 0.05 then 'error' else 'warning' end,
  'Active rakes calculated from cycle (' + cast(@ar_calc as nvarchar) + ') differs from supplied value by ' + cast(cast(@ar_var*100 as int) as nvarchar) + '%'
;

		-------------------######################################################################################################################-----------------------	
		-------------------######################################################################################################################-----------------------
		-------------------######################################################################################################################-----------------------
		--------------------------------------------------------------------------------Port----------------------------------------------------------------------------
; 
	With Screened as (
	-- Screened
	select distinct  c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Screened'  as 'Attribute', c.Unit, c.[Cost?]
			,c.[Value]*(1 - pa.[Value]) as [Value]
			FROM #Final_Calc_Dataset c
			left join #Final_Calc_Dataset pa on coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'')  and coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.workstream, '') = coalesce(pa.workstream,'')  and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.location,'') = coalesce(pa.location,'')
			where  c.Attribute = 'Lump' and pa.Attribute = 'LRP Bypass') --select * from Screened
	-- RSF
	,RSF as (
	select distinct  c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'RSF'  as 'Attribute' ,c.Unit, c.[Cost?] 
			,case when  pa.value = 0 or (1 + (c.[Value]/(pa.value))) = 0 then 0 else (c.[Value]/(pa.value))/(1 + (c.[Value]/(pa.value))) end as [Value]
			FROM #Final_Calc_Dataset c
			left join Screened pa on coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'')  and coalesce(c.Activity, '') = coalesce(pa.Activity,'') and coalesce(c.workstream, '') = coalesce(pa.workstream,'')  and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.location,'') = coalesce(pa.location,'')
			where  c.Attribute = 'Screen stack' and pa.Attribute = 'Screened' ) --select * from RSF

				-- Yard outflow
				,YardOutflow as ( select distinct c.sheet, C.[Org_Group], C.Location, 'Yard Outflow'  as 'Attribute' ,c.Unit, c.[Cost?]
								, sum(c.value) as Value
								from #Final_Calc_Dataset c
									where c.Org_Group = 'Port' and c.Activity = 'Outflow' and c.Equipment_Type = 'Shiploader' and c.Attribute = 'Target Tonnes'
								group by  c.sheet, C.[Org_Group], C.Location,c.Unit, c.[Cost?]) -- select * from YardOutflow

	-- Lump ratio
	,LR as (select distinct  c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Lump ratio'  as 'Attribute' ,c.Unit, c.[Cost?] 
			,case when ca.value = 0 then 0 else c.[Value] / ca.[Value] end as [Value]
			FROM #Final_Calc_Dataset c
			inner join YardOutflow ca on ca.Location = c.Location and ca.Org_Group = c.Org_Group
			where  c.Attribute = 'Lump' ) 
				
	-- DSO
	,DSO as (select distinct  c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'DSO'  as 'Attribute', c.Unit, c.[Cost?] 
			,ca.[Value] * c.[Value] as [Value]
			FROM #Final_Calc_Dataset c
			inner join #Final_Calc_Dataset ca on coalesce(c.org_group ,'') = coalesce(ca.org_group ,'')  and coalesce(c.workstream, '') = coalesce(ca.workstream,'')  and coalesce(c.equipment,'') = coalesce(ca.equipment,'') and coalesce(c.equipment_type,'') = coalesce(ca.equipment_type,'') and coalesce(c.location,'') = coalesce(ca.location,'')
			where  ca.Attribute = 'Yard Inflow' and c.Attribute = 'DL%' )	
		
	-- Screen Stack Per Ton outflow
	, ScreenStackPerTon as (select c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Screen stack per ton outflow'  as 'Attribute' , c.Unit, c.[Cost?]  
								,case when (1-pb.Value) = 0 then 0 else (pb.Value * pa.Value *(1 - c.[Value])) / (1 - pb.Value) end as [Value]
								from   #Final_Calc_Dataset c
								inner join LR pa on coalesce(c.org_group ,'') = coalesce(pa.org_group ,'')  and coalesce(c.workstream, '') = coalesce(pa.workstream,'')  and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.location,'') = coalesce(pa.location,'')
								inner join RSF pb on coalesce(c.org_group ,'') = coalesce(pb.org_group ,'')  and coalesce(c.workstream, '') = coalesce(pb.workstream,'')  and coalesce(c.equipment,'') = coalesce(pb.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pb.equipment_type,'') and coalesce(c.location,'') = coalesce(pb.location,'')
								where c.Attribute = 'LRP Bypass') 	 
		 
	--Reclaimed 
	,Reclaimed as (select c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Reclaimed'  as 'Attribute', c.Unit, c.[Cost?] 
						,(ca.Value + (ca.Value*pe.Value) - c.Value) as [Value] 
						from   DSO c
						inner join YardOutflow ca on ca.Location = c.Location and ca.Org_Group = c.Org_Group  
						inner join ScreenStackPerTon pe on coalesce(c.org_group ,'') = coalesce(pe.org_group ,'') and coalesce(c.[function],'') = coalesce(pe.[function] ,'')  and coalesce(c.location,'') = coalesce(pe.location,'')
							) 

					-- Reclaimer Throughput
				,ReclaimerThroughput as ( select distinct c.sheet, C.[Org_Group], C.Location, 'Reclaimer Throughput'  as 'Attribute' 
								, sum(c.value) as Value
								from #Final_Calc_Dataset c
									where c.Org_Group = 'Port' and c.Activity = 'Outflow' and c.Equipment_Type = 'Reclaimer' and c.Attribute = 'Target Tonnes'
								group by  c.sheet, C.[Org_Group], C.Location) 

	--Write off adjustments
	,WriteOffAdj as (	select c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Write off adjustments'  as 'Attribute', c.Unit, c.[Cost?] 
								,  c.Value - ca.Value  as [Value] 
								from  Reclaimed c
								inner join ReclaimerThroughput ca on    coalesce(c.org_group ,'') = coalesce(ca.org_group ,'') and coalesce(c.location ,'') = coalesce(ca.location ,'') 
									) 
	--Inventory change 
	,InventoryChange as (select c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Inventory change'  as 'Attribute', c.Unit, c.[Cost?]
							,c.Value - ca.Value + cb.Value as [Value] 
									from   #Final_Calc_Dataset c
									inner join YardOutflow ca on    coalesce(c.org_group ,'') = coalesce(ca.org_group ,'') and coalesce(c.location ,'') = coalesce(ca.location ,'')
									inner join WriteOffAdj cb on coalesce(c.org_group ,'') = coalesce(cb.org_group ,'') and coalesce(c.[function],'') = coalesce(cb.[function] ,'') and coalesce(c.workstream, '') = coalesce(cb.workstream,'')  and coalesce(c.equipment,'') = coalesce(cb.equipment,'') and coalesce(c.equipment_type,'') = coalesce(cb.equipment_type,'') and coalesce(c.location,'') = coalesce(cb.location,'')
									where c.Attribute = 'Yard Inflow') 
	-- opening stock
	,OpeningStock as ( 
					select c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Opening stock'  as 'Attribute', c.Unit, c.[Cost?] 
				,c.Value - ca.Value as [Value] 
				from   #Final_Calc_Dataset c
						inner join InventoryChange ca on    coalesce(c.org_group ,'') = coalesce(ca.org_group ,'') and coalesce(c.location,'') = coalesce(ca.location,'')
						where c.Attribute = 'Closing stock')
	-- Total Ore Handled
	,TotalOreHandled as (select c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Total ore handled'  as 'Attribute', c.Unit, c.[Cost?] 
						,pe.Value + c.Value - pe.Value + pa.Value + pb.Value as [Value] 
							from   #Final_Calc_Dataset c
							inner join DSO pe		on coalesce(c.org_group ,'') = coalesce(pe.org_group ,'') and coalesce(c.[function],'') = coalesce(pe.[function] ,'')  and coalesce(c.location,'') = coalesce(pe.location,'')
							inner join Reclaimed pa on coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.[function],'') = coalesce(pa.[function] ,'')  and coalesce(c.location,'') = coalesce(pa.location,'')
							inner join Screened pb	on coalesce(c.org_group ,'') = coalesce(pb.org_group ,'') and coalesce(c.[function],'') = coalesce(pb.[function] ,'')  and coalesce(c.location,'') = coalesce(pb.location,'')
							where c.Attribute = 'Yard Inflow')
	--select * from #Final_Calc_Dataset where Equipment like 'SL C%'
	, SLConVDirectCost as (select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost rate'  as 'Attribute', '$/t' as Unit, c.[Cost?]  
								,case when sum (distinct ca.Value) + sum(distinct cb.value) = 0 
								then 0 
								else (c.Value/(sum (distinct ca.Value) + sum(distinct cb.value))) end as Value
								from #Final_Calc_Dataset c 
								inner join (select case when a.Location in ('North Yard','South Yard') then 'Nelson Point' else 'Finucane Island' end as location,'Reclaimed' as 'Attribute' , sum(a.Value) as Value 
													from Reclaimed a where a.Attribute = 'Reclaimed' group by a.Location) ca on c.Location = ca.location
								inner join (select case when a.Location in ('North Yard','South Yard') then 'Nelson Point' else 'Finucane Island' end as location,'Screened' as 'Attribute' , sum(a.Value) as Value 
													from Screened a where a.Attribute = 'Screened' group by a.Location) cb on c.Location = cb.location
								where c.Cost_Type = 'Direct Cost' and ca.Attribute = 'Reclaimed' and c.Equipment = 'SL Conv Syst' and cb.Attribute = 'Screened'
								group by c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, c.[Cost?],c.Value )
	/*, SLConVMaterials as (select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost rate'  as 'Attribute', '$/t' as Unit, c.[Cost?]  
								,case when sum (distinct ca.Value) + sum(distinct cb.value) = 0 
								then 0 
								else (c.Value/(sum (distinct ca.Value) + sum(distinct cb.value))) end as Value
								from #Final_Calc_Dataset c 
								inner join (select case when a.Location in ('North Yard','South Yard') then 'Nelson Point' else 'Finucane Island' end as location,'Reclaimed' as 'Attribute' , sum(a.Value) as Value 
													from Reclaimed a where a.Attribute = 'Reclaimed' group by a.Location) ca on c.Location = ca.location
								inner join (select case when a.Location in ('North Yard','South Yard') then 'Nelson Point' else 'Finucane Island' end as location,'Screened' as 'Attribute' , sum(a.Value) as Value 
													from Screened a where a.Attribute = 'Screened' group by a.Location) cb on c.Location = cb.location
								where c.Cost_Type = 'Maintenance Materials' and ca.Attribute = 'Reclaimed' and c.Equipment = 'SL Conv Syst' and cb.Attribute = 'Screened'
								group by c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, c.[Cost?],c.Value )*/	
	Select a.* into #PortCostDrivers from (
			select * from Screened union all
			select * from RSF union all
			select * from LR union all
			select * from DSO union all
			select * from ScreenStackPerTon union all
			select * from Reclaimed union all
			select * from WriteOffAdj union all
			select * from InventoryChange union all
			select * from OpeningStock union all
			select * from TotalOreHandled union all
			--select * from SLConVMaterials union all
			select * from SLConVDirectCost
	) a
; 
 
		-------------------######################################################################################################################-----------------------	
		-------------------######################################################################################################################-----------------------
		-------------------######################################################################################################################-----------------------
		--------------------------------------------------------------------------Cost Rates----------------------------------------------------------------------------
select a.* into #MiningCostRates from (
	-- drills
	select distinct c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, c.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Cost Rate' as Attribute, 'A$/m' as Unit, c.[Cost?]
	,case when (ca.[Value] is null or da.[Value] is null or (ca.[Value]*da.[Value])=0) then c.Value else c.[Value]/(ca.[Value]*da.[Value]) end as 'Value'
		from #Final_Calc_Dataset c
		left join #Final_Calc_Dataset ca on ca.Attribute = 'Average no. of drills' and coalesce(ca.org_group, '') = coalesce(c.org_group, '') and coalesce(ca.location, '') = coalesce(c.location, '') and coalesce(ca.[function], '') = coalesce(c.[function], '') and coalesce(c.Equipment, '') = coalesce(ca.Equipment, '')  and coalesce(c.Equipment_type, '') = coalesce(ca.Equipment_type, '')
		left join #Final_Calc_Dataset da on da.Attribute = 'Meters drilled per rig' and coalesce(da.org_group, '') = coalesce(c.org_group, '') and coalesce(da.location, '') = coalesce(c.location, '') and coalesce(da.[function], '') = coalesce(c.[function], '')   and coalesce(c.Equipment, '') = coalesce(da.Equipment, '') and coalesce(c.Equipment_type, '') = coalesce(da.Equipment_type, '')
		where c.Activity ='drill' and c.Cost_Type is not null and c.Equipment is not null
	union all
	-- Overall Cost rate drill
	select distinct c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, c.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Cost Rate' as Attribute, 'A$/m' as Unit, c.[Cost?]
	,case when (ca.[Value])=0 then c.[Value] else c.[Value]/(ca.[Value]) end as 'Value'
		from #Final_Calc_Dataset c
		inner join (select c.Location, sum(c.Value*ca.Value) as value  from #Final_Calc_Dataset c
							inner join #Final_Calc_Dataset ca on coalesce(ca.org_group, '') = coalesce(c.org_group, '') and coalesce(ca.location, '') = coalesce(c.location, '') and coalesce(ca.[function], '') = coalesce(c.[function], '') and coalesce(c.Equipment, '') = coalesce(ca.Equipment, '')  and coalesce(c.Equipment_type, '') = coalesce(ca.Equipment_type, '')
							where  c.Attribute = 'Average no. of drills' and  ca.Attribute = 'Meters drilled per rig'
							group by c.Location) ca on c.Location = ca.Location
		where c.Activity = 'Drill' and Equipment_Type is null and c.Cost_Type is not null
	union all
	-- blast cost rates 
	select distinct c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Cost Rate'  as 'Attribute', '$/t' as [Unit], c.[Cost?]
	,case when ca.[Value] = 0 then C.[Value] else (C.[Value] / ca.[Value]) end as Value
		from #Final_Calc_Dataset c
		inner join #Final_Calc_Dataset ca on coalesce(ca.org_group, '') = coalesce(c.org_group, '') and coalesce(ca.location, '') = coalesce(c.location, '') and coalesce(ca.[function], '') = coalesce(c.[function], '') and coalesce(c.Equipment, '') = coalesce(ca.Equipment, '') and coalesce(c.workstream, '') = coalesce(ca.workstream, '') and coalesce(c.equipment_type, '') = coalesce(ca.equipment_type, '')
		where c.[Function] ='Mining' and c.Activity = 'Blast' and ca.Attribute = 'Tonnes blasted' and c.Cost_Type <> 'Consumables' 
	union all
	-- Load & Haul diesel cost rates
	select distinct c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream, C.[Equipment_type], C.equipment, c.Cost_Type, c.Product, 'Cost Rate'  as 'Attribute', c.Unit, c.[Cost?]
	,case when coalesce(cb.value, 0) * coalesce(cb.value, 0) = 0 then c.value else ca.value * cb.value end as Value
		from #Final_Calc_Dataset c
		left join #MiningCostDrivers ca on ca.Attribute = 'Diesel price per litre' and coalesce(ca.org_group, '') = coalesce(c.org_group, '') and coalesce(ca.location, '') = coalesce(c.location, '') and coalesce(ca.[function], '') = coalesce(c.[function], '') and coalesce(c.Equipment, '') = coalesce(ca.Equipment, '') and coalesce(c.equipment_type, '') = coalesce(ca.equipment_type, '') ---and coalesce(c.workstream, '') = coalesce(ca.workstream, '') and coalesce(c.activity, '') = coalesce(ca.activity, '') 
		left join #MiningCostDrivers cb on cb.Attribute = 'Burn Rate' and coalesce(cb.org_group, '') = coalesce(c.org_group, '') and coalesce(cb.location, '') = coalesce(c.location, '') and coalesce(cb.[function], '') = coalesce(c.[function], '') and coalesce(c.Equipment, '') = coalesce(cb.Equipment, '') and coalesce(c.equipment_type, '') = coalesce(ca.equipment_type, '') --and coalesce(c.workstream, '') = coalesce(ca.workstream, '') and coalesce(c.activity, '') = coalesce(ca.activity, '') 
		where c.[Function] ='Mining' and c.Cost_Type = 'Diesel' and c.Attribute='Variable cost' and c.Activity in ('Load', 'Haul') --c.Equipment in ('WB LBHR 996 EXC','WB LBHR 9400 EXC','WB CAT 994 FEL','KOM 1200 FEL','WB CAT 793F DT','WB CAT 793C DT', 'ER LBHR 9250 EXC','ER LBHR 9400 EXC','ER CAT 992 FEL','ER CAT 993 FEL','ER CAT 994 FEL','ER CAT 793F DT','ER CAT 785C DT','MAC LBHR 996 EXC','MAC HIT 3600 EXC','MAC LBHR 9400 EXC','MAC KOM 1200 FEL','MAC CAT 793F DT','MAC CAT 793D DT','MAC CAT 789C DT','Liebherr 996 Manned','Liebherr 996 AHPT','Liebherr 9400 Manned','Liebherr 9400 AHPT','CAT 994 Manned','CAT 994 AHPT','YND LBHR 996 EXC','YND LBHR 9400 EXC','YND CAT 994 FEL','YND KOM 1200 FEL','YND LET 1850 FEL','YND CAT 793F DT','YND CAT 785C DT')
	) a;
	-- Mine Services cost rates
		--Mine Services - Part 1 (Calculating Productive Movement)

	-- Other Cost rates (Rail & Port)
		with productive_mvmt as (
		select distinct  c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream
		,sum ((1-c.[Value])*pa.[Value]) as  [Value]
		FROM #MiningCostDrivers c
		inner join #Final_Calc_Dataset pa on coalesce(c.org_group ,'') = coalesce(pa.org_group ,'') and coalesce(c.location  ,'') = coalesce(pa.Location  ,'') and coalesce(c.[function],'') = coalesce(pa.[function],'')  and coalesce(c.equipment,'') = coalesce(pa.equipment,'') and coalesce(c.equipment_type,'') = coalesce(pa.equipment_type,'') and coalesce(c.activity,'') = coalesce(pa.activity,'')
		where c.attribute='RH tonnes %' and  pa.attribute='Total movement tonnes' and c.Activity='Haul'
		group by c.Sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream
	 		)
		--Mine Services - Part 2 (Calculating Cost Rates)
	,MineServices as (select distinct c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream,c.Equipment_Type,c.Equipment, c.Cost_Type, c.Product, 'Cost Rate'  as 'Attribute', c.Unit, c.[Cost?]
						,case when ca.value = 0 then C.[Value] else c.[Value]/ca.[Value] end  as Value 
						from #Final_Calc_Dataset c
						inner join productive_mvmt ca on coalesce(ca.org_group, '') = coalesce(c.org_group ,'')  and coalesce(c.location  ,'') = coalesce(ca.Location  ,'') and coalesce(c.[function],'') = coalesce(ca.[function],'')  
						where  c.Activity ='Mine Services')
	,PortThroughPut as (
						select  case when C.Location in ('North Yard', 'South Yard') then 'Nelson Point' else 'Finucane Island' end as location
								, sum(DISTINCT c.value) as value
									from #Final_Calc_Dataset c
								inner join #Final_Calc_Dataset ca on coalesce(ca.org_group, '') = coalesce(c.org_group, '') and coalesce(ca.location, '') = coalesce(c.location, '') and coalesce(ca.[function], '') = coalesce(c.[function], '') and coalesce(c.Equipment, '') = coalesce(ca.Equipment, '') and coalesce(c.equipment_type, '') = coalesce(ca.equipment_type, '') and coalesce(c.workstream, '') = coalesce(ca.workstream, '') and coalesce(c.activity, '') = coalesce(ca.activity, '') 
								where c.Attribute = 'Target Tonnes' and c.Org_Group = 'Port' and c.Activity = 'Outflow'
						group by C.Location)

	, PortOpsCostRate as ( select c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream,c.Equipment_Type,c.Equipment, c.Cost_Type, c.Product, 'Cost Rate'  as 'Attribute', c.Unit, c.[Cost?]
								,case when sum(distinct a.value) = 0 then 0 else  c.Value / sum(distinct a.value) end as value
								from #Final_Calc_Dataset c
								inner join ( select case when Location in ('North Yard', 'South Yard') then 'Nelson Point' else 'Finucane Island' end location , sum(distinct value) value from #PortCostDrivers where Attribute = 'Total ore handled' group by location) a on a.Location = c.Location
								where c.Attribute = 'Variable Cost' and c.Cost_Type = 'Electricity'
								group by c.sheet, C.[Org_Group], C.Location, C.[function], C.Activity, C.Workstream,c.Equipment_Type,c.Equipment, c.Cost_Type, c.Product,c.Unit, c.[Cost?], c.value
								)
	--Rail - Diesel Cost Rate
	--[Tonnes per rake]

	,tonsperRake as		(select distinct c.sheet, C.[Org_Group], Ca.[Location], C.[function], C.Activity, C.Workstream,c.Equipment_Type,c.Equipment, c.Cost_Type, c.Product, 'Tons per rake'  as 'Attribute', 't/rake' as Unit, c.[Cost?]
						,((c.[Value])*(ca.[Value])) as value
						from #Final_Calc_Dataset c
						inner join #Final_Calc_Dataset ca on coalesce(ca.org_group, '') = coalesce(c.org_group ,'') and coalesce(c.[function],'') = coalesce(ca.[function],'')  
						where c.Org_Group ='Rail' and c.Attribute='No. of ore cars' /*and c.[Function]='Mainline'*/ and ca.Org_Group ='Rail' and ca.Activity ='Rail' and ca.Attribute = 'Ore car tonnage' 
						),

		--[OOR]
	OOR as				(select distinct c.sheet, C.[Org_Group], case Equipment when 'OB18 TLO' then 'OB18' when 'OB24 TLO' then 'OB24' else c.[Location] end as Location, C.[function], C.Activity, C.Workstream,c.Equipment_Type, c.Cost_Type, c.Product, 'OOR'  as 'Attribute', 'tpa' as Unit, c.[Cost?]
						,sum(c.[Value]) as [Value]
						from #Final_Calc_Dataset c 
						where Activity = 'TLO' and Attribute = 'Target Tonnes' and Equipment in ('TLO','OB25 TLO','TLO','JMB TLO','TLO1','TLO2', 'OB24 TLO', 'OB18 TLO')
						group by c.sheet, C.[Org_Group], case Equipment when 'OB18 TLO' then 'OB18' when 'OB24 TLO' then 'OB24' else c.[Location] end, C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Cost_Type, c.Product, c.[Attribute], c.[Unit], c.[Cost?]
						),

	--[No. rakes] =  ([OOR]/[Tonnes per rake])
	NumberOfRakes as	(select distinct c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream,c.Equipment_Type,c.Cost_Type, c.Product, 'No. rakes'  as 'Attribute', '#' as Unit, c.[Cost?]
						,case when ca.value = 0 then 0 else (c.[Value]/ca.[Value]) end as value
						from OOR c
						inner join tonsperRake ca on coalesce(c.[Location],'') = coalesce(ca.[Location],'')  
						),

	--[Diesel Volume] = ([Diesel Consumption per trip]*[No. locos]*[No. rakes])
	DieselVolume as		(select distinct c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream,c.Equipment_Type,c.Cost_Type, c.Product, 'Diesel Volume'  as 'Attribute', 'l' as Unit, c.[Cost?]
						,(c.[Value])*(ca.[Value])*case ca.Location when 'OB18' then 1 when 'OB24' then 1 else 2 end as value
						from #Final_Calc_Dataset c
						inner join NumberOfRakes ca on coalesce(c.[Location],'') = coalesce(ca.[Location],'') 
						where c.Org_Group ='Rail' and c.org_group ='Rail' and c.Attribute = 'Diesel consumption per trip'
						), 
				
	--[Total Diesel Volume] = ([Diesel Consumption per trip]*[No. locos]*[No. rakes])
	TotDieselVolume as	(select distinct c.sheet, C.[Org_Group], C.[function], C.Activity, C.Workstream,c.Equipment_Type, c.Cost_Type, c.Product, 'Diesel Volume'  as 'Attribute', 'l' as Unit, c.[Cost?]
						,sum(c.[Value]) as [Value]
						from DieselVolume c
						group by c.sheet, C.[Org_Group], C.[function], C.Activity, C.Workstream,c.Equipment_Type, c.Cost_Type, c.Product, c.[Attribute], c.[Unit], c.[Cost?]
						),

	--[Diesel Cost Rate] = ([Diesel Fixed Cost]/[Total Diesel Volume])
	DieselCostRate as	(select distinct c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost rate'  as 'Attribute', '$/L' as Unit, c.[Cost?]
						,case when ca.Value = 0 then 0 else (c.[Value]/ca.[Value]) end as value
						from #Final_Calc_Dataset c
						inner join TotDieselVolume ca on coalesce(ca.org_group, '') = coalesce(c.org_group ,'')
						where c.Org_Group ='Rail' and c.Cost_Type='Diesel' 
						)
	,ActualThroughput as	(select distinct c.sheet, 'Rail' as [Org_Group], C.[function], C.Activity, C.Workstream,c.Equipment_Type, c.Cost_Type, c.Product, 'Actual throughput'  as 'Attribute', 'tpa' as Unit, c.[Cost?]
						,sum(c.[Value]) as [Value]
						from #Final_Calc_Dataset c 
						where Activity = 'TLO' and Attribute = 'Target Tonnes' and Equipment in ('TLO','OB25 TLO','TLO','JMB TLO','TLO1','TLO2')
						group by c.sheet, C.[Org_Group], C.[function], C.Activity, C.Workstream,c.Equipment_Type, c.Cost_Type, c.Product, c.[Attribute], c.[Unit], c.[Cost?]
						),

	OtherCostRate as	(select distinct c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost rate'  as 'Attribute', '$/t' as Unit, c.[Cost?]
						,case when ca.Value = 0 then 0 else (c.[Value]/ca.[Value]) end as value
						from #Final_Calc_Dataset c
						inner join ActualThroughput ca on coalesce(ca.org_group, '') = coalesce(c.org_group ,'')
						where c.Org_Group ='Rail' and c.Workstream='Direct Cost' and c.Cost_Type='Other')
	,RateLoss as (select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Rate Loss'  as 'Attribute', '$/t' as Unit, c.[Cost?]  
						,case when ca.value = 0 then 0 else 1 - (c.Value/ca.Value) end as Value
						from #Final_Calc_Dataset c 
						inner join #Final_Calc_Dataset ca on coalesce(ca.org_group, '') = coalesce(c.org_group, '') and coalesce(ca.location, '') = coalesce(c.location, '') and coalesce(ca.[function], '') = coalesce(c.[function], '') and coalesce(c.Equipment, '') = coalesce(ca.Equipment, '') and coalesce(c.equipment_type, '') = coalesce(ca.equipment_type, '') and coalesce(c.workstream, '') = coalesce(ca.workstream, '') and coalesce(c.activity, '') = coalesce(ca.activity, '') 
						where c.Attribute = 'Net Rate' and ca.Attribute = 'Design Rate')
				
	, STKDirectCost as (select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost rate'  as 'Attribute', '$/t' as Unit, c.[Cost?]  
						,case when sum(ca.value) = 0 then 0 else (c.Value/sum (distinct ca.Value)) end as Value
						from #Final_Calc_Dataset c 
						inner join (select case when a.Location in ('North Yard','South Yard') then 'Nelson Point' else 'Finucane Island' end as location,'Ore Inflow' as 'Attribute' , sum(a.Value) as Value 
											from #Final_Calc_Dataset a where a.Attribute = 'Yard Inflow' group by a.Location) ca on c.Location = ca.location
						where c.Cost_Type = 'Direct Cost' and ca.Attribute = 'Ore Inflow' and c.Equipment = 'STK Conv Syst'
						group by c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, c.[Cost?],c.Value )
				
	/*, STKMaintenenceMaterials as (select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost rate'  as 'Attribute', '$/t' as Unit, c.[Cost?]  
						,case when sum(distinct ca.value) = 0 then 0 else (c.Value/sum (distinct ca.Value)) end as Value
						from #Final_Calc_Dataset c 
						inner join (select case when a.Location in ('North Yard','South Yard') then 'Nelson Point' else 'Finucane Island' end as location,'Ore Inflow' as 'Attribute' , sum(a.Value) as Value 
											from #Final_Calc_Dataset a where a.Attribute = 'Yard Inflow' group by a.Location) ca on c.Location = ca.location
						where c.Cost_Type = 'Maintenance Materials' and ca.Attribute = 'Ore Inflow' and c.Equipment = 'STK Conv Syst'
						group by c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, c.[Cost?],c.Value )*/
	-- Load & Haul Cost rates
	,LHCostRateseqp as ( select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost rate'  as 'Attribute', '$/t' as Unit, c.[Cost?]  
						,coalesce(case when sum(distinct ca.value) = 0 then sum(c.Value) else (sum(c.Value)/sum (distinct ca.Value)) end, sum(c.Value)) as Value
						from #Final_Calc_Dataset c
						left join ( select distinct c.equipment ,case when c.Value = 0 then 1 else  cb.Value/c.Value end as value 
											from #Final_Calc_Dataset c 
											inner join #Final_Calc_Dataset cb on coalesce(cb.org_group, '') = coalesce(c.org_group, '') and coalesce(cb.location, '') = coalesce(c.location, '') and coalesce(cb.[function], '') = coalesce(c.[function], '') and coalesce(c.Equipment, '') = coalesce(cb.Equipment, '') and coalesce(c.equipment_type, '') = coalesce(cb.equipment_type, '') and coalesce(c.workstream, '') = coalesce(cb.workstream, '') and coalesce(c.activity, '') = coalesce(cb.activity, '') and coalesce(c.cost_type, '') = coalesce(cb.cost_type, '')
											where c.Activity in ('load', 'haul') and c.Attribute = 'Net Rate' and cb.Attribute = 'Total movement tonnes') ca on c.Equipment = ca.Equipment
						where c.Cost_Type <> 'Diesel' and c.Cost_Type is not null and c.Activity in ('Load', 'Haul')
						group by c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, c.[Cost?]  )
	,LandHCostRates as ( select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost rate'  as 'Attribute', '$/t' as Unit, c.[Cost?]  
		,case when sum(distinct ca.value) = 0 then 0 else (sum(c.Value)/sum (distinct ca.Value)) end as Value
		from #Final_Calc_Dataset c
		inner join ( select distinct c.Location ,case when sum(c.Value) = 0 then 0 else  sum(cb.Value - c.Value) end as value 
							from #Final_Calc_Dataset c 
							inner join #Final_Calc_Dataset cb on coalesce(cb.org_group, '') = coalesce(c.org_group, '') and coalesce(cb.location, '') = coalesce(c.location, '') and coalesce(cb.[function], '') = coalesce(c.[function], '') and coalesce(c.Equipment, '') = coalesce(cb.Equipment, '') and coalesce(c.equipment_type, '') = coalesce(cb.equipment_type, '') and coalesce(c.workstream, '') = coalesce(cb.workstream, '') and coalesce(c.activity, '') = coalesce(cb.activity, '') and coalesce(c.cost_type, '') = coalesce(cb.cost_type, '')
							where  c.Attribute = 'Rehandle movement' and cb.Attribute = 'Total movement tonnes' and c.Activity in ('haul')
							group by c.Location) ca on c.Location = ca.Location
		where  c.Cost_Type is not null and c.Activity = 'Load & Haul'
		group by c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, c.[Cost?]  )
				
	,OB18CostRate as ( select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, 'OB18 Proc Dir O_H' as Equipment, 'Electricity' as 'Cost_Type', c.Product, 'Cost rate'  as 'Attribute', '$/t' as Unit, c.[Cost?]  
		,case when sum(distinct c.value) = 0 then 0 else (sum(cb.Value)/sum (distinct c.Value)) end as Value
		from #Final_Calc_Dataset c
		inner join #Final_Calc_Dataset cb on coalesce(cb.org_group, '') = coalesce(c.org_group, '') and coalesce(cb.location, '') = coalesce(c.location, '') and coalesce(cb.[function], '') = coalesce(c.[function], '') and coalesce(c.equipment_type, '') = coalesce(cb.equipment_type, '') and coalesce(c.workstream, '') = coalesce(cb.workstream, '') and coalesce(c.activity, '') = coalesce(cb.activity, '') 
		where  c.Attribute = 'Target Tonnes'  and cb.Cost_Type is not null and cb.Equipment = 'OB18 Proc Dir O_H'
		group by c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, c.[Cost?]  )
	--OFR Cost rates
	,OFRCostRate as ( select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost rate'  as 'Attribute', '$/t' as Unit, c.[Cost?]  
		,case when ca.value = 0 then 0 else (c.Value/ca.Value) end as Value
		from #Final_Calc_Dataset c
			inner join ( select a.Location, sum(a.Value) as Value from #Final_Calc_Dataset a where a.Activity = 'OFR' and a.Attribute = 'Target Tonnes' group by a.Location) ca on c.Location = ca.Location
			where c.Cost_Type is not null and c.Activity = 'OFR' and c.Cost_Type = 'Electricity'	)
	--Unallocated costs
	,UnallocatedRates as (select  c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost rate'  as 'Attribute', '$/t' as Unit, c.[Cost?]  
								, case when ca.Value = 0 then 0 else c.Value/ca.Value end as Value
								from #Final_Calc_Dataset c
								inner join ( select a.Location, sum(a.Value) as Value from #Final_Calc_Dataset a where a.Activity = 'OFR' and a.Attribute = 'Target Tonnes' group by a.Location) ca on c.Location = ca.Location
								where c.Cost_Type is not null and  c.Workstream = 'Operations' and c.Cost_Type = 'Electricity' and c.Activity = 'Non Allocated Cost' and C.Location <> 'Eastern Ridge')
	,ERElecCostRate as (select  c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost rate'  as 'Attribute', '$/t' as Unit, c.[Cost?]  
								, case when ca.Value = 0 then 0 else c.Value/ca.Value end as Value
								from #Final_Calc_Dataset c
								inner join ( select a.Location, sum(a.Value) as Value from #Final_Calc_Dataset a where a.Activity in ('OFR', 'OFH') and a.Attribute = 'Target Tonnes' group by a.Location) ca on c.Location = ca.Location
								where c.Cost_Type is not null and  c.Workstream = 'Operations' and c.Cost_Type = 'Electricity' and c.Activity = 'Non Allocated Cost' and C.Location = 'Eastern Ridge')
	--Port demurrage cost rates
	,CargoSize as (select  c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, cb.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cargo Size'  as 'Attribute', 't' as Unit, c.[Cost?]  
								, case when c.Value = 0 then 0 else c.Value*cb.value/c.Value end as Value
								from #Final_Calc_Dataset c
								inner join #Final_Calc_Dataset cb on coalesce(cb.org_group, '') = coalesce(c.org_group, '') and coalesce(cb.location, '') = coalesce(c.location, '') and coalesce(cb.[function], '') = coalesce(c.[function], '')  and coalesce(c.workstream, '') = coalesce(cb.workstream, '') and coalesce(c.activity, '') = coalesce(cb.activity, '') and coalesce(c.cost_Type, '') = coalesce(cb.cost_Type, '') 
								where c.attribute = 'Average ship size' and cb.attribute = 'Cargo size baseline')

	, LaytimeAllowed as (select  c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, cb.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Laytime Allowed'  as 'Attribute', 't' as Unit, c.[Cost?]  
								, case when cb.Value = 0 then 0 else (c.Value/cb.value) + ca.Value end as Value
								from CargoSize c
								inner join #Final_Calc_Dataset cb on coalesce(cb.org_group, '') = coalesce(c.org_group, '') and coalesce(cb.location, '') = coalesce(c.location, '') and coalesce(cb.[function], '') = coalesce(c.[function], '')  and coalesce(c.workstream, '') = coalesce(cb.workstream, '') and coalesce(c.activity, '') = coalesce(cb.activity, '') and coalesce(c.cost_Type, '') = coalesce(cb.cost_Type, '') and coalesce(c.equipment_type, '') = coalesce(cb.equipment_type, '') 
								inner join #Final_Calc_Dataset ca on coalesce(ca.org_group, '') = coalesce(c.org_group, '') and coalesce(ca.location, '') = coalesce(c.location, '') and coalesce(ca.[function], '') = coalesce(c.[function], '')  and coalesce(c.workstream, '') = coalesce(ca.workstream, '') and coalesce(c.activity, '') = coalesce(ca.activity, '') and coalesce(c.cost_Type, '') = coalesce(ca.cost_Type, '') and coalesce(c.equipment_type, '') = coalesce(ca.equipment_type, '') 
								where c.attribute = 'Cargo Size' and cb.attribute = 'Contract rate' and ca.attribute = 'Turn time/free time'
							
							)
	, DaysPerShip as ( select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Days Per Ship'  as 'Attribute', 't' as Unit, c.[Cost?]  
								, ca.value - c.Value as Value
								from LaytimeAllowed c
								inner join #Final_Calc_Dataset ca on coalesce(ca.org_group, '') = coalesce(c.org_group, '') and coalesce(ca.location, '') = coalesce(c.location, '') and coalesce(ca.[function], '') = coalesce(c.[function], '')  and coalesce(c.workstream, '') = coalesce(ca.workstream, '') and coalesce(c.activity, '') = coalesce(ca.activity, '') and coalesce(c.cost_Type, '') = coalesce(ca.cost_Type, '') and coalesce(c.equipment_type, '') = coalesce(ca.equipment_type, '') 
								where ca.Attribute = 'Laytime used') --select * from DaysPerShip
-- Work from here
	, TotalShips as ( select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, NULL as Equipment, c.Cost_Type, c.Product, 'Total Ships'  as 'Attribute', 't' as Unit, c.[Cost?]  
								,case when c.value = 0 then 0 else  Sum(ca.value)/c.value end as Value
								from #Final_Calc_Dataset c
								inner join #Final_Calc_Dataset ca ON C.org_group = ca.org_group
								where ca.Attribute = 'Target Tonnes' and  ca.Equipment_Type = 'Shiploader' and c.attribute = 'Average ship size' 
								group by c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Cost_Type, c.Product, c.[Cost?] ,c.value
								
						)
	, CFRFOBShips as ( select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, c.attribute, 't' as Unit, c.[Cost?]  
								, c.value*ca.Value as Value
								from #Final_Calc_Dataset c, TotalShips ca
								where c.Attribute in ( 'FOB ratio' , 'CFR Ratio')) --select * from CFRFOBShips

	, TotalDays as ( select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Days Per Ship'  as 'Attribute', 't' as Unit, c.[Cost?]  
								, ca.value - c.Value as Value
								from DaysPerShip c
								inner join #Final_Calc_Dataset ca on coalesce(ca.org_group, '') = coalesce(c.org_group, '') and coalesce(ca.location, '') = coalesce(c.location, '') and coalesce(ca.[function], '') = coalesce(c.[function], '')  and coalesce(c.workstream, '') = coalesce(ca.workstream, '') and coalesce(c.activity, '') = coalesce(ca.activity, '') and coalesce(c.cost_Type, '') = coalesce(ca.cost_Type, '') and coalesce(c.equipment_type, '') = coalesce(ca.equipment_type, '') 
								where ca.Attribute = 'Laytime used')

	, DemurrageCostRate as ( select c.sheet, C.[Org_Group], c.[Location], C.[function], C.Activity, C.Workstream, c.Equipment_Type, c.Equipment, c.Cost_Type, c.Product, 'Cost Rate'  as 'Attribute', 't' as Unit, c.[Cost?]  
								,case when (c.value*ca.Value) = 0 then 0 else cb.value/(c.value*ca.Value) end as Value
								from DaysPerShip c
								inner join CFRFOBShips ca on coalesce(ca.org_group, '') = coalesce(c.org_group, '') and coalesce(ca.location, '') = coalesce(c.location, '') and coalesce(ca.[function], '') = coalesce(c.[function], '')  and coalesce(c.workstream, '') = coalesce(ca.workstream, '') and coalesce(c.activity, '') = coalesce(ca.activity, '') and coalesce(c.cost_Type, '') = coalesce(ca.cost_Type, '') and coalesce(c.equipment_type, '') = coalesce(ca.equipment_type, '') 
								inner join #Final_Calc_Dataset cb on coalesce(cb.org_group, '') = coalesce(c.org_group, '') and coalesce(cb.location, '') = coalesce(c.location, '') and coalesce(cb.[function], '') = coalesce(c.[function], '')  and coalesce(c.workstream, '') = coalesce(cb.workstream, '') and coalesce(c.activity, '') = coalesce(cb.activity, '') and coalesce(c.cost_Type, '') = coalesce(cb.cost_Type, '') and coalesce(c.equipment_type, '') = coalesce(cb.equipment_type, '') 
								where cb.attribute = 'Variable Cost'
								) 


	select a.* into #CostRates from 
	(	
		select * from MineServices union all
		select * from PortOpsCostRate union all
		select * from OtherCostRate union all
		select * from DieselCostRate union all
		select * from RateLoss union all
		select * from STKDirectCost union all
		--select * from STKMaintenenceMaterials union all
		select * from LHCostRateseqp union all
		select * from LandHCostRates union all
		select * from ERElecCostRate union all
		select * from OFRCostRate union all
		select * from OB18CostRate union all
		--select * from DemurrageCostRate union all
		select * from UnallocatedRates) a;


--######################################################################################
--################################## NB CHANGES ########################################
--######################################################################################
Select * into #Final_Calc_Dataset_Complete from #Final_Calc_Dataset ;

-- Update 
--Other updates to ensure that records match after calculations

update #Final_Calc_Dataset_Complete set Attribute = 'Target Tonnes' where Attribute = 'Tonnes Drilled';  -- this is an assumption
--update #Final_Calc_Dataset_Complete set Attribute = 'Blasted Tonnes' where Attribute = 'Tonnes Blasted';
update #Final_Calc_Dataset_Complete set Attribute = 'Strip Ratio', Activity=null, Workstream=null where Attribute='Stripping Ratio';

update #Final_Calc_Dataset_Complete set Attribute = 'No of ore cars' where Attribute = 'No. of ore cars';
update #Final_Calc_Dataset_Complete set Attribute = 'Port to dumper queue Finucane Island' where attribute = 'Port to dumper queue' and location = 'Finucane Island';
update #Final_Calc_Dataset_Complete set Attribute = 'Port to dumper queue Nelson Point' where attribute = 'Port to dumper queue' and location = 'Nelson Point';
update #Final_Calc_Dataset_Complete set Attribute = 'Post dump Finucane Island' where attribute = 'Post dump' and location = 'Finucane Island';
update #Final_Calc_Dataset_Complete set Attribute = 'Post dump Nelson Point' where attribute = 'Post dump' and location = 'Nelson Point';
update #Final_Calc_Dataset_Complete set Attribute = 'Junction to mine queue east' where attribute like 'Junction to mine queue' and location = 'east';
update #Final_Calc_Dataset_Complete set Attribute = 'Junction to mine queue west' where attribute = 'Junction to mine queue' and location = 'west';
update #Final_Calc_Dataset_Complete set Attribute = 'Mine queue to junction east' where attribute like 'Mine queue to junction' and location = 'east';
update #Final_Calc_Dataset_Complete set Attribute = 'Mine queue to junction west' where attribute = 'Mine queue to junction' and location = 'west';
update #Final_Calc_Dataset_Complete set Location = NULL where Attribute like 'Port to dumper queue%';
update #Final_Calc_Dataset_Complete set Location = NULL where Attribute like 'Post dump%';
update #Final_Calc_Dataset_Complete set Location = NULL where Attribute like 'Junction to mine queue%';
update #Final_Calc_Dataset_Complete set Location = NULL where Attribute like 'Mine queue to junction%';

update #Final_Calc_Dataset_Complete set Attribute = 'Scheduled equipment downtime' where Attribute = 'Scheduled maintenance time';
update #Final_Calc_Dataset_Complete set Attribute = 'Unscheduled equipment downtime' where Attribute = 'Unscheduled maintenance time';
update #Final_Calc_Dataset_Complete set Attribute = 'Scheduled process downtime' where Attribute = 'Scheduled operating loss time';
update #Final_Calc_Dataset_Complete set Attribute = 'Unscheduled process downtime excl. starve/block' where Attribute = 'Unscheduled operating loss time (exclude starve/block time)';
update #Final_Calc_Dataset_Complete set Attribute = 'Scheduled standby hours' where Attribute = 'Standby time' and Activity in ('load','haul','drill')
update #Final_Calc_Dataset_Complete set Attribute = 'Scheduled standby time' where Attribute = 'Standby time' and Activity not in ('load','haul','drill')
update #Final_Calc_Dataset_Complete set Attribute = 'Standby time' where Attribute in ('Standby time', 'Scheduled standby time', 'Scheduled standby hours') and activity in ('Load','Haul','OFH','OFR','TLO','Beneficiation','Inflow','Outflow','Drill');
update #CostRates set Equipment = 'OB18 Proc Dir O_H' where Equipment = 'OB18' and Activity = 'OFH'

update #Final_Calc_Dataset_Complete set Attribute = 'Rehandle tonnes' where Attribute = 'Rehandle movement' and Activity in ('load', 'haul')
update #Final_Calc_Dataset_Complete set Attribute = 'Target tonnes' where Attribute in ('Total movement', 'Total movement tonnes') and Activity in ('load', 'haul')
--update #Final_Calc_Dataset_Complete set Attribute = 'Tonnes processed' where Attribute = 'Target Tonnes' and  Activity in ('OFH','OFR','Beneficiation','Inflow','Outflow')

update #Final_Calc_Dataset_Complete set Location = 'North Yard' where Attribute in ( 'CD1 to North Yard', 'CD2 to North Yard', 'CD3 to North Yard', 'CD4 to North Yard', 'CD5 to North Yard') 
update #Final_Calc_Dataset_Complete set Location = 'East Yard' where Attribute in ( 'CD1 to East Yard', 'CD2 to East Yard', 'CD3 to East Yard', 'CD4 to East Yard', 'CD5 to East Yard')
update #Final_Calc_Dataset_Complete set Location = 'West Yard' where Attribute in ( 'CD1 to West Yard', 'CD2 to West Yard', 'CD3 to West Yard', 'CD4 to West Yard', 'CD5 to West Yard') 
update #Final_Calc_Dataset_Complete set Location = 'South Yard' where Attribute in ( 'CD1 to South Yard', 'CD2 to South Yard', 'CD3 to South Yard', 'CD4 to South Yard', 'CD5 to South Yard') 

update #Final_Calc_Dataset_Complete set Attribute = 'Tonnes transferred' where Attribute in ( 'CD1 to North Yard', 'CD2 to North Yard', 'CD3 to North Yard', 'CD4 to North Yard', 'CD5 to North Yard') 
update #Final_Calc_Dataset_Complete set Attribute = 'Tonnes transferred' where Attribute in ( 'CD1 to East Yard', 'CD2 to East Yard', 'CD3 to East Yard', 'CD4 to East Yard', 'CD5 to East Yard')
update #Final_Calc_Dataset_Complete set Attribute = 'Tonnes transferred' where Attribute in ( 'CD1 to West Yard', 'CD2 to West Yard', 'CD3 to West Yard', 'CD4 to West Yard', 'CD5 to West Yard') 
update #Final_Calc_Dataset_Complete set Attribute = 'Tonnes transferred' where Attribute in ( 'CD1 to South Yard', 'CD2 to South Yard', 'CD3 to South Yard', 'CD4 to South Yard', 'CD5 to South Yard') 

update #Final_Calc_Dataset_Complete set Location = Equipment where Attribute = 'Queue to Dump'
update #Final_Calc_Dataset_Complete set Equipment = NULL where Attribute = 'Queue to Dump'

update #Final_Calc_Dataset_Complete set Cost_Type = 'Consumables' where Attribute = 'Powder Factor' and Activity = 'Blast'
update #Final_Calc_Dataset_Complete set Attribute = 'Unscheduled process downtime excl. starve/block' where Attribute = 'Unscheduled process downtime' and Activity = 'TLO'
update #Final_Calc_Dataset_Complete 
set Value = ca.Value
from #Final_Calc_Dataset_Complete c
inner join #ProductionTime ca on coalesce(c.org_group ,'') = coalesce(ca.org_group ,'') and coalesce(c.location  ,'') = coalesce(ca.Location  ,'') and coalesce(c.[function],'') = coalesce(ca.[function],'')  and coalesce(c.equipment,'') = coalesce(ca.equipment,'') and coalesce(c.equipment_type,'') = coalesce(ca.equipment_type,'') and coalesce(c.activity,'') = coalesce(ca.activity,'') and coalesce(c.workstream,'') = coalesce(ca.workstream,'') and coalesce(c.attribute,'') = coalesce(ca.attribute,'')

-- Update Active Rakes
update #Final_Calc_Dataset_Complete set Value = b.value
		from #ActiveRakesCalc b
		inner join #Final_Calc_Dataset_Complete a on a.Org_Group = b.Org_group and a.[Function] = b.[function]
		where a.Attribute = 'Active Rakes' and  a.[Function] = 'mainline'

update #ActiveRakesCalc set Attribute = 'TLO downtime affecting loading' where location in ('Area C','Eastern Ridge','Jimblebar','Whaleback') and Equipment like '%TLO%'
update #ActiveRakesCalc set Attribute = 'TLO 1 downtime affecting loading' where location in ('Yandi') and Equipment = 'TLO1'
update #ActiveRakesCalc set Attribute = 'TLO 2 downtime affecting loading' where location in ('Yandi') and Equipment = 'TLO2'
update #ActiveRakesCalc set [function] = 'Mainline' where location in ('Area C','Eastern Ridge','Jimblebar','Whaleback', 'Yandi') and Attribute not in ('Maximum tonnes constraint','Maximum allowable active rakes')
update #ActiveRakesCalc set Equipment = NULL where location in ('Area C','Eastern Ridge','Jimblebar','Whaleback', 'Yandi')

update #ActiveRakesCalc set Attribute = 'CD downtime affecting dumping' where Equipment in ('CD1','CD2','CD3','CD4','CD5')
update #ActiveRakesCalc set location = Equipment where Equipment in ('CD1','CD2','CD3','CD4','CD5')
update #ActiveRakesCalc set Org_group = 'Rail' where Equipment in ('CD1','CD2','CD3','CD4','CD5')
update #ActiveRakesCalc set [function] = 'Mainline' where Equipment in ('CD1','CD2','CD3','CD4','CD5')
update #ActiveRakesCalc set Activity = 'Rail' where Equipment in ('CD1','CD2','CD3','CD4','CD5')
update #ActiveRakesCalc set Workstream = 'Operations' where Equipment in ('CD1','CD2','CD3','CD4','CD5')
update #ActiveRakesCalc set Equipment_Type = NULL where Equipment in ('CD1','CD2','CD3','CD4','CD5')
update #ActiveRakesCalc set Equipment = NULL where Equipment in ('CD1','CD2','CD3','CD4','CD5')

update #Final_Calc_Dataset_Complete set Equipment_Type = NULL Where attribute = 'CFR ratio'
update #Final_Calc_Dataset_Complete set Equipment_Type = NULL Where attribute = 'FOB ratio'
;


----
--- Create an upadate into the results table for the assumptions
--- 
/*update #Final_Calc_Dataset_Complete set Value = 100000000 where Attribute = 'Maximum tonnes constraint'
update #Final_Calc_Dataset_Complete set Value = 6 where Attribute = 'Maximum allowable active rakes'*/

select a.* into #final_attribute from 
(	select * from #MiningCostDrivers A union all
	select * from #MiningCostRates union all
	select * from #CostRates		union all
	select * from #PortCostDrivers union all
	--select * from #LoadAdjustments union all
	--select * from #HaulAdjustments union all
	select * from #ActiveRakesCalc union all
	select * from #Final_Calc_Dataset_Complete
	) a


select a.* into #retrieve_attribute_id from (
select distinct a.*, og.og_id, l.loc_id, f.func_id, ac.act_id, ws.ws_id, et.eqp_type_id, e.eqp_id, ct.ct_id, prod.prod_id  from 
	#final_attribute a
	left join vdt.org_group og on a.Org_Group = og.og_name
	left join vdt.location l on l.loc_name = a.Location
	left join vdt.[function] f on f.func_name = a.[Function]
	left join vdt.activity ac on ac.act_name = a.Activity 
	left join vdt.workstream ws on ws.ws_name = a.Workstream
	left join vdt.eqp_type et on et.eqp_type_name = a.Equipment_Type 
	left join vdt.equipment e on e.eqp_name  = a.Equipment
	left join vdt.cost_type ct on ct.ct_name = a.Cost_Type 
	left join vdt.product prod on prod.prod_name = a.Product) a
;

insert into @results (severity, [message])
select 'info', cast(count(*) as nvarchar) + ' data points mapped from Excel' from #retrieve_attribute_id

select c.* into #fulljoin from (
select b.attr_id, b.attribute as model_attribute, a.Org_Group, a.Location, a.[Function], a.Activity, a.Workstream, a.Equipment_Type, a.Equipment, a.Cost_Type, a.Product, a.Attribute, a.Value
from (vdt.attribute_category b inner join vdt.attribute x on b.attr_id=x.attr_id) full outer join #retrieve_attribute_id a 
on									coalesce(a.og_id		,-1)	= coalesce(b.og_id 		,-1)
								and coalesce(b.loc_id		,-1)	= coalesce(a.loc_id 	,-1)
								and coalesce(a.func_id		,-1)	= coalesce(b.func_id 	,-1)
								and coalesce(a.act_id		,-1)	= coalesce(b.act_id 	,-1)
								and coalesce(a.ws_id		,-1)	= coalesce(b.ws_id		,-1)
								and coalesce(a.eqp_id		,-1)	= coalesce(b.eqp_id		,-1)
								and coalesce(a.eqp_type_id	,-1)	= coalesce(b.eqp_type_id,-1)
								and coalesce(a.ct_id		,-1)	= coalesce(b.ct_id		,-1)
								and coalesce(a.prod_id      ,-1)    = coalesce(b.prod_id    ,-1)
								and coalesce(a.Attribute	,'')	= coalesce(b.attribute 	,'')
where x.is_calculated = 0) c 

insert into @results (severity, [message])
select top 25 'error', concat(org_group, '-', Location, '-', [Function], '-', [Activity], '-', Workstream, '-',
                              Equipment_Type, '-', Equipment, '-', Cost_Type, '-', Attribute, ' has no corresponding lever in the model')
from #fulljoin where attr_id is null;
if (select count(*) from #fulljoin where attr_id is null) > 25
insert into @results (severity, [message]) values ('error', 'More than 25 rows in the Excel file have no corresponding lever in the model')

insert into @results (severity, [message])
select top 25 'warning', concat(C.org_group, '-', C.Location, '-', C.[Function], '-', C.[Activity], '-', C.Workstream, '-',
                              C.Equipment_Type, '-', C.Equipment, '-', C.Cost_Type, '-', C.Attribute, ' is in the model but not in Excel')
from vdt.attribute_category C inner join #fulljoin F on C.attr_id=F.attr_id
where F.org_group is null;
if (select count(*) from #fulljoin where org_group is null) > 25
insert into @results (severity, [message]) values ('warning', 'More than 25 levers in the model do not have corresponding rows in the Excel file')

insert into @results (severity, [message])
select 'error', concat('Duplicate row: ', org_group, '-', location, '-', [function], '-', activity, '-', workstream, '-',
   equipment_type, '-', equipment, '-', cost_type, '-', attribute, '=', cast(value as nvarchar)) from #fulljoin where attr_id in (
select attr_id from #fulljoin where org_group is not null group by attr_id having count(*) > 1)
and org_group is not null order by attr_id

select a.attr_id, b.Value into #backcalcs  from vdt.attribute A
inner join #retrieve_attribute_id B
on                  coalesce(a.og_id    ,-1)  = coalesce(b.og_id    ,-1)
                and coalesce(b.loc_id   ,-1)  = coalesce(a.loc_id   ,-1)
                and coalesce(a.func_id    ,-1)  = coalesce(b.func_id  ,-1)
                and coalesce(a.act_id   ,-1)  = coalesce(b.act_id   ,-1)
                and coalesce(a.ws_id    ,-1)  = coalesce(b.ws_id    ,-1)
                and coalesce(a.eqp_id   ,-1)  = coalesce(b.eqp_id   ,-1)
                and coalesce(a.eqp_type_id  ,-1)  = coalesce(b.eqp_type_id,-1)
                and coalesce(a.ct_id    ,-1)  = coalesce(b.ct_id    ,-1)
				and coalesce(a.prod_id  ,-1)  = coalesce(b.prod_id  ,-1)
                and coalesce(a.attr_name  ,'')  = coalesce(b.attribute  ,'')
where a.attr_id is not null;

declare @err_count int = (select count(*) from @results where severity='error');
if @err_count > 0
begin
  insert into @results (severity, [message]) values ('info', 'Not creating data set due to ' + cast(@err_count as nvarchar) + ' errors');
end
else
begin
  insert into @results (severity, [message]) values ('info', 'Creating data set');
  declare @dataset_id int;

  insert into vdt.dataset (model_id, dataset_name, origin, last_update, [description], created, category_id, deleted)
  select @model_id, @dataset_name, 'user', CURRENT_TIMESTAMP, @description, CURRENT_TIMESTAMP, category_id, 0
  from vdt.dataset_category where category_name=@category;
  set @dataset_id=SCOPE_IDENTITY();

  insert into vdt.value (dataset_id, attr_id, value) select @dataset_id, attr_id, value from #backcalcs

  insert into @results (severity, [message]) values ('id', cast(@dataset_id as nvarchar));
  insert into @results (severity, [message]) values ('info', 'Data import complete');
end

drop table #Final_Calc_Dataset;
drop table #Final_Calc_Dataset_Complete;
drop table #MiningCostDrivers;
drop table #MiningCostRates;
drop table #retrieve_attribute_id;
drop table #final_attribute;
drop table #fulljoin;
drop table #ProductionTime
drop table #CostRates
drop table #ActiveRakesCalc
drop table #HaulAdjustments
drop table #LoadAdjustments
drop table #LoadHaulRHRate
drop table #PortCostDrivers
drop table #backcalcs


  -- return report to the user
  select * from @results;
end
go

