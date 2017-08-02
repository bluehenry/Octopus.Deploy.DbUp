--USE *database_name*

create procedure vdt.sp_get_unit_formats
as
begin
  set nocount on;

  select unit, [format] from vdt.unit_format;
end
go

create procedure [vdt].[sp_get_attributes]
@user_id uniqueidentifier
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT attr_id, attr_name
  FROM [vdt].[attribute]
  WHERE (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  sec_level

END 


GO


create procedure [vdt].[sp_get_user_by_id]
       @user_id uniqueidentifier               
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
	[is_revert] [bit] NULL
)
GO

GO

CREATE TYPE [vdt].[FilterTableType] AS TABLE(
	[item] [nvarchar](100) NULL
)
GO

create procedure [vdt].[sp_update_scenario_value]
 @stt ScenarioTableType READONLY, 
 @user_id uniqueidentifier
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

    DELETE [vdt].[scenario_change] 
      FROM  @stt change
     INNER JOIN  [vdt].[scenario_change] original
        ON original.scenario_id = change.scenario_id AND original.attr_id = change.attr_id
     WHERE change.is_revert = 1


	INSERT INTO  [vdt].[scenario_change_log] (scenario_id, attr_id,new_value ,old_value,[user_id],change_date) 
	  SELECT s.scenario_id, s.attr_id,s.new_value,v.value,@user_id,GETDATE()
			FROM  @stt s
			inner join vdt.scenario sc on sc.scenario_id = s.scenario_id
			inner join vdt.value v on v.attr_id = s.attr_id and v.dataset_id = sc.view_dataset_id
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
   @user_id uniqueidentifier
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
		 ,Dataset.model_version
  FROM [vdt].[dataset] Dataset
  inner join vdt.model Model on Dataset.model_id=Model.model_id
  left join vdt.scenario Scenario on Scenario.view_dataset_id = Dataset.dataset_id
  left join vdt.[dataset] [BaseDataset] on BaseDataset.dataset_id =  Scenario.base_dataset_id
  left join vdt.[user] [LockUser] on Scenario.editing_by = [LockUser].[user_id]
  left join vdt.[user] [OwnerUser] on Scenario.created_by = [OwnerUser].[user_id]
  left join vdt.[value] [Data] on [Data].dataset_id = [Dataset].dataset_id 
  left join vdt.[attribute] [Attr] on [Data].attr_id=[Attr].attr_id
  left join vdt.[unit_format] [Format] on [Format].unit = [Attr].unit 
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
   @user_id uniqueidentifier,
   @category_id int,
   @is_public bit,
   @model_version int
AS 


declare @model_id int;
declare @dataset_id int;
select @model_id =model_id from VDT.dataset where dataset_id = @base_dataset_id

BEGIN 
     SET NOCOUNT ON 

INSERT into VDT.dataset (model_id, dataset_name,origin,last_update,[description],created,deleted,category_id,model_version)
values(@model_id,@dataset_name,'user',GETDATE(),@description,GETDATE(),0,@category_id,@model_version)

select @dataset_id= CAST(scope_identity() AS int)

INSERT into VDT.value 
(dataset_id, attr_id, value) 
select @dataset_id, V.attr_id, coalesce(C.new_value, V.value)  from VDT.value as V
inner join vdt.dataset DS on V.dataset_id=DS.dataset_id
left join vdt.scenario DSS on DS.dataset_id = DSS.view_dataset_id
left join vdt.scenario BDSS on DSS.base_dataset_id = BDSS.view_dataset_id
left join vdt.scenario_change as C on DSS.scenario_id=C.scenario_id and V.attr_id=C.attr_id
left join vdt.scenario_change BC on BDSS.scenario_id = BC.scenario_id and V.attr_id = BC.attr_id
where V.dataset_id = @base_dataset_id

 
INSERT into VDT.scenario 
(model_id, base_dataset_id, view_dataset_id,scenario_name,created_by,editing_by,is_public) 

values (@model_id,@base_dataset_id,@dataset_id,@dataset_name,@user_id,null,@is_public)

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
   @user_id uniqueidentifier = null
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
   @scenario_name nvarchar(100),
   @user_id uniqueidentifier  
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT CASE WHEN Count(dataset_id) = 1 THEN 0 ELSE 1 END
  FROM [vdt].[dataset]
  WHERE dataset_name = @dataset_name  And exists(select * from vdt.scenario where created_by=@user_id and scenario_name = @dataset_name)

  UNION ALL

  SELECT CASE WHEN Count(scenario_id) = 1 THEN 0 ELSE 1 END 
  FROM [vdt].[scenario]
  WHERE scenario_name = @scenario_name and created_by = @user_id
  

END


GO


create procedure [vdt].[sp_get_user_details]
	@user_id uniqueidentifier
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
	@user_id uniqueidentifier
AS
BEGIN
	SET NOCOUNT ON;

		SELECT 
			A.attr_id,
			A.attr_name as attribute,
			A.is_cost, 
			A.is_lever as attribute_is_lever,
			A.is_kpi as attribute_is_kpi,
			SV.impact_of_decrease , 
			SV.impact_of_increase
		
		FROM [vdt].[sensitivity] S
			inner JOIN [vdt].[sensitivity_value] SV ON (SV.sensitivity_id = S.sensitivity_id) 		
			left join vdt.attribute A on A.attr_id = sv.attr_id

		WHERE s.dataset_id = @DataSetId
		AND (select sec_level from vdt.[user] where [user_id] = @user_id)  >=  A.sec_level
END


GO


create procedure [vdt].[sp_get_attribution_analysis_chart_data]
	@BaseDatasetId int,
	@BenchmarkDatasetId int,
	@user_id uniqueidentifier
AS
BEGIN
	SET NOCOUNT ON;

		SELECT 
			A.attr_id,
			A.attr_name as attribute,
			A.is_cost, 
			A.is_lever as attribute_is_lever,
			A.is_kpi as attribute_is_kpi,
			UF.[format] as [format],
			AV.impact_on_target, 
			COALESCE(SC.new_value, DS.value) as scenario_value, 
			DB.value as benchmark_value
			
		
		FROM [vdt].[attribution] AA
			inner JOIN [vdt].[attribution_value] AV ON (AV.attribution_id = AA.attribution_id) 		
			left join vdt.attribute A on A.attr_id = AV.attr_id
			left join vdt.unit_format UF on A.unit = UF.unit			
			left join [vdt].[value] DS on (DS.dataset_id =  @BaseDatasetId AND DS.attr_id = A.attr_id)
			left join [vdt].[value] DB on (DB.dataset_id =  @BenchmarkDatasetId AND DB.attr_id = A.attr_id)
			left join vdt.scenario DSS on DS.dataset_id = DSS.view_dataset_id
			left join vdt.scenario BDSS on DSS.base_dataset_id = BDSS.view_dataset_id
			left join vdt.scenario_change SC on DSS.scenario_id=SC.scenario_id and A.attr_id=SC.attr_id

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
		'Location,Activity,Equipment,Accountability,Cost Type,Product' as groupings, -- TODO: remove this
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

create procedure [vdt].[sp_get_vdt_data]
@dataset_id int, 
@benchmark_id int=null,
@user_id uniqueidentifier,
@vdtStructureId int

AS 
BEGIN 
     SET NOCOUNT ON 

	 declare @sec_level int;
	 set @sec_level = (select sec_level from vdt.[user] where [user_id] = @user_id);

SELECT
	V.[attr_id] as AttributeId
    ,UF.[format] as ValueFormat
	,UF.[unit] as ValueUnit,
	 A.is_kpi as AttributeIsKpi,
	 A.is_lever as AttributeIsLever,
	 A.is_calculated as AttributeIsCalculated,
	 A.is_non_driver as AttributeIsNonDriver,
	 V.is_calc_redundant as AttributeIsCalcRedundant
	,CASE WHEN @sec_level >=  A.sec_level THEN coalesce(B.value, V.value) ELSE null END AS  Actual
	,CASE WHEN @sec_level >=  A.sec_level THEN BM.value ELSE null END AS [Target]
	,coalesce(((BM.value - coalesce(B.value, V.value)) / nullif(coalesce(B.value, V.value),0)),null) as TargetChange
	,coalesce(((coalesce(C.new_value, V.value) - coalesce(B.value, V.value)) / nullif(coalesce(B.value, V.value),0)),null) as WhatIfChange
	,CASE WHEN @sec_level >=  A.sec_level THEN coalesce(C.new_value, V.value) ELSE null END AS WhatIf
	,C.new_value as OverrideValue
	,1 as IncreaseIsGood
	,node.vdt_node_id as NodeId
	,node.name as Name
  FROM [vdt].[value] V
  inner join vdt.attribute A on V.attr_id = A.attr_id
  inner join vdt.dataset DS on V.dataset_id = DS.dataset_id
  left join vdt.scenario DSS on DS.dataset_id = DSS.view_dataset_id
  left join vdt.scenario_change C on DSS.scenario_id = C.scenario_id and A.attr_id = C.attr_id
  left join vdt.unit_format UF on A.unit = UF.unit
  left join vdt.value B on V.attr_id = B.attr_id and B.dataset_id = DSS.base_dataset_id
  left join vdt.value BM on V.attr_id = BM.attr_id and BM.dataset_id = @benchmark_id
    left join vdt.vdt_node node on node.attributeId  = V.[attr_id] and node.vdt_structure_id = @vdtStructureId 
  WHERE V.dataset_id = @dataset_id
  AND (A.attr_id IN ( select attributeId  from vdt.vdt_node where vdt_structure_id = @vdtStructureId))
  
END

GO


create procedure [vdt].[sp_get_sensitivity_analysis_summary]
	@DataSetId int
AS
BEGIN

	SET NOCOUNT ON;

		SELECT 
		A.attr_name,
		'Location,Activity,Equipment,Accountability,Cost Type,Product' as groupings, -- TODO: remove this
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
@user_id uniqueidentifier

AS 
BEGIN 
     SET NOCOUNT ON 

select sec_level from vdt.[user] where [user_id] = @user_id
  
END 

GO

create procedure vdt.sp_get_models
  @user_id uniqueidentifier
as
begin
  set nocount on;

   select model.model_id, model.[version], model.file_date,mg.name from vdt.model model
  inner join vdt.model_group mg on mg.model_group_id = model.model_group_id;
end
GO

create procedure [vdt].[sp_get_filters]
as
begin
  set nocount on;
  	select grouping_name,category_name,category_id,parent_category_id,grouping.grouping_id,is_filter from vdt.category category
		inner join  vdt.grouping grouping on grouping.grouping_id = category.grouping_id
end

GO

create procedure [vdt].[sp_update_user]
	   @user_id uniqueidentifier,
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
  @user_id uniqueidentifier
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

 -- SITES CAN CUSTOMIZE HERE
 -- END CUSTOMIZATION

 select * from @views

END 

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
	   @user_id uniqueidentifier	                   
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

create procedure [vdt].[sp_get_model_groups]
@group_names FilterTableType READONLY,
@admin_user int 
AS 
BEGIN 
     SET NOCOUNT ON 
	declare @sec_level int,@ad_name nvarchar(200),@is_admin_user int, @user_id uniqueidentifier;
	set @sec_level = (select sec_level from vdt.[user] where [user_id] = @user_id);
	set @ad_name = (select ad_name from vdt.[user] where [user_id] = @user_id);
	set @is_admin_user = (select is_admin_user from vdt.[user] where [user_id] = @user_id);

	IF @admin_user = 0
	BEGIN
	select DISTINCT mg.model_group_id,mg.name as model_group_name,m.model_id,m.[filename],m.[version] as model_version, mg.identifier as identifier,mg.[instances]

	 from vdt.model_group as mg
	 inner join vdt.model as m on m.model_group_id = mg.model_group_id
	 inner join vdt.access_group as ag on m.model_group_id = ag.model_group_id
	 where m.is_active = 1 and m.upload_complete = 1 and (ag.ad_name  in (select item from @group_names)) order by mg.model_group_id ASC
	 -- @sec_level>= ag.security_level
	 END
	 ELSE
	  BEGIN
	     select DISTINCT mg.model_group_id,mg.name as model_group_name,m.model_id,m.[filename],m.[version] as model_version, mg.identifier as identifier ,mg.[instances]

		 from vdt.model_group as mg
		 inner join vdt.model as m on m.model_group_id = mg.model_group_id
		 inner join vdt.access_group as ag on m.model_group_id = ag.model_group_id
		 where m.is_active = 1 and m.upload_complete = 1  order by mg.model_group_id ASC
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
	   mg.identifier as identifier,
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
   @identifier uniqueidentifier,
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

	 INSERT INTO [vdt].[model_group] ([name],[is_offline],[instances],[identifier])
     VALUES (@model_group_name,0,@no_of_instances,@identifier)

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

select mg.model_group_id,mg.name as model_group_name,m.upload_complete,m.model_id,m.[filename],m.[version] ,mg.[instances]
 from vdt.model_group as mg
 inner join vdt.model as m on m.model_group_id = mg.model_group_id
 where m.is_active = 1

END
go

create procedure [vdt].[sp_replicate_insert_update_user]
	   @user_id uniqueidentifier,
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

select mg.model_group_id,mg.name as model_group_name,m.model_id,m.[filename],m.[version] as model_version,mg.identifier as identifier,mg.[instances]
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
   @model_id int,
   @model_identifier nvarchar(50)
AS 

BEGIN 
     SET NOCOUNT ON 
	 
	 SET IDENTITY_INSERT [vdt].[model] ON	
	
	declare @model_group_id int
	 INSERT INTO [vdt].[model_group] ([name],[is_offline],[instances],[identifier])
     VALUES (@model_group_name,0,@no_of_instances,@model_identifier)

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

create procedure vdt.sp_get_vdt_edges
@vdtStructureId int

AS 
BEGIN 
     SET NOCOUNT ON 

select parentId as ParentId,childId as ChildId from vdt.vdt_edge where vdt_structure_id = @vdtStructureId 

END 
go

CREATE  procedure [vdt].[sp_get_model_vdts]
@user_id uniqueidentifier
AS 
BEGIN 
     SET NOCOUNT ON 
	 select vdt_id,name from vdt.[vdt_structure]
END 
go

CREATE procedure [vdt].[sp_get_attributes_by_categoty_id]
@user_id uniqueidentifier,
@categoryIds vdt.IdFilterTableType READONLY

AS 
BEGIN 
     SET NOCOUNT ON 

	--select a.attr_id,a.attr_name from vdt.attribute_category as ac
	--	inner join vdt.attribute as a on a.attr_id = ac.attr_id
	--where ac.category_id in (select id from @categoryIds)

	select a.attr_id,a.attribute from vdt.attribute_category as ac
		inner join report.attribute_category as a on a.attr_id = ac.attr_id
	where (a.attribute_is_kpi<>0 or a.attribute_is_lever<>0) AND ac.category_id in (select id from @categoryIds)

END
go	

create  procedure [vdt].[sp_attribute_categories]
@itt vdt.IdFilterTableType READONLY
AS 
BEGIN 
     SET NOCOUNT ON 

select attr_id, ac.[grouping_id],ac.category_id, c.category_name,g.grouping_name,g.is_filter  from vdt.attribute_category  ac
inner join vdt.category c on c.category_id = ac.category_id
inner join vdt.grouping g on g.grouping_id = c.grouping_id
where attr_id in (select id from @itt)

END 

go

create procedure [vdt].[sp_get_screen_data]
  @screen nvarchar(100),
  @dataset_id int,
  @user_id uniqueidentifier
as
begin
  set nocount on;

  -- SITES CAN CUSTOMIZE HERE

declare @base_dataset_id int = @dataset_id;
select @base_dataset_id=base_dataset_id from vdt.scenario where view_dataset_id=@dataset_id;

with qerent_categories as (
select A.attr_id, Q.category_id
from vdt.attribute A
inner join vdt.attribute_category Q on A.attr_id=Q.attr_id and Q.grouping_id = (select grouping_id from vdt.grouping where grouping_name='Qerent')
)
, cat_tree as (
select grouping_id, category_id, 1 as level, parent_category_id, category_name from vdt.category
where parent_category_id is null and grouping_id=(select grouping_id from vdt.grouping where grouping_name='Qerent')
union all
select C.grouping_id, C.category_id, P.level+1 as level, C.parent_category_id, C.category_name
from vdt.category C inner join cat_tree P on C.parent_category_id=P.category_id
)
, cat_walk as (
select grouping_id, category_id, level, parent_category_id, category_name from cat_tree
union all
select C.grouping_id, C.category_id, P.level, P.parent_category_id, P.category_name
from cat_walk C inner join cat_tree P on C.parent_category_id=P.category_id
)
, pivoted as (
select grouping_id, category_id,[1],[2],[3] from cat_walk
pivot (min(category_name) for level in ([1],[2],[3])) as pt)
, cat_cols as (
select P.grouping_id, G.grouping_name, P.category_id, max([1]) as L1,max([2]) as L2,max([3]) as L3
from pivoted P inner join vdt.grouping G on P.grouping_id=G.grouping_id
group by P.grouping_id, G.grouping_name, category_id)

select
  coalesce(Q.L2, 'Summary') as tab_name, coalesce(Q.L3, 'Summary') as panel, 'DrillDownTemplate' as template, '{"groupings":["Qerent"]}' as config, J.attr_id,
  A.attr_name as label, A.attr_name as attribute, A.unit as attribute_unit, A.sec_level as attribute_sec_level,
  A.is_cost as attribute_is_cost, A.is_lever as attribute_is_lever, A.is_aggregate as attribute_is_aggregate, A.is_kpi as attribute_is_kpi,A.is_calculated as attribute_is_calculated,
  coalesce(SC.new_value,V.value) as value, coalesce(BSC.new_value,BV.value) as base_value, SC.new_value as override_value
from qerent_categories J
inner join vdt.attribute A on J.attr_id=A.attr_id
inner join vdt.value V on J.attr_id=V.attr_id and V.dataset_id=@dataset_id
inner join vdt.value BV on J.attr_id=BV.attr_id and BV.dataset_id=@base_dataset_id
left join cat_cols Q on Q.category_id=J.category_id
left join vdt.scenario S on S.view_dataset_id = @dataset_id
left join vdt.scenario BS on BS.view_dataset_id = @base_dataset_id
left join vdt.scenario_change SC on SC.attr_id=J.attr_id and SC.scenario_id=S.scenario_id
left join vdt.scenario_change BSC on BSC.attr_id = J.attr_id and BSC.scenario_id=  BS.scenario_id
where Q.L1=@screen
  and (select sec_level from vdt.[user] where [user_id] = @user_id) >=  A.sec_level
  and (A.is_lever=1 or A.is_kpi=1)
  
  -- END CUSTOMIZATION

end


go

CREATE procedure [vdt].[sp_get_screens]
  @user_id uniqueidentifier
as
begin
  set nocount on;

  -- SITES CAN CUSTOMIZE HERE
  select category_name as Screen, 'LeverDashboard' as Renderer
  from vdt.category where parent_category_id is null and grouping_id = (
    select grouping_id from vdt.grouping where grouping_name='Qerent');
  -- END CUSTOMIZATION

end

GO

create procedure vdt.sp_get_site_reports
  @site nvarchar(100),
  @dataset_id int,
  @user_id uniqueidentifier
as
begin
  set nocount on;

  declare @reports table (
    title nvarchar(50),
	sproc nvarchar(50),
	args nvarchar(100),
	renderer nvarchar(50),
	value_format nvarchar(50));

  -- SITES CAN CUSTOMIZE HERE
  -- END CUSTOMIZATION
  select * from @reports;
end

GO

create procedure [vdt].[sp_import_vdt_structure]
  --@model_id int,
  @structure staging.vdt_structure_type READONLY,
  @nodes staging.vdt_node_type READONLY,
  @edges staging.vdt_edge_type READONLY
AS 
BEGIN 
SET NOCOUNT ON

-- TODO: VDT structures should be associated with a model_group_id
-- we can pass in the model_id from the data adapter to look this up

declare @name nvarchar(100)
select @name = [name] from @structure;

declare @structureId int = (select vdt_id from vdt.vdt_structure where name=@name) /* and model_group_id=(select model_group_id from vdt.model where model_id=@model_id) */
if @structureId is null
begin
  insert into vdt.vdt_structure ([name]) values (@name);
  select @structureId= CAST(scope_identity() AS int)
end
else
begin
  delete from vdt.vdt_edge where vdt_structure_id=@structureId;
  delete from vdt.vdt_node where vdt_structure_id=@structureId;
end

--declare @node_ids table (qerent_node_id int, db_node_id int);
insert into vdt.vdt_node (nodeId, attributeId, name, vdt_structure_id, link)
--output inserted.nodeId, inserted.vdt_node_id into @node_ids
select N.nodeId, N.attributeId, N.name, @structureId, N.link from @nodes N

insert into vdt.vdt_edge (parentId,childId,vdt_structure_id) (
select parent.vdt_node_id,child.vdt_node_id,@structureId from @edges
inner join vdt.vdt_node parent  on parent.nodeId = parentId and parent.vdt_structure_id = @structureId
inner join vdt.vdt_node child  on child.nodeId = childId and child.vdt_structure_id = @structureId
)

END 

GO

CREATE procedure [vdt].[sp_get_attribute_by_id]
@dataset_id int, 
@user_id uniqueidentifier,
@attributeIds vdt.IdFilterTableType READONLY

AS 
BEGIN 
     SET NOCOUNT ON 

	 declare @sec_level int;
	 set @sec_level = (select sec_level from vdt.[user] where [user_id] = @user_id);

select 
	  attr_id, attribute, attribute_unit,
	  attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi,attribute_is_calculated,attribute_is_calc_redundant,attribute_is_non_driver, value, base_value, override_value
	from vdt.data where dataset_id=@dataset_id
  AND (attr_id IN (select id from @attributeIds)) 
  AND  @sec_level>=  attribute_sec_level

END 	
go

CREATE procedure [vdt].[sp_get_attributes_by_string]
@dataset_id int, 
@user_id uniqueidentifier,
@search_string nvarchar(Max)

AS 
BEGIN 
     SET NOCOUNT ON 

     declare @sec_level int;
     set @sec_level = (select sec_level from vdt.[user] where [user_id] = @user_id);
			
	select    distinct  d.attr_id, d.attribute, d.attribute_unit, d.attribute_sec_level, d.attribute_is_cost,
		      d.attribute_is_lever, d.attribute_is_aggregate, d.attribute_is_kpi,d.attribute_is_calculated,d.attribute_is_calc_redundant,d.attribute_is_non_driver, d.value, d.base_value, d.override_value
	from vdt.data as d
	inner join vdt.attribute_category as ac on ac.attr_id = d.attr_id
	inner join vdt.category as c on c.category_id = ac.category_id
	where d.dataset_id=@dataset_id
	AND   ((c.category_name like '%'+@search_string+'%') or (d.attribute like '%'+@search_string+'%'))
	AND  (d.attribute_is_kpi = 0 and d.attribute_is_lever =1)    
	AND  @sec_level>=  d.attribute_sec_level
	order by d.attribute
  
END
go

CREATE procedure [vdt].[sp_get_groupings]
@user_id uniqueidentifier
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT grouping_id,grouping_name from vdt.grouping where is_filter = 1

END 
go


CREATE procedure [vdt].[sp_get_assumption_attributes]
@dataset_id int,
@user_id uniqueidentifier,
@scenario_id int

AS 
BEGIN 
     SET NOCOUNT ON 

	 declare @sec_level int;
	 set @sec_level = (select sec_level from vdt.[user] where [user_id] = @user_id);

select 
	  attr_id, attribute, attribute_unit,
	  attribute_sec_level, attribute_is_cost, attribute_is_lever, attribute_is_aggregate, attribute_is_kpi,attribute_is_calculated,attribute_is_calc_redundant, attribute_is_non_driver,value, base_value, override_value
	from vdt.data where dataset_id=@dataset_id
  AND (attr_id IN (select attr_id from vdt.scenario_change where scenario_id = @scenario_id))
  AND  @sec_level>=  attribute_sec_level
END 	

go

create procedure [vdt].[sp_get_attribute_export_data]
@user_id uniqueidentifier,
@dataset_id int,
@attributeIds vdt.IdFilterTableType READONLY

AS 
BEGIN 
     SET NOCOUNT ON 
	 select A.attr_id as AttributeId, A.attr_name as Attribute, A.unit as Unit, A.is_cost as [Cost?],
            A.is_lever as [Lever?], A.is_aggregate as [Aggregate?], A.is_kpi as [KPI?], V.base_value as [Baseline Value],
			V.override_value as [Scenario Value], S.object_path + '[' + A.attr_name + ']' as q_i_AttributePath
			from vdt.data V inner join vdt.attribute A on A.attr_id=V.attr_id
			inner join staging.qerent_attr_ids S on S.attr_id = A.attr_id
			where V.dataset_id=@dataset_id and A.sec_level <= (select sec_level from vdt.[user] where [user_id]=@user_id)
			AND (A.attr_id IN (select id from @attributeIds)) 
END

go

create procedure [vdt].[sp_get_model_identifier]
   @model_group_id int
AS 

BEGIN 
     SET NOCOUNT ON 

	 select identifier from vdt.model_group where model_group_id = @model_group_id
END  

go

create procedure [vdt].[sp_get_attribute_ids_by_path]
@aptt vdt.AttrPathType READONLY,
@model_id int
AS 
BEGIN 
     SET NOCOUNT ON 

select attr_id,CONCAT(object_path,'[',attr_name,']') as [path] from staging.qerent_attr_ids 
where CONCAT(object_path,'[',attr_name,']') in (select [path] from @aptt)
and model_id = @model_id

END


go

create procedure [vdt].[sp_get_overriden_calculated_attributes]
@scenario_id int
AS 
BEGIN 
     SET NOCOUNT ON 

SELECT a.[attr_id]
  FROM [vdt].[attribute] a 
  left join vdt.scenario_change sc on sc.attr_id = a.[attr_id]
  where a.is_calculated = 1  and sc.new_value is not NULL and sc.scenario_id = @scenario_id

END

go

create  procedure [vdt].[sp_get_model_attribute_dependencies]
@model_id int
AS 
BEGIN 
     SET NOCOUNT ON 


SELECT ad.[attr_id],ad.[depends_on_attr_id]
  FROM [vdt].[attribute_dependency] ad
  inner join vdt.attribute a on a.attr_id = ad.[depends_on_attr_id]
  where ad.model_id = @model_id

END

go

create procedure [vdt].[sp_set_calculation_redundant]
@attribute_ids vdt.IdFilterTableType READONLY,
@scenario_id int
AS 
BEGIN 
     SET NOCOUNT ON 

update vdt.value set is_calc_redundant = 0 where is_calc_redundant = 1 and dataset_id = (select view_dataset_id from vdt.scenario where scenario_id = @scenario_id )

update vdt.value set is_calc_redundant = 1 where attr_id in (select id from @attribute_ids) and dataset_id = (select view_dataset_id from vdt.scenario where scenario_id = @scenario_id )

END 

go

create procedure [vdt].[sp_get_overriden_calculated_attribute_childs]
@attribute_id int,
@scenario_id int
AS 
BEGIN 
     SET NOCOUNT ON 

-- all posible childs of @nodeParentId
DECLARE @nodeParentId BIGINT;

SET @nodeParentId = (Select vdt_node_id from vdt.vdt_node where attributeId = @attribute_id);

WITH nodeChilds AS
(
   select ve.vdt_node_id,ve.parentId,ve.childId from vdt.vdt_edge ve where ve.parentId= @nodeParentId
   UNION ALL
   select ve.vdt_node_id,ve.parentId,ve.childId from vdt.vdt_edge ve
   INNER JOIN nodeChilds  ON ve.parentId = nodeChilds.childId
)

select vn.vdt_node_id,nc.childId,nc.parentId,vn.attributeId,vn.name,vn.vdt_structure_id,v.is_calc_redundant from vdt.vdt_node as vn
inner join nodeChilds as nc on vn.vdt_node_id= nc.parentId
inner join vdt.attribute as a on a.attr_id = vn.attributeId
inner join vdt.value as v on v.attr_id =  a.attr_id and v.dataset_id = (select view_dataset_id from vdt.scenario where scenario_id = @scenario_id )
where  vn.vdt_node_id = nc.childId
union 

select vn.vdt_node_id,nc.childId,nc.parentId,vn.attributeId,vn.name,vn.vdt_structure_id,v.is_calc_redundant from vdt.vdt_node as vn
inner join nodeChilds as nc on vn.vdt_node_id= nc.childId
inner join vdt.attribute as a on a.attr_id = vn.attributeId
inner join vdt.value as v on v.attr_id =  a.attr_id and v.dataset_id = (select view_dataset_id from vdt.scenario where scenario_id = @scenario_id )
where vn.vdt_node_id = nc.childId

END

go
create procedure [vdt].[sp_flag_model_ready]
   @model_id int
AS 

BEGIN 
     SET NOCOUNT ON 
	 update vdt.model set upload_complete = 1 where model_id = @model_id
END  

go

go