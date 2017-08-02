--USE *database_name*

create schema vdt;
go
create schema report;
go

create table vdt.[grouping](
	[grouping_id] int identity not null primary key,
	is_filter bit not null,
	grouping_name nvarchar(100) not null);

insert into vdt.[grouping] (is_filter, grouping_name) values (0, 'Qerent');

create table vdt.category(
	category_id int identity not null primary key,
	category_name nvarchar(100) not null,
	parent_category_id int null references vdt.category (category_id),
	[grouping_id] int not null references vdt.[grouping] ([grouping_id]));

create table vdt.attribute_category(
	attr_id int not null,
	[grouping_id] int not null,
	category_id int not null references vdt.category (category_id),
	constraint attribute_category_attr_id_grouping_id primary key (attr_id, [grouping_id]));

create nonclustered index attr_category_grouping_id on vdt.attribute_category (grouping_id) include ([attr_id], [category_id]);

create table vdt.[user] (
  user_id uniqueidentifier DEFAULT NEWID() not null primary key,
  login nvarchar(128) not null unique,   -- will be email address
  sec_level int,
  ad_name nvarchar(128) unique,          -- AD user name e.g. WAIO\msmith
  pwd_hash nvarchar(128),               -- if not an AD user
  is_power_user [bit] not null,
  is_admin_user [bit] not null);
  
CREATE TABLE [vdt].[model_group](
	[model_group_id] int identity primary key,
	[name] [nvarchar](50) not null,
	[is_offline] [bit] not null,
	[identifier] [nvarchar](50) not null,
	[instances] [int] not null
);

CREATE TABLE [vdt].[access_group](
	[access_group_id] int identity primary key,
	[security_level] [int] not null,
	[ad_name] [nvarchar](50) not null,
	[model_group_id] int not null references vdt.model_group (model_group_id)
);

CREATE TABLE [vdt].[super_user_group](
	[super_user_group_id] int identity primary key,
	[ad_name] [nvarchar](50) not null,
	[model_group_id] int not null references vdt.model_group (model_group_id)
	);

CREATE TABLE [vdt].[model](
	[model_id] int identity primary key,
	[filename] [nvarchar](256) not null,
	[file_date] [datetime] not null,
	[is_active] [bit] not null,
	[version] [int] not null,
	[connection_string] [nvarchar](256) not null,
	[model_group_id] int not null references vdt.model_group (model_group_id),
	[last_rollback] int null,
	[rollback_filename] [nvarchar](256) NULL,
	[rollback_file_date] [datetime] NULL,
	[top_of_tree_attr_id] int null,
	[upload_complete] bit not null default(0)
	);

create table vdt.[attribute] (
  attr_id int identity not null primary key,
  attr_name nvarchar(100) not null,
  unit nvarchar(20) not null,
  sec_level int not null,
  is_cost bit not null,
  is_lever bit not null,
  is_calculated bit not null,
  is_aggregate bit not null,
  is_non_driver bit not null default(0),
  is_kpi bit not null);

create table vdt.unit_format (
  unit nvarchar(20) not null primary key,
  format nvarchar(20) not null default '{0:4a}');
  
create table [vdt].[dataset_category] (
  [category_id] [int] identity not null primary key,
  [category_name] [nvarchar](100) NOT NULL)
  
create table vdt.[dataset] (
  dataset_id int identity not null primary key,
  model_id int not null references vdt.model (model_id),
  dataset_name nvarchar(100) not null,
  origin nvarchar(10) not null,   -- one of: system, benchmark, scenario
  last_update datetime not null,
  [description] nvarchar(1000) null,
  created datetime not null default current_timestamp,
  category_id int not null references vdt.dataset_category (category_id),
  [model_version] [int] NOT NULL DEFAULT ((1)),
  deleted bit not null default 0);

create table vdt.[scenario] (
  scenario_id int identity not null primary key,
  model_id int not null references vdt.model (model_id),
  base_dataset_id int not null references vdt.dataset (dataset_id),  -- dataset on which this scenario is based (overlaid)
  view_dataset_id int not null references vdt.dataset (dataset_id),  -- materialized view of this scenario as a dataset
  scenario_name nvarchar(100) not null,
  created_by uniqueidentifier not null references vdt.[user] (user_id),
  editing_by uniqueidentifier references vdt.[user] (user_id),
  is_public bit not null);

create table vdt.[scenario_change] (
  scenario_id int not null references vdt.[scenario] (scenario_id),
  attr_id int not null references vdt.[attribute] (attr_id),
  new_value float not null,
  primary key (scenario_id, attr_id));

create table vdt.[scenario_change_log] (
  scenario_id int not null references vdt.[scenario] (scenario_id),
  attr_id int not null references vdt.[attribute] (attr_id),
  user_id uniqueidentifier not null references vdt.[user] (user_id),
  change_date datetime not null,
  old_value float not null,
  new_value float);          -- null indicates reversion to the base data

create table vdt.[value] (
  dataset_id int not null references vdt.[dataset] (dataset_id),
  attr_id int not null references vdt.[attribute] (attr_id),
  value float not null,
  is_calc_redundant bit not null default(0),
  primary key (dataset_id, attr_id));

create index value_dataset on vdt.[value] (dataset_id);

CREATE NONCLUSTERED INDEX [_dta_index_value_50_1335675806__K2_K1_3_1410] ON [vdt].[value]
(
                [attr_id] ASC,
                [dataset_id] ASC
)
INCLUDE ([value]) WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]
GO

create table vdt.[sensitivity] (
  sensitivity_id int identity not null primary key,
  dataset_id int not null references vdt.[dataset] (dataset_id),
  percent_change float not null default 0.1,
  target_attr_id int not null references vdt.[attribute] (attr_id),  -- usually EBITDA
  target_base_value float not null,                                  -- what was it at the time the sensitivity was run
  last_update datetime not null);

create table vdt.[sensitivity_value] (
  sensitivity_id int not null references vdt.[sensitivity] (sensitivity_id),
  attr_id int not null references vdt.[attribute] (attr_id),
  impact_of_increase float not null,
  impact_of_decrease float not null,
  primary key (sensitivity_id, attr_id));

create table vdt.[attribution] (
  attribution_id int identity not null primary key,
  base_dataset_id int not null references vdt.[dataset] (dataset_id),
  benchmark_dataset_id int not null references vdt.[dataset] (dataset_id),
  is_cumulative bit not null,
  target_attr_id int not null references vdt.[attribute] (attr_id),
  target_base_value float not null,
  last_update datetime not null);

create table vdt.[attribution_value] (
  attribution_id int not null references vdt.[attribution] (attribution_id),
  attr_id int not null references vdt.[attribute] (attr_id),
  seq_number int,           -- in a cumulative attribution this will be a 1-based number giving the sequence of changes
  impact_on_target float not null,
  primary key (attribution_id, attr_id));
  
  
create type [vdt].[IdFilterTableType] AS TABLE(
	[id] int not null
)

create table [vdt].[audit_category](
	[audit_category_id] int identity not null primary key,
	[name] [nvarchar](100) not null);

create table [vdt].[audit_entry](
	[audit_id] int identity not null primary key,
	[audit_date] [datetime] not null,
	[category] [int] not null references vdt.[audit_category] ([audit_category_id]),
	[sub_category] [int] null references vdt.[audit_category] ([audit_category_id]),
	[user_id] [uniqueidentifier] not null)

SET IDENTITY_INSERT [vdt].[audit_category] ON	

insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (1, N'Scenario Management')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (2, N'Scenario Planning')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (3, N'Sensitivity Analysis')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (4, N'Size of Prize Analysis')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (5, N'System View')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (6, N'Value Driver Tree')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (7, N'View')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (8, N'Calculate')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (9, N'Copy')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (10, N'Edit')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (11, N'Delete')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (12, N'Save as')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (13, N'Toggle lock')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (14, N'Load')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (15, N'Import data')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (16, N'Export to excel')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (17, N'Login')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (18, N'Custom Account')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (19, N'Windows Authentication')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (20, N'Watch Window')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (21, N'Add Attribute(s)')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (22, N'Remove Attribute(s)')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (23, N'Admin Console')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (24, N'Create Model')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (25, N'Upload Model')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (26, N'Update Model')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (27, N'Rollback Model')
insert into [vdt].[audit_category] ([audit_category_id],[name]) VALUES (28, N'Test Connection')

SET IDENTITY_INSERT [vdt].[audit_category] OFF

insert into vdt.unit_format (unit, [format]) values ('#', '{0:3s}');
insert into vdt.unit_format (unit, [format]) values ('$', '{0:2f}');
insert into vdt.unit_format (unit, [format]) values ('$/day', '{0:2f}');
insert into vdt.unit_format (unit, [format]) values ('$/hr', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('$/kg', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('$/L', '{0:4f}');
insert into vdt.unit_format (unit, [format]) values ('$/m', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('$/t', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('%', '{0:3%}');
insert into vdt.unit_format (unit, [format]) values ('$M', '{0:4,1000000s}');
insert into vdt.unit_format (unit, [format]) values ('A$M', '{0:4,1000000s}');
insert into vdt.unit_format (unit, [format]) values ('bcm/m', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('day', '{0:2s}');
insert into vdt.unit_format (unit, [format]) values ('hr', '{0:3s}');
insert into vdt.unit_format (unit, [format]) values ('kg/t', '{0:3s}');
insert into vdt.unit_format (unit, [format]) values ('L', '{0:3s}');
insert into vdt.unit_format (unit, [format]) values ('L/hr', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('m', '{0:3s}');
insert into vdt.unit_format (unit, [format]) values ('m/hr', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('m3/bcm', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('Mt', '{0:4,1000000s}');
insert into vdt.unit_format (unit, [format]) values ('Mtpa', '{0:4,1000000s}');
insert into vdt.unit_format (unit, [format]) values ('t', '{0:2s}');
insert into vdt.unit_format (unit, [format]) values ('t/day', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('t/hr', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('t:t', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('$/$', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('$/service', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('A$M/t', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('cycles/service', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('km/cycle', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('km/t', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('MWh/service', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('MWh/t', '{0:4s}');
insert into vdt.unit_format (unit, [format]) values ('t/m', '{0:4s}');

insert into vdt.dataset_category (category_name) values ('Actuals');
insert into vdt.dataset_category (category_name) values ('Target');
insert into vdt.dataset_category (category_name) values ('Scenario');
insert into vdt.dataset_category (category_name) values ('Benchmark');

go

create table vdt.attribute_dependency (
  attr_id int not null references vdt.attribute (attr_id),
  depends_on_attr_id int not null references vdt.attribute (attr_id),
  model_id int not null references vdt.model(model_id),
  primary key (attr_id, depends_on_attr_id,model_id)
);


create table vdt.[config] (
  config_id int identity primary key,
  db_version int not null,
  updated_on date not null);

create table vdt.[customization] (
  sproc nvarchar(100) not null primary key,
  updated_on date not null,
  sp_version int);
  
CREATE TYPE [vdt].[AccessGroupType] AS TABLE(
	[access_group_id] [int] NOT NULL,
	[security_level] [int] NOT NULL,
	[ad_name] [nvarchar](200) NOT NULL
);
GO

CREATE TYPE [vdt].[SuperUserAccessGroupType] AS TABLE(
	[super_user_access_group_id] [int] NOT NULL,
	[ad_name] [nvarchar](200) NOT NULL
);
GO

create table vdt.vdt_structure(
	vdt_id int identity not null primary key,
	name nvarchar(100) not null,	
)

create table vdt.vdt_node(
	vdt_node_id int identity not null primary key,
	nodeId int not null,
	attributeId int not null references vdt.attribute (attr_id),
	name nvarchar(100) not null,
	vdt_structure_id int not null references vdt.vdt_structure(vdt_id),
	link nvarchar(100)  null);
	
create table vdt.vdt_edge(
    vdt_node_id int identity not null,
	parentId int not null references vdt.vdt_node(vdt_node_id),
	childId int not null  references vdt.vdt_node(vdt_node_id),
	vdt_structure_id int not null references vdt.vdt_structure(vdt_id));

go

CREATE TYPE [vdt].[AttrPathType] AS TABLE(
	[path] [nvarchar](500) NULL
)
GO

CREATE view [vdt].[data] as
select
    DS.dataset_id as dataset_id,
	DSS.scenario_id as scenario_id,
	DS.dataset_name as dataset,
	A.attr_id,
	A.attr_name as attribute,
	A.unit as attribute_unit,
	A.sec_level as attribute_sec_level,
	A.is_cost as attribute_is_cost,
	A.is_lever as attribute_is_lever,
	A.is_aggregate as attribute_is_aggregate,
	A.is_kpi as attribute_is_kpi,
	A.is_calculated as attribute_is_calculated,
	V.is_calc_redundant as attribute_is_calc_redundant,
	A.is_non_driver as attribute_is_non_driver,
    coalesce(C.new_value, V.value) as value,
	coalesce(coalesce(BC.new_value, B.value),V.Value) as base_value,
	C.new_value as override_value
from vdt.value V 
inner join vdt.dataset DS on V.dataset_id=DS.dataset_id
inner join vdt.attribute A on V.attr_id=a.attr_id
left join vdt.scenario DSS on DS.dataset_id = DSS.view_dataset_id
left join vdt.scenario BDSS on DSS.base_dataset_id = BDSS.view_dataset_id
left join vdt.scenario_change C on DSS.scenario_id=C.scenario_id and A.attr_id=C.attr_id
left join vdt.scenario_change BC on BDSS.scenario_id = BC.scenario_id and A.attr_id = BC.attr_id
left join vdt.value B on V.attr_id = B.attr_id and B.dataset_id = DSS.base_dataset_id


GO


insert into vdt.config (db_version, updated_on) values (19, getdate());
