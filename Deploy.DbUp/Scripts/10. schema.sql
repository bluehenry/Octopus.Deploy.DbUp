--USE *database_name*

create schema vdt;
go

create table vdt.[user] (
  user_id int identity not null primary key,
  login nvarchar(128) not null unique,   -- will be email address
  sec_level int,
  ad_name nvarchar(128) unique,          -- AD user name e.g. WAIO\msmith
  pwd_hash nvarchar(128),               -- if not an AD user
  is_power_user [bit] not null,
  is_admin_user [bit] not null);
  
create table vdt.[org_group] (
  og_id int identity not null primary key,
  og_name nvarchar(100) not null unique);

create table vdt.[function] (
  func_id int identity not null primary key,
  func_name nvarchar(100) not null unique);

create table vdt.[activity] (
  act_id int identity not null primary key,
  act_name nvarchar(100) not null unique);

create table vdt.[workstream] (
  ws_id int identity not null primary key,
  ws_name nvarchar(100) not null unique);

create table vdt.[location] (
  loc_id int identity not null primary key,
  loc_name nvarchar(100) not null unique);

create table vdt.[eqp_type] (
  eqp_type_id int identity not null primary key,
  eqp_type_name nvarchar(100) not null unique);

create table vdt.[equipment] (
  eqp_id int identity not null primary key,
  eqp_name nvarchar(100) not null unique);

create table vdt.[cost_type] (
  ct_id int identity not null primary key,
  ct_name nvarchar(100) not null unique);

create table vdt.[product] (
  prod_id int identity not null primary key,
  prod_name nvarchar(100) not null unique);

CREATE TABLE [vdt].[model_group](
	[model_group_id] int identity primary key,
	[name] [nvarchar](50) not null,
	[is_offline] [bit] not null,
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
	[top_of_tree_attr_id] int null
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
  is_kpi bit not null,
  og_id int references vdt.org_group (og_id),
  func_id int references vdt.[function] (func_id),
  act_id int references vdt.activity (act_id),
  ws_id int references vdt.workstream (ws_id),
  loc_id int references vdt.location (loc_id),
  eqp_type_id int references vdt.eqp_type (eqp_type_id),
  eqp_id int references vdt.equipment (eqp_id),
  ct_id int references vdt.cost_type (ct_id),
  prod_id int references vdt.product (prod_id));

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
  deleted bit not null default 0);

create table vdt.[scenario] (
  scenario_id int identity not null primary key,
  model_id int not null references vdt.model (model_id),
  base_dataset_id int not null references vdt.dataset (dataset_id),  -- dataset on which this scenario is based (overlaid)
  view_dataset_id int not null references vdt.dataset (dataset_id),  -- materialized view of this scenario as a dataset
  scenario_name nvarchar(100) not null,
  created_by int not null references vdt.[user] (user_id),
  editing_by int references vdt.[user] (user_id),
  is_public bit not null);

create table vdt.[scenario_change] (
  scenario_id int not null references vdt.[scenario] (scenario_id),
  attr_id int not null references vdt.[attribute] (attr_id),
  new_value float not null,
  primary key (scenario_id, attr_id));

create table vdt.[scenario_change_log] (
  scenario_id int not null references vdt.[scenario] (scenario_id),
  attr_id int not null references vdt.[attribute] (attr_id),
  user_id int not null references vdt.[user] (user_id),
  change_date datetime not null,
  old_value float not null,
  new_value float);          -- null indicates reversion to the base data

create table vdt.[value] (
  dataset_id int not null references vdt.[dataset] (dataset_id),
  attr_id int not null references vdt.[attribute] (attr_id),
  value float not null,
  primary key (dataset_id, attr_id));

create index value_dataset on vdt.[value] (dataset_id);

go
create view [vdt].[data] as
select
    DS.dataset_id as dataset_id,
	DSS.scenario_id as scenario_id,
	DS.dataset_name as dataset,
	OG.og_name as org_group,
	FN.func_name as [function],
	ACT.act_name as activity,
	WS.ws_name as workstream,
	LOC.loc_name as location,
	ETP.eqp_type_name as equipment_type,
	EQP.eqp_name as equipment,
	CT.ct_name as cost_type,
	PR.prod_name as product,
	A.attr_id,
	A.attr_name as attribute,
	A.unit as attribute_unit,
	A.sec_level as attribute_sec_level,
	A.is_cost as attribute_is_cost,
	A.is_lever as attribute_is_lever,
	A.is_aggregate as attribute_is_aggregate,
	A.is_kpi as attribute_is_kpi,
    coalesce(C.new_value, V.value) as value,
	coalesce(coalesce(BC.new_value, B.value),V.Value) as base_value,
	C.new_value as override_value
from vdt.value V 
inner join vdt.dataset DS on V.dataset_id=DS.dataset_id
inner join vdt.attribute A on V.attr_id=a.attr_id
left join vdt.org_group OG on A.og_id=OG.og_id
left join vdt.[function] FN on A.func_id=FN.func_id
left join vdt.[workstream] WS on A.ws_id=WS.ws_id
left join vdt.location LOC on A.loc_id=LOC.loc_id
left join vdt.activity ACT on A.act_id=ACT.act_id
left join vdt.eqp_type ETP on A.eqp_type_id=ETP.eqp_type_id
left join vdt.equipment EQP on A.eqp_id=EQP.eqp_id
left join vdt.cost_type CT on A.ct_id=CT.CT_id
left join vdt.product PR on A.prod_id=PR.prod_id
left join vdt.scenario DSS on DS.dataset_id = DSS.view_dataset_id
left join vdt.scenario BDSS on DSS.base_dataset_id = BDSS.view_dataset_id
left join vdt.scenario_change C on DSS.scenario_id=C.scenario_id and A.attr_id=C.attr_id
left join vdt.scenario_change BC on BDSS.scenario_id = BC.scenario_id and A.attr_id = BC.attr_id
left join vdt.value B on V.attr_id = B.attr_id and B.dataset_id = DSS.base_dataset_id

GO


create view vdt.attribute_category as
select A.attr_id, A.attr_name as attribute, O.og_name as org_group, L.loc_name as location,
  F.func_name as [function], X.act_name as activity, W.ws_name as workstream,
  T.eqp_type_name as [equipment_type], E.eqp_name as equipment,
  C.ct_name as cost_type, P.prod_name as product,
  A.unit as attribute_unit, A.sec_level as attribute_sec_level,
  A.is_cost as attribute_is_cost, A.is_lever as attribute_is_lever,
  A.is_aggregate as attribute_is_aggregate, A.is_kpi as attribute_is_kpi,
  O.og_id, L.loc_id, F.func_id, X.act_id, W.ws_id, T.eqp_type_id, E.eqp_id, C.ct_id, P.prod_id
from vdt.attribute A
left join vdt.org_group O on A.og_id=O.og_id
left join vdt.location L on A.loc_id=L.loc_id
left join vdt.[function] F on A.func_id=F.func_id
left join vdt.activity X on A.act_id=X.act_id
left join vdt.workstream W on A.ws_id=W.ws_id
left join vdt.eqp_type T on A.eqp_type_id=T.eqp_type_id
left join vdt.equipment E on A.eqp_id=E.eqp_id
left join vdt.cost_type C on A.ct_id=C.ct_id
left join vdt.product P on A.prod_id=P.prod_id
go

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
	[user_id] [int] not null)

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
  primary key (attr_id, depends_on_attr_id)
);


create table vdt.[config] (
  config_id int identity primary key,
  db_version int not null,
  updated_on date not null);
  
CREATE TYPE [vdt].[AccessGroupType] AS TABLE(
	[access_group_id] [int] NOT NULL,
	[security_level] [int] NOT NULL,
	[ad_name] [nvarchar](200) NOT NULL
)
GO

CREATE TYPE [vdt].[SuperUserAccessGroupType] AS TABLE(
	[super_user_access_group_id] [int] NOT NULL,
	[ad_name] [nvarchar](200) NOT NULL
)
GO

insert into vdt.config (db_version, updated_on) values (17, getdate());
