--Use [DatabaseName]; --Replace with your desired database name

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- The script will  create audit object that TECI toolkit will use
-- The objects will store details on job execution and error logs

--author: Syed et al.
--copyright (C) Dept of Biomedical Informatics, UAMS. 

-----------------------------------------------------------------------------------------------------------------------------------------------------


if (object_id('dbo.teci_job_master') is not null) drop table dbo.teci_job_master;
if (object_id('dbo.teci_job_audit') is not null) drop table dbo.teci_job_audit;
if (object_id('dbo.teci_job_error') is not null) drop table dbo.teci_job_error;
--proc
if (object_id('dbo.teci_spx_start_audit') is not null) drop procedure dbo.teci_spx_start_audit;
if (object_id('dbo.teci_spx_write_audit') is not null) drop procedure dbo.teci_spx_write_audit;
if (object_id('dbo.teci_spx_job_audit') is not null) drop procedure dbo.teci_spx_job_audit;
if (object_id('dbo.teci_spx_end_audit') is not null) drop procedure dbo.teci_spx_end_audit;
if (object_id('dbo.teci_spx_write_error') is not null) drop procedure dbo.teci_spx_write_error;


 CREATE TABLE dbo.teci_job_master
(
	job_id bigint not null identity,
	start_date datetime NULL,
	end_date datetime NULL,
	active nvarchar(1) NULL,
	username nvarchar(50) NULL,
	time_elapsed_secs numeric(38,0) NULL,
	build_id numeric(38,0) NULL,
	session_id numeric(38,0) NOT NULL,
	schema_name nvarchar(50) NULL,
	job_status nvarchar(50) NULL,
	job_name nvarchar(500) NULL,
	CONSTRAINT teci_job_master_pk PRIMARY KEY (job_id)
 ) ;
 

CREATE TABLE dbo.teci_job_audit
(
  seq_id bigint not null identity,
  job_id bigint,
  schema_name nvarchar(100) NULL,
  procedure_name nvarchar(100) NULL,
  step_desc nvarchar(500) NULL,
  step_status nvarchar(100) NULL,
  records_manipulated numeric(38,0),
  step_numeric numeric null,
  job_date datetime default getdate(),
  time_elapsed_secs integer null,
  version_id integer null,
  CONSTRAINT teci_job_audit_pk PRIMARY KEY (seq_id)
 ) ;


create table dbo.teci_job_error
(
   seq_id bigint not null identity ,
   job_id bigint,
   error_numeric integer,
   error_message nvarchar(700),
   error_id numeric,
   error_severity integer,
   error_state integer,
   error_line integer,
   error_procedure  nvarchar(100),
   error_stack  nvarchar(200),
   error_backtrace  nvarchar(200),
   CONSTRAINT teci_job_error_pk PRIMARY KEY (seq_id)
) ;
go
create procedure dbo.teci_spx_start_audit
(
	@job_name varchar(500),
	@database_name varchar(50),
	@job_id BIGINT OUTPUT
) 
as
begin
	insert into dbo.teci_job_master
		(start_date, 
		active, 
		username,
		session_id, 
		schema_name,
		job_name,
		job_status) 
	select 
		getdate(),
		'Y', 
		suser_name(),
		@@SPID, 
		@database_name,
		@job_name,
		'Running'
		
	set @job_id=@@identity
end;
go
--Proc teci_write_audit
create procedure dbo.teci_spx_write_audit (
	@job_id bigint,
	@database_name varchar(50) , 
	@procedure_name varchar (100), 
	@step_desc varchar (4000), 
	@records_manipulated bigint,
	@step_number bigint,
	@step_status varchar(50)='Success'
)
as

begin
	insert 	into dbo.teci_job_audit(
		job_id, 
		schema_name,
 		procedure_name, 
 		step_desc, 
		records_manipulated,
		step_numeric,
		step_status,
		version_id	
	)
	select
 		@job_id,
		@database_name,
		@procedure_name,
		@step_desc,
		@records_manipulated,
		@step_number,
		@step_status,
		1
end	;
go

--teci_end_audit
create procedure dbo.teci_spx_end_audit 
(
	@job_id bigint, 
	@job_status varchar(50) = 'Success'
)
as
begin
	update dbo.teci_job_master
		set 
			active='N',
			end_date = getDate(),
			job_status = @job_status		
		where active='Y' 
		and job_id=@job_id
end;
go

create procedure dbo.teci_spx_write_error
(
	@job_id bigint,
	@error_id bigint,
	@error_number bigint,
	@error_severity bigint,
	@error_state bigint,
	@error_line bigint,
	@error_procedure varchar(100),
	@error_message ntext
)
as
begin
	insert into dbo.teci_job_error(
		job_id,
		error_id,
		error_numeric,
		error_severity,
		error_state,
		error_line,
		error_procedure,
		error_message
		)
	select
		@job_id,
		@error_id,
		@error_number,
		@error_severity,
		@error_state,
		@error_line,
		@error_procedure,
		@error_message
		
end



