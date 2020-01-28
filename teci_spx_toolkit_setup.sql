--Use [DatabaseName]; --Replace with your desired database name

if (object_id('dbo.teci_spx_toolkit_setup') is not null) drop procedure dbo.teci_spx_toolkit_setup;
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- The script will create object that TECI toolkit will use to hold
-- Elixhauser comrobidity codes, patient diagnosis/drg records,
-- and comorbidity Index and van Walraven scores.

--author: Syed et al.
--copyright (C) Dept of Biomedical Informatics, UAMS. 


--Script was built using SQL script provided in the manuscript of 
    --Development and validation of astructured query language implementation of the Elixhauser comorbidity index
	--author: Richard H. Epstein, MD
	--copyright (C) UM Department of Anesthesia
	--Based ON the HCUP SAS Elixhauser Comorbidity Software v3.7
	--https://www.hcup-us.ahrq.gov/toolssoftware/comorbidity/comorbidity.jsp 
-----------------------------------------------------------------------------------------------------------------------------------------------------

go
Create procedure [dbo].[teci_spx_toolkit_setup]
@job_id [bigint] = null
as
begin

--Audit variables
DECLARE @new_job_flag INT
DECLARE @database_name VARCHAR(50)
DECLARE @procedure_name VARCHAR(50)
DECLARE @data_source_id	int
DECLARE @step_ct INT

Begin Try
		SELECT @database_name = db_name(), @procedure_name= name FROM sysobjects WHERE id =@@PROCID
		----------------------Audit JOB Initialization--------------------------------
		--If Job ID does not exist, then this is a single procedure run and we need to create it
		IF(@job_id IS NULL  or @job_id < 1)
			BEGIN
					SET @new_job_flag = 1 -- True
					EXEC dbo.teci_spx_start_audit @procedure_name, @database_name,@job_id OUTPUT
			END

		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: creating teci objects', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1

		if (object_id('dbo.teci_icd_codes') is not null) drop table dbo.teci_icd_codes;
			CREATE TABLE dbo.teci_icd_codes
			(
				IDX int IDENTITY(1,1) NOT NULL,
				ICD_TYPE NVARCHAR(100) NOT NULL,
				ICD nvarchar(255) NOT NULL,
				CODE_HCUP nvarchar(16) NULL,
				CODE_HTN nvarchar(16) NULL,
				CODE_HTN_C nvarchar(16) NULL,
				CODE_RENLFAIL nvarchar(16) NULL,
				CODE_CHF nvarchar(16) NULL,
				CODE_HTNPREG nvarchar(16) NULL,
				CODE_HTNWOCHF nvarchar(16) NULL,
				CODE_HTNWCHF nvarchar(16) NULL,
				CODE_HRENWORF nvarchar(16) NULL,
				CODE_HRENWRF nvarchar(16) NULL,
				CODE_HHRWOHRF nvarchar(16) NULL,
				CODE_HHRWCHF nvarchar(16) NULL,
				CODE_HHRWRF nvarchar(16) NULL,
				CODE_HHRWHRF nvarchar(16) NULL,
				CODE_OHTNPREG nvarchar(16) NULL,
			PRIMARY KEY CLUSTERED 
			(
				ICD, ICD_TYPE ASC
			)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
			) ON [PRIMARY]


		if (object_id('dbo.teci_drg_codes') is not null) drop table dbo.teci_drg_codes;
			CREATE TABLE dbo.teci_drg_codes
			(
				idx int IDENTITY(1,1) NOT NULL,
				DRG nvarchar(50) NULL,
				CODE nvarchar(50) NULL,
				VER smallint NULL,
				start_date date,
				end_date date default '2999-01-01'
			) ON [PRIMARY]


		if (object_id('dbo.teci_input_source') is not null) drop table dbo.teci_input_source;
			Create TABLE dbo.teci_input_source
			(
			 id bigint identity(1,1),
			 --input_src_key nvarchar(255) unique not null,
			 code nvarchar(100),
			 code_type nvarchar(10),
			 patient_id nvarchar(100),
			 first_dx_drg_date datetime,
			 dx_drg_prim_sec nvarchar(50)
			)

		----This is the final table, we have to rename this
		if (object_id('dbo.teci_comorb_van_score_op') is not null) drop table dbo.teci_comorb_van_score_op;
			Create TABLE dbo.teci_comorb_van_score_op
			(
			 id bigint identity(1,1),
			 study_name nvarchar(255),
			 encounter_id nvarchar(255),
			 patient_id nvarchar(255),
			 AIDS tinyint,
			 ALCOHOL tinyint,
			 ANEMDEF tinyint,
			 ARTH tinyint,
			 BLDLOSS tinyint,
			 CHF tinyint,
			 CHRNLUNG tinyint,
			 COAG tinyint,
			 DEPRESS tinyint,
			 DM tinyint,
			 DMCX tinyint,
			 DRUG tinyint,
			 HTN_C tinyint,
			 HYPOTHY tinyint,
			 LIVER tinyint,
			 LYMPH tinyint,
			 LYTES tinyint,
			 METS tinyint,
			 NEURO tinyint,
			 OBESE tinyint,
			 PARA tinyint,
			 PERIVASC tinyint,
			 PSYCH tinyint,
			 PULMCIRC tinyint,
			 RENLFAIL tinyint,
			 TUMOR tinyint,
			 ULCER tinyint,
			 VALVE tinyint,
			 WGHTLOSS tinyint,
			 ARRHYTH tinyint,
			 comorbidity_score int,
			 van_index int,
			 run_date datetime default getdate()
			)

		if (object_id('dbo.teci_comorb_codes') is not null) drop table dbo.teci_comorb_codes;
			CREATE TABLE dbo.teci_comorb_codes
			(
				ID  int IDENTITY(1,1) NOT NULL,
				CODE varchar(16) NULL,
				BinaryCODE bigint NULL,
			PRIMARY KEY CLUSTERED 
			(
				ID ASC
			)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
			) ON [PRIMARY]

		if (object_id('dbo.teci_vwindex_weights') is not null) drop table dbo.teci_vwindex_weights;
		CREATE TABLE dbo.teci_vwindex_weights
		(
				comorb_code varchar(16) NOT NULL,
				indx_weight smallint NULL
		)

		--dbo.ExTkit_allpat_comorb=teci_pat_comorb
		IF OBJECT_ID (N'teci_cdr_pat_comorb', N'U') IS NOT NULL 	
		drop table dbo.teci_cdr_pat_comorb
		create TABLE dbo.teci_cdr_pat_comorb 
		(
			id bigint identity(1,1),
			patient_id nvarchar(100),
			code_type nvarchar(10), 
			comorb_code varchar(16), 
			comorb_prim_sec nvarchar(50), 
			min_comorb_date datetime,
			BinaryCode bigint, 
			run_date datetime default getdate()
		)

		--ExTkit_allpat_comorb_wcode=teci_pat_comorb
		IF OBJECT_ID (N'teci_cdr_pat_comorb_bincode', N'U') IS NOT NULL 	
		drop table   dbo.teci_cdr_pat_comorb_bincode
		create TABLE dbo.teci_cdr_pat_comorb_bincode 
		(
			id bigint identity(1,1),
			patient_id nvarchar(100),
			code nvarchar(100),
			code_type nvarchar(10), 
			comorb_code varchar(16), 
			comorb_prim_sec nvarchar(50), 
			min_comorb_date datetime,
			BinaryCode bigint, 
			run_date datetime default getdate()
		)

		IF OBJECT_ID (N'teci_date_specf_dataset', N'U') IS NOT NULL 	
		drop table   dbo.teci_date_specf_dataset
		CREATE TABLE dbo.teci_date_specf_dataset
		(
			encounter_id nvarchar(100) not null,
			patient_id nvarchar(100) not null,
			code nvarchar(100) ,
			code_type nvarchar(50) ,
			diag_prim_sec nvarchar(50),
			admission_date datetime,
			diagnosis_date datetime,
			discharge_date datetime,
			drg nvarchar(100) ,
			elix_drg nvarchar(100),
			study_name nvarchar(500) NOT NULL
		)


		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'End: creating teci objects', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1

		EXEC dbo.teci_spx_load_icd @job_id;
		EXEC dbo.teci_spx_load_drg @job_id;	

		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: loading teci_comorb_codes', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1

		TRUNCATE TABLE dbo.teci_comorb_codes
		INSERT INTO dbo.teci_comorb_codes  select 'AIDS', 1
		INSERT INTO dbo.teci_comorb_codes  select 'ALCOHOL', 2
		INSERT INTO dbo.teci_comorb_codes  select 'ANEMDEF', 4
		INSERT INTO dbo.teci_comorb_codes  select 'ARTH', 8
		INSERT INTO dbo.teci_comorb_codes  select 'BLDLOSS', 16
		INSERT INTO dbo.teci_comorb_codes  select 'CHF', 32
		INSERT INTO dbo.teci_comorb_codes  select 'CHRNLUNG', 64
		INSERT INTO dbo.teci_comorb_codes  select 'COAG', 128
		INSERT INTO dbo.teci_comorb_codes  select 'DEPRESS', 256
		INSERT INTO dbo.teci_comorb_codes  select 'DM', 512
		INSERT INTO dbo.teci_comorb_codes  select 'DMCX', 1024
		INSERT INTO dbo.teci_comorb_codes  select 'DRUG', 2048
		INSERT INTO dbo.teci_comorb_codes  select 'HTN_C', 4096
		INSERT INTO dbo.teci_comorb_codes  select 'HYPOTHY', 8192
		INSERT INTO dbo.teci_comorb_codes  select 'LIVER', 16384
		INSERT INTO dbo.teci_comorb_codes  select 'LYMPH', 32768
		INSERT INTO dbo.teci_comorb_codes  select 'LYTES', 65536
		INSERT INTO dbo.teci_comorb_codes  select 'METS', 131072
		INSERT INTO dbo.teci_comorb_codes  select 'NEURO', 262144
		INSERT INTO dbo.teci_comorb_codes  select 'OBESE', 524288
		INSERT INTO dbo.teci_comorb_codes  select 'PARA', 1048576
		INSERT INTO dbo.teci_comorb_codes  select 'PERIVASC', 2097152
		INSERT INTO dbo.teci_comorb_codes  select 'PSYCH', 4194304
		INSERT INTO dbo.teci_comorb_codes  select 'PULMCIRC', 8388608
		INSERT INTO dbo.teci_comorb_codes  select 'RENLFAIL', 16777216
		INSERT INTO dbo.teci_comorb_codes  select 'TUMOR', 33554432
		INSERT INTO dbo.teci_comorb_codes  select 'ULCER', 67108864
		INSERT INTO dbo.teci_comorb_codes  select 'VALVE', 134217728
		INSERT INTO dbo.teci_comorb_codes  select 'WGHTLOSS', 268435456
		INSERT INTO dbo.teci_comorb_codes  select 'HTN', 536870912
		INSERT INTO dbo.teci_comorb_codes  select 'HTN_COMP', 1073741824
		INSERT INTO dbo.teci_comorb_codes  select 'ARRYTH', 2147483648
		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'End: loading teci_comorb_codes', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1

		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: loading teci_vwindex_weights', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1
		--INSERT INTO #BuildLog SELECT  GetDate(), 'POPULATED: dbo.teci_comorb_codes'
		TRUNCATE TABLE dbo.teci_vwindex_weights 
		INSERT INTO dbo.teci_vwindex_weights   select 'AIDS', 0
		INSERT INTO dbo.teci_vwindex_weights  select 'ALCOHOL', 0
		INSERT INTO dbo.teci_vwindex_weights  select 'ANEMDEF', -2
		INSERT INTO dbo.teci_vwindex_weights  select 'ARRYTH', 5
		INSERT INTO dbo.teci_vwindex_weights  select 'ARTH', 0
		INSERT INTO dbo.teci_vwindex_weights  select 'BLDLOSS', -2
		INSERT INTO dbo.teci_vwindex_weights  select 'CHF', 7
		INSERT INTO dbo.teci_vwindex_weights  select 'CHRNLUNG', 3
		INSERT INTO dbo.teci_vwindex_weights  select 'COAG', 3
		INSERT INTO dbo.teci_vwindex_weights  select 'DEPRESS', -3
		INSERT INTO dbo.teci_vwindex_weights  select 'DM', 0
		INSERT INTO dbo.teci_vwindex_weights  select 'DMCX', 0
		INSERT INTO dbo.teci_vwindex_weights  select 'DRUG', -7
		INSERT INTO dbo.teci_vwindex_weights  select 'HTN_C', 0
		INSERT INTO dbo.teci_vwindex_weights  select 'HYPOTHY', 0
		INSERT INTO dbo.teci_vwindex_weights  select 'LIVER', 11
		INSERT INTO dbo.teci_vwindex_weights  select 'LYMPH', 9
		INSERT INTO dbo.teci_vwindex_weights  select 'LYTES', 5
		INSERT INTO dbo.teci_vwindex_weights  select 'METS', 12
		INSERT INTO dbo.teci_vwindex_weights  select 'NEURO', 6
		INSERT INTO dbo.teci_vwindex_weights  select 'OBESE', -4
		INSERT INTO dbo.teci_vwindex_weights  select 'PARA', 7
		INSERT INTO dbo.teci_vwindex_weights  select 'PERIVASC', 2
		INSERT INTO dbo.teci_vwindex_weights  select 'PSYCH', 0
		INSERT INTO dbo.teci_vwindex_weights  select 'PULMCIRC', 4
		INSERT INTO dbo.teci_vwindex_weights  select 'RENLFAIL', 5
		INSERT INTO dbo.teci_vwindex_weights  select 'TUMOR', 4
		INSERT INTO dbo.teci_vwindex_weights  select 'ULCER', 0
		INSERT INTO dbo.teci_vwindex_weights  select 'VALVE', -1
		INSERT INTO dbo.teci_vwindex_weights  select 'WGHTLOSS', 6;

		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'End: loading teci_vwindex_weights', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1


End try

BEGIN CATCH
END CATCH

END

GO


