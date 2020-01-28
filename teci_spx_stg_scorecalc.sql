--Use DatabaseName; --Replace with your desired database name
use OMAP;

if (object_id('dbo.teci_spx_stg_scorecalc') is not null) drop procedure dbo.teci_spx_stg_scorecalc;
go

-----------------------------------------------------------------------------------------------------------------------------------------------------

-- The script will identify first occurence of Elixhauser comorbidity
-- from the input source for all CDR patients.

--author: Syed et al.
--copyright (C) Dept of Biomedical Informatics, UAMS. 

--Script was built using SQL script provided in the manuscript of 
    --Development and validation of astructured query language implementation of the Elixhauser comorbidity index
	--author: Richard H. Epstein, MD
	--copyright (C) UM Department of Anesthesia
	--Based ON the HCUP SAS Elixhauser Comorbidity Software v3.7
	--https://www.hcup-us.ahrq.gov/toolssoftware/comorbidity/comorbidity.jsp 
-----------------------------------------------------------------------------------------------------------------------------------------------------

CREATE proc dbo.teci_spx_stg_scorecalc
@job_id bigint = null
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

		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: creating #teci_source,#teci_source_icd tables', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1

			IF OBJECT_ID('tempdb..#teci_source') IS NOT NULL
			drop table #teci_source
			CREATE TABLE #teci_source (patient_id nvarchar(100), code nvarchar(100),code_type nvarchar(10), dx_drg_prim_sec nvarchar(50),  first_dx_drg_date datetime)

			IF OBJECT_ID('tempdb..#teci_source_icd') IS NOT NULL
			drop table #teci_source_icd
			CREATE TABLE #teci_source_icd (id bigint identity(1,1),patient_id nvarchar(100), code nvarchar(100),code_type nvarchar(10),comorb_code nvarchar(100), dx_drg_prim_sec nvarchar(50), first_dx_drg_date datetime,BinaryCode bigint)
		
			Truncate TABLE dbo.teci_cdr_pat_comorb;
			Truncate TABLE dbo.teci_cdr_pat_comorb_bincode;

	-- ******************************************************************************************************
	-- identify first comorbidity dates
	-- ******************************************************************************************************
	   EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: identify first comorbidity dates', @@rowcount, @step_ct
	   set @step_ct = @step_ct + 1

		Truncate table  #teci_source;
		Insert into #teci_source
		(
			patient_id,code,code_type,dx_drg_prim_sec,first_dx_drg_date
		)
		select patient_id, code, code_type, dx_drg_prim_sec,first_dx_drg_date
		from dbo.teci_input_source;


		Truncate table  #teci_source_icd;
		insert into #teci_source_icd
		(
			patient_id,
			code,
			code_type,
			comorb_code,
			BinaryCode,
			dx_drg_prim_sec,
			first_dx_drg_date
		)
		SELECT
			a.patient_id,
			a.code,
			a.code_type,
			b.CODE_HCUP,
			c.BinaryCODE,
			a.dx_drg_prim_sec,
			a.first_dx_drg_date
		FROM #teci_source a 
		INNER JOIN dbo.teci_icd_codes b
		ON a.code = b.icd
		INNER JOIN dbo.teci_comorb_codes c
		ON b.CODE_HCUP = c.CODE

	-- ******************************************************************************************************
	-- SET  the comorbodities related to HTN w/ and w/o complications, renal failure, and CHF 
	-- ******************************************************************************************************
	   EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: set comorb for HTN, CHF and Renal Failure', @@rowcount, @step_ct
	   set @step_ct = @step_ct + 1

			INSERT INTO #teci_source_icd
			(
			 patient_id,code,code_type,comorb_code,BinaryCode,dx_drg_prim_sec,first_dx_drg_date
			)
			SELECT
				a.patient_id,
				a.code,
				a.code_type,
				b.CODE_HTN,
				c.BinaryCODE,
				a.dx_drg_prim_sec,
				a.first_dx_drg_date
			FROM #teci_source a 
			INNER JOIN dbo.teci_icd_codes b
			ON a.code = b.icd
			INNER JOIN dbo.teci_comorb_codes c
			ON b.CODE_HTN = c.CODE;

			INSERT INTO #teci_source_icd
			(
			 patient_id,code,code_type,comorb_code,BinaryCode,dx_drg_prim_sec,first_dx_drg_date
			)
			SELECT
				a.patient_id,
				a.code,
				a.code_type,
				b.CODE_HTN_c,
				c.BinaryCODE,
				a.dx_drg_prim_sec,
				a.first_dx_drg_date
			FROM #teci_source a 
			INNER JOIN dbo.teci_icd_codes b
			ON a.code = b.icd
			INNER JOIN dbo.teci_comorb_codes c
			ON b.CODE_HTN_c = c.CODE;

			INSERT INTO #teci_source_icd
			(
			 patient_id,code,code_type,comorb_code,BinaryCode,dx_drg_prim_sec,first_dx_drg_date
			)
			SELECT
				a.patient_id,
				a.code,
				a.code_type,
				b.CODE_RENLFAIL,
				c.BinaryCODE,
				a.dx_drg_prim_sec,
				a.first_dx_drg_date
			FROM #teci_source a 
			INNER JOIN dbo.teci_icd_codes b
			ON a.code = b.icd
			INNER JOIN dbo.teci_comorb_codes c
			ON b.CODE_RENLFAIL = c.CODE;

			INSERT INTO #teci_source_icd
			(
			 patient_id,code,code_type,comorb_code,BinaryCode,dx_drg_prim_sec,first_dx_drg_date
			)
			SELECT
				a.patient_id,
				a.code,
				a.code_type,
				b.CODE_CHF,
				c.BinaryCODE,
				a.dx_drg_prim_sec,
				a.first_dx_drg_date
			FROM #teci_source a 
			INNER JOIN dbo.teci_icd_codes b
			ON a.code = b.icd
			INNER JOIN dbo.teci_comorb_codes c
			ON b.CODE_CHF = c.CODE;

        insert into dbo.teci_cdr_pat_comorb_bincode
		(
		 patient_id,code,code_type,comorb_code,comorb_prim_sec,min_comorb_date,BinaryCode
		)
		Select patient_id,code,code_type,comorb_code,dx_drg_prim_sec,min(first_dx_drg_date)as min_comorb_date,BinaryCode 
		from #teci_source_icd
		group by patient_id,code,code_type,comorb_code,dx_drg_prim_sec,BinaryCode 

	-- ****************************************************************************************************************************
	-- Populate the staging table (teci_cdr_pat_comorb) using #teci_source_icd containing unique comorbidities for each record
	-- ****************************************************************************************************************************
		Truncate table dbo.teci_cdr_pat_comorb
		INSERT INTO  dbo.teci_cdr_pat_comorb
		(
			patient_id,CODE_type,comorb_code,BinaryCODE, comorb_prim_sec,min_comorb_date
		)
		SELECT
			patient_id,CODE_type,comorb_code,BinaryCODE, dx_drg_prim_sec, min(first_dx_drg_date) as min_comorb_date 
		FROM #teci_source_icd
		GROUP BY patient_id,CODE_type,comorb_code,BinaryCODE, dx_drg_prim_sec;

	 EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'End: identify first comorbidity dates', @@rowcount, @step_ct
	 set @step_ct = @step_ct + 1


end try

	BEGIN CATCH
		--End Proc
		EXEC dbo.teci_spx_end_audit @job_id, 'FAIL'
	END CATCH


END

GO


