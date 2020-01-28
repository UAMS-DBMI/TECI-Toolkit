--Use DatabaseName; --Replace with your desired database name
use OMAP;

if (object_id('dbo.teci_spx_comrb_index_score') is not null) drop procedure dbo.teci_spx_comrb_index_score;
go
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- The script will calculate vw score and comorbidity index based on the first 
-- occurence of Elixhauser comorbidity computed by SP teci_spx_stg_scorecalc.
-- SP needs source table name, study end date as input parameter.
-- Can run the SP to exclude primary DX.
-- Can run the SP to calcuate score on admissions or on patient level.

--author: Syed et al.
--copyright (C) Dept of Biomedical Informatics, UAMS. 

--Script was built using SQL script provided in the manuscript of 
    --Development and validation of astructured query language implementation of the Elixhauser comorbidity index
	--author: Richard H. Epstein, MD
	--copyright (C) UM Department of Anesthesia
	--Based ON the HCUP SAS Elixhauser Comorbidity Software v3.7
	--https://www.hcup-us.ahrq.gov/toolssoftware/comorbidity/comorbidity.jsp 
-----------------------------------------------------------------------------------------------------------------------------------------------------


CREATE PROCEDURE [dbo].[teci_spx_comrb_index_score]
	@input_table_name nvarchar(255),
	@study_end_date date,
	@inlcude_prim smallint,						    -- 1 = include both primary and secondary dx, else includes only secondary dx.  
	@cal_score_by_admission smallint,                 -- 1 = calculate comrobidity per admission/encoutner, else calculate comrobidity per patient
	@ARRHYTH_Include tinyint = 0,						-- 1 = include the ARRHYTH DRG, 0 = exclude
	@job_id bigint = null
AS
Begin
--Audit variables
DECLARE @new_job_flag INT
DECLARE @database_name VARCHAR(50)
DECLARE @procedure_name VARCHAR(50)
DECLARE @data_source_id	int
DECLARE @step_ct INT 
Declare @prim_sec_dx nvarchar(255) 
Declare @drg_rank_no INT 

Begin Try

		SELECT @database_name = db_name(), @procedure_name= name FROM sysobjects WHERE id =@@PROCID
		----------------------Audit JOB Initialization--------------------------------
		--If Job ID does not exist, then this is a single procedure run and we need to create it
		IF(@job_id IS NULL  or @job_id < 1)
			BEGIN
					SET @new_job_flag = 1 -- True
					EXEC dbo.teci_spx_start_audit @procedure_name, @database_name,@job_id OUTPUT
		    END

        EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: creating teci_comrb_index_score tables', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1

		IF OBJECT_ID('tempdb..#input_dataset') IS NOT NULL
		drop table #input_dataset;
	 
		create table #input_dataset
		(
			 id bigint identity(1,1),
			 encounter_id nvarchar(100),
			 patient_id nvarchar(100),
			 code_type  nvarchar(100),
			 code nvarchar(100),
			 comorb_prim_sec nvarchar(100), 
			 code_assigned_date datetime,
			 admission_date datetime,
			 discharge_date datetime,
			 drg  nvarchar(100),
			 elix_drg nvarchar(100),
			 study_name nvarchar(255)
		);

		IF OBJECT_ID('tempdb..#input_dataset_mult_drg') IS NOT NULL
		drop table #input_dataset_mult_drg;
	 
		create table #input_dataset_mult_drg
		(
			 id bigint,
			 encounter_id nvarchar(100),
			 patient_id nvarchar(100),
			 code_type  nvarchar(100),
			 code nvarchar(100),
			 comorb_prim_sec nvarchar(100), 
			 code_assigned_date datetime,
			 admission_date datetime,
			 discharge_date datetime,
			 drg  nvarchar(100),
			 elix_drg nvarchar(100),
			 rank_no int,
			 study_name nvarchar(255)
		);

		IF OBJECT_ID('tempdb..#patient_comrb_with_dates') IS NOT NULL
		drop table #patient_comrb_with_dates 
		Create table #patient_comrb_with_dates
		(
			 id bigint identity(1,1),
			 encounter_id nvarchar(100),
			 patient_id nvarchar(100),
			 comorb_code nvarchar(100),
			 code_type nvarchar(100),
			 first_comorb_date datetime,
			 comorb_flag tinyint default 0,
			 binaryCode bigint,
			 sum_binaryCode bigint,
			 comorb_prim_sec nvarchar(100),
			 drg_reject_yn nvarchar(1) default 'N',
			 update_date datetime default getdate()
		);

		IF (object_id('tempdb..#patient_comrb_op') is not null) 
		drop table #patient_comrb_op;
		Create TABLE #patient_comrb_op
				(
				 id bigint identity(1,1),
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
				 run_date datetime default getdate()
				);
		
		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: set primary and secondary DX flag ', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1

		if @inlcude_prim = 1 
		Begin
			 set @prim_sec_dx = '(''primary'',''secondary'')'
		END
		Else
		Begin
			 set @prim_sec_dx = '(''secondary'')'
		END
	   --print @prim_sec_dx

	   
        EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: loading #input_dataset', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1
		-- ********************************************************************************************
	    --Start loading input dataset into temp table
	    -- ********************************************************************************************
	    TRUNCATE TABLE #input_dataset
		EXEC (
		   'INSERT INTO #input_dataset
			( encounter_id,patient_id,code_type,code,comorb_prim_sec,code_assigned_date,
			admission_date,discharge_date,drg,study_name)
			SELECT 
			case 
			when ' + @cal_score_by_admission + ' = 1 then encounter_id else ''1'' end as  encounter_id,
				patient_id,
				code_type,
				code,
				diag_prim_sec as comorb_prim_sec,
				diagnosis_date as code_assigned_date,
				admission_date,
				discharge_date,
				drg,
				study_name
			FROM dbo.' + @input_table_name + 
		  ' WHERE code is not null and code <> '''' and diag_prim_sec in ' + @prim_sec_dx  
		  +	' and cast(discharge_date as date) <= ''' + @study_end_date + ''''
		);

		--Select * 
		--into dbo.fn_test_input_dataset
		--From #input_dataset

		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: Mapping source DRG to Elix DRGs', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1

		 insert into #input_dataset_mult_drg
		 (id, encounter_id,patient_id,code_type,code,comorb_prim_sec,code_assigned_date,
		 admission_date,discharge_date,drg,study_name,elix_drg, rank_no)
		 Select distinct id, encounter_id,patient_id,code_type,code,comorb_prim_sec,code_assigned_date,
		 admission_date,discharge_date,drg,study_name,multi_drg as elix_drg, rank_no
		 from 
         (
			Select a.*,b.code as multi_drg, row_number() over(partition by a.id order by b.code) as rank_no
			--set a.elix_drg = b.code
			from #input_dataset a
			join dbo.teci_drg_codes b
			on cast(a.drg as int) = cast(b.drg as int)
			and cast(coalesce(a.discharge_date,a.admission_date) as date) between b.start_date and b.end_date
		 )
		 x
		 order by id;

		update a 
		set a.elix_drg = b.elix_drg
		from #input_dataset a
		join #input_dataset_mult_drg b
		on a.id = b.id
		and b.rank_no = 1;

		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: inserting into #patient_comrb_with_dates', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1

		insert into #patient_comrb_with_dates
		(encounter_id,patient_id,code_type,comorb_code,comorb_flag,binaryCode,comorb_prim_sec,first_comorb_date)
		SELECT
			encounter_id,patient_id,CODE_type,comorb_code,1 as comorb_flag, BinaryCODE,max(comorb_prim_sec) comorb_prim_sec
			, min(min_comorb_date) as first_comorb_date -- change this to first comrob_date
		FROM
			 (
				 Select distinct a.encounter_id,c.*
				 FROM #input_dataset a
				 join dbo.teci_icd_codes b
				 on a.code = b.icd
				 and b.icd_type like a.code_type + '%'
				 join dbo.teci_cdr_pat_comorb c
				 on  a.patient_id =c.patient_id
				 and a.admission_date>= c.min_comorb_date
				 and b.code_hcup = c.comorb_code
				 and c.code_type like 'ICD%'
				 --order by patient_id
			 )
			src
		GROUP BY 
		encounter_id,patient_id,CODE_type,comorb_code,BinaryCODE

		--when calculating comrobidity on patient level we need prior comorbidities that are not available in specific dataset
		if @cal_score_by_admission= 0
			BEGIN
			    insert into #patient_comrb_with_dates
				(encounter_id,patient_id,code_type,comorb_code,comorb_flag,binaryCode,comorb_prim_sec,first_comorb_date)
				SELECT
						encounter_id,patient_id,CODE_type,comorb_code,1 as comorb_flag, BinaryCODE,max(comorb_prim_sec) comorb_prim_sec
						, min(min_comorb_date) as first_comorb_date 
				FROM
				(
					 Select 1 as encounter_id, a.*
					 from dbo.teci_cdr_pat_comorb a
					 join #input_dataset b
					 on a.patient_id = b.patient_id
					 where a.min_comorb_date<=@study_end_date
				 )
				 src
				GROUP BY 
				encounter_id,patient_id,CODE_type,comorb_code,BinaryCODE
			END
		-- ******************************************************************************************************
		-- SET  the comorbodities related to HTN w/ and w/o complications, renal failure, and CHF 
		-- from the secondary diagnosis ICD codes. 
		-- ******************************************************************************************************
		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: comorbs related to HTN w/woComplication,RF,CHF', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1

		if @cal_score_by_admission= 1
			BEGIN
			IF OBJECT_ID('tempdb..#tmp_merge_records') IS NOT NULL
			drop table #tmp_merge_records;
			Select *
			into #tmp_merge_records
			from 
			(
				Select d.encounter_id, d.patient_id, b.CODE_HTN as comorb_code, d.code_type, d.first_comorb_date, 
				d.comorb_flag,c.binaryCode, d.sum_binaryCode, d.comorb_prim_sec, d.drg_reject_yn
				from #input_dataset a
				join  dbo.teci_icd_codes b
				on b.icd= a.code
				join dbo.teci_comorb_codes c
				on c.code = b.CODE_HTN
				join  #patient_comrb_with_dates d
				on b.code_HCUP= d.comorb_code
				and a.patient_id = d.patient_id
				and a.encounter_id = d.encounter_id
				union
				Select d.encounter_id, d.patient_id, b.CODE_HTN_C as comorb_code, d.code_type, d.first_comorb_date, 
				d.comorb_flag,c.binaryCode, d.sum_binaryCode, d.comorb_prim_sec, d.drg_reject_yn
				from #input_dataset a
				join  dbo.teci_icd_codes b
				on b.icd= a.code
				join dbo.teci_comorb_codes c
				on c.code = b.CODE_HTN_C
				join  #patient_comrb_with_dates d
				on b.code_HCUP= d.comorb_code
				and a.patient_id = d.patient_id
				and a.encounter_id = d.encounter_id
				union
				Select d.encounter_id, d.patient_id, b.CODE_RENLFAIL as comorb_code, d.code_type, d.first_comorb_date, 
				d.comorb_flag,c.binaryCode, d.sum_binaryCode, d.comorb_prim_sec, d.drg_reject_yn
				from #input_dataset a
				join  dbo.teci_icd_codes b
				on b.icd= a.code
				join dbo.teci_comorb_codes c
				on c.code = b.CODE_RENLFAIL
				join  #patient_comrb_with_dates d
				on b.code_HCUP= d.comorb_code
				and a.patient_id = d.patient_id
				and a.encounter_id = d.encounter_id
				union
				Select d.encounter_id, d.patient_id, b.CODE_CHF as comorb_code, d.code_type, d.first_comorb_date, 
				d.comorb_flag,c.binaryCode, d.sum_binaryCode, d.comorb_prim_sec, d.drg_reject_yn
				from #input_dataset a
				join  dbo.teci_icd_codes b
				on b.icd= a.code
				join dbo.teci_comorb_codes c
				on c.code = b.CODE_CHF
				join  #patient_comrb_with_dates d
				on b.code_HCUP= d.comorb_code
				and a.patient_id = d.patient_id
				and a.encounter_id = d.encounter_id
			)
			xx
			option(hash join) ;

			merge into #patient_comrb_with_dates a
			using (Select * from #tmp_merge_records)new
			on (a.patient_id = new.patient_id and a.encounter_id = new.encounter_id and a.comorb_code = new.comorb_code)
			WHEN NOT MATCHED THEN INSERT
			(encounter_id,patient_id,comorb_code,code_type,first_comorb_date,comorb_flag,binaryCode,sum_binaryCode,comorb_prim_sec,drg_reject_yn)
			values
			(encounter_id,patient_id,comorb_code,code_type,first_comorb_date,comorb_flag,binaryCode,sum_binaryCode,comorb_prim_sec,drg_reject_yn);
           End

		--Select * 
		--into dbo.fn_test_patient_comrb_with_dates2
		--From #patient_comrb_with_dates

        -- ******************************************************************************************************
		-- add sum of the binary codes associated with each comorbidity, then add DRG for each record
		-- ******************************************************************************************************
		update a
		set a.sum_binarycode = b.sum_binarycode
		from #patient_comrb_with_dates a
		join 
		(
			Select encounter_id,patient_id, sum(binarycode) as sum_binarycode 
			from  #patient_comrb_with_dates
			group by encounter_id, patient_id
		)
		b
		on a.encounter_id = b.encounter_id
		and  a.patient_id = b.patient_id

		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: update hypertensive flags', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1
		-- ******************************************************************************************************
		-- Check if in the current dataset patient has below flags
		-- Update the specific HTN codes
		-- HTNWOCHF		= Hypertensive heart disease without heart failure  
		-- HTNWCHF		= Hypertensive heart disease with heart failure
		-- HRENWORF		= Hypertensive renal disease without renal failure
		-- HRENWRF		= Hypertensive renal disease with renal failure  
		-- HHRWOHRF		= Hypertensive heart and renal disease without heart or renal failure
		-- HHRWCHF		= Hypertensive heart and renal disease with heart failure
		-- HHRWRF		= Hypertensive heart and renal disease with renal failure
		-- HHRWHRF		" Hypertensive heart and renal disease with heart and renal failure
		-- OHTNPREG		= Other hypertension in pregnancy
		--
		-- ******************************************************************************************************
		if @cal_score_by_admission= 1
			BEGIN
			Insert into #patient_comrb_with_dates
			(patient_id,code_type,comorb_code, comorb_flag, a.comorb_prim_sec,encounter_id,first_comorb_date,sum_binaryCode)
			select a.patient_id, a.code_type, 'HTNPREG' as comorb_code, comorb_flag, a.comorb_prim_sec,a.encounter_id,first_comorb_date,sum_binaryCode
			from #patient_comrb_with_dates a
			inner join #input_dataset b
			on a.encounter_id = b.encounter_id  
			and a.patient_id = b.patient_id
			inner join dbo.teci_icd_codes c
			on b.code = c.icd
			and c.icd_type like b.code_type + '%'
			and c.CODE_HTNPREG = 'HTNPREG'
			union
			select a.patient_id, a.code_type, 'HTNWOCHF' as comorb_code, comorb_flag, a.comorb_prim_sec,a.encounter_id,first_comorb_date,sum_binaryCode
			from #patient_comrb_with_dates a
			inner join #input_dataset b
			on a.encounter_id = b.encounter_id  
			and a.patient_id = b.patient_id
			inner join dbo.teci_icd_codes c
			on b.code = c.icd
			and c.icd_type like b.code_type + '%'
			and c.CODE_HTNWOCHF = 'HTNWOCHF'
			union
			select a.patient_id, a.code_type, 'HTNWCHF' as comorb_code, comorb_flag, a.comorb_prim_sec,a.encounter_id,first_comorb_date,sum_binaryCode
			from #patient_comrb_with_dates a
			inner join #input_dataset b
			on a.encounter_id = b.encounter_id  
			and a.patient_id = b.patient_id
			inner join dbo.teci_icd_codes c
			on b.code = c.icd
			and c.icd_type like b.code_type + '%'
			and c.CODE_HTNWCHF = 'HTNWCHF'
			union
			select a.patient_id, a.code_type, 'HRENWORF' as comorb_code, comorb_flag, a.comorb_prim_sec,a.encounter_id,first_comorb_date,sum_binaryCode
			from #patient_comrb_with_dates a
			inner join #input_dataset b
			on a.encounter_id = b.encounter_id  
			and a.patient_id = b.patient_id
			inner join dbo.teci_icd_codes c
			on b.code = c.icd
			and c.icd_type like b.code_type + '%'
			and c.CODE_HRENWORF = 'HRENWORF'
			union
			select a.patient_id, a.code_type, 'HRENWRF' as comorb_code, comorb_flag, a.comorb_prim_sec,a.encounter_id,first_comorb_date,sum_binaryCode
			from #patient_comrb_with_dates a
			inner join #input_dataset b
			on a.encounter_id = b.encounter_id  
			and a.patient_id = b.patient_id
			inner join dbo.teci_icd_codes c
			on b.code = c.icd
			and c.icd_type like b.code_type + '%'
			and c.CODE_HRENWRF = 'HRENWRF'
			union
			select a.patient_id, a.code_type, 'HHRWOHRF' as comorb_code, comorb_flag, a.comorb_prim_sec,a.encounter_id,first_comorb_date,sum_binaryCode
			from #patient_comrb_with_dates a
			inner join #input_dataset b
			on a.encounter_id = b.encounter_id  
			and a.patient_id = b.patient_id
			inner join dbo.teci_icd_codes c
			on b.code = c.icd
			and c.icd_type like b.code_type + '%'
			and c.CODE_HHRWOHRF = 'HHRWOHRF'
			union
			select a.patient_id, a.code_type, 'HHRWCHF' as comorb_code, comorb_flag, a.comorb_prim_sec,a.encounter_id,first_comorb_date,sum_binaryCode
			from #patient_comrb_with_dates a
			inner join #input_dataset b
			on a.encounter_id = b.encounter_id 
			and a.patient_id = b.patient_id
			inner join dbo.teci_icd_codes c
			on b.code = c.icd
			and c.icd_type like b.code_type + '%'
			and c.CODE_HHRWCHF = 'HHRWCHF'
			union
			select a.patient_id, a.code_type, 'HHRWRF' as comorb_code, comorb_flag, a.comorb_prim_sec,a.encounter_id,first_comorb_date,sum_binaryCode
			from #patient_comrb_with_dates a
			inner join #input_dataset b
			on a.encounter_id = b.encounter_id  
			and a.patient_id = b.patient_id
			inner join dbo.teci_icd_codes c
			on b.code = c.icd
			and c.icd_type like b.code_type + '%'
			and c.CODE_HHRWRF = 'HHRWRF'	
			union
			select a.patient_id, a.code_type, 'HHRWHRF' as comorb_code, comorb_flag, a.comorb_prim_sec,a.encounter_id,first_comorb_date,sum_binaryCode
			from #patient_comrb_with_dates a
			inner join #input_dataset b
			on a.encounter_id = b.encounter_id  
			and a.patient_id = b.patient_id
			inner join dbo.teci_icd_codes c
			on b.code = c.icd
			and c.icd_type like b.code_type + '%'
			and c.CODE_HHRWHRF = 'HHRWHRF'
			union
			select a.patient_id, a.code_type, 'OHTNPREG' as comorb_code, comorb_flag, a.comorb_prim_sec,a.encounter_id,first_comorb_date,sum_binaryCode
			from #patient_comrb_with_dates a
			inner join #input_dataset b
			on a.encounter_id = b.encounter_id  
			and a.patient_id = b.patient_id
			inner join dbo.teci_icd_codes c
			on b.code = c.icd
			and c.icd_type like b.code_type + '%'
			and c.CODE_OHTNPREG = 'OHTNPREG'
			End
		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: remove comorbidites without complications', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1
		-- ******************************************************************************************************
		--  Remove comorbidites without complications WHEN the same comorbidity with compliations is also present
		--  for HTN/HTN_COMP, DM/DMCX, and TUMOR/METS
		-- ******************************************************************************************************
		if @cal_score_by_admission= 1
			BEGIN
			update a
			set a.comorb_flag =0
			from #patient_comrb_with_dates a
			join 
			(
				Select encounter_id,patient_id,count(*) row_count
				from #patient_comrb_with_dates
				where comorb_code in ('HTN','HTN_COMP')
				and comorb_flag=1
				group by encounter_id, patient_id
			)
			b
			on a.encounter_id = b.encounter_id
			and a.patient_id = b.patient_id
			and b.row_count>1
			and a.comorb_code = 'HTN'

			update a
			set a.comorb_flag =0
			from #patient_comrb_with_dates a
			join 
			(
				Select encounter_id,patient_id,count(*) row_count
				from #patient_comrb_with_dates
				where comorb_code in ('DM','DMCX')
				and comorb_flag=1
				group by encounter_id,patient_id
			)
			b
			on a.encounter_id = b.encounter_id
			and a.patient_id = b.patient_id
			and b.row_count>1
			and a.comorb_code = 'DM'

			update a
			set a.comorb_flag =0
			from #patient_comrb_with_dates a
			join 
			(
				Select encounter_id,patient_id,count(*) row_count
				from #patient_comrb_with_dates
				where comorb_code in ('TUMOR','METS')
				and comorb_flag=1
				group by encounter_id,patient_id
			)
			b
			on a.encounter_id = b.encounter_id
			and a.patient_id = b.patient_id
			and b.row_count>1
			and a.comorb_code = 'TUMOR'
			End
		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: remove comorbidites related to DRGs', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1
		-- ******************************************************************************************************
		--  Remove comorbidites realted to the DRG.
		--  This is based on the logic in the HCUP SAS Elixhauser code v3.7, noted in the /* */ sections
		--  xxxFLG in this code is equivalent to xxxDRG in the SQL code
		-- ******************************************************************************************************
			if @cal_score_by_admission= 1
			BEGIN
			  
			  --Cursor to  handle same DRGs that are tied to multiple comorbidities. 
			  Declare multi_drg cursor for
			  Select distinct rank_no
			  From #input_dataset_mult_drg

			  OPEN multi_drg
			  FETCH NEXT FROM multi_drg
			  INTO @drg_rank_no
			  WHILE @@FETCH_STATUS = 0
			  BEGIN

			
				   --IF CARDFLG THEN CHF  0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'CARDDRG'
				   and a.comorb_code = 'CHF'
				   and b.rank_no=@drg_rank_no
				   

				   --IF VALVE AND CARDFLG  THEN  VALVE = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'CARDDRG'
				   and a.comorb_code = 'VALVE'
				   and b.rank_no=@drg_rank_no
				   
				   --IF PULMCIRC AND ( CARDFLG OR PULMFLG ) THEN PULMCIRC = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg in ('CARDDRG','PULMDRG')
				   and a.comorb_code = 'PULMCIRC'
				   and b.rank_no=@drg_rank_no
				   
				   --IF PERIVASC AND PERIFLG THEN PERIVASC = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'PERIDRG'
				   and a.comorb_code = 'PERIVASC'
				   and b.rank_no=@drg_rank_no
				   
				   -- IF HTN AND HTNFLG THEN HTN = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'HTNDRG'
				   and a.comorb_code = 'HTN'
				   and b.rank_no=@drg_rank_no
				   
				   -- IF HTNCX AND HTNCXFLG THEN HTNCX = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'HTNCXDRG'
				   and a.comorb_code = 'HTN_COMP'
				   and b.rank_no=@drg_rank_no
				   
				   --IF HTNPREG_  AND HTNCXFLG THEN HTNCX = 0;
				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg = 'HTNCXDRG'
					   and a.comorb_code = 'HTN_COMP'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg = 'HTNCXDRG'
					   and a.comorb_code = 'HTNPREG'
					   and b.rank_no=@drg_rank_no
					   
				   )
				   and x.comorb_code = 'HTN_COMP'

				   -- IF HTNWOCHF_ AND (HTNCXFLG OR CARDFLG) THEN HTNCX = 0;
				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'HTNCXDRG', 'CARDDRG')
					   and a.comorb_code = 'HTN_COMP'
					   and b.rank_no=@drg_rank_no
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'HTNCXDRG', 'CARDDRG')
					   and a.comorb_code = 'HTNWOCHF'
					   and b.rank_no=@drg_rank_no
					   
				   )
				    and x.comorb_code = 'HTN_COMP'
					option(hash join)

				   -- IF HTNWCHF_  THEN DO; IF HTNCXFLG THEN HTNCX  = 0; IF CARDFLG THEN DO; HTNCX = 0; CHF=-0; END; END;
				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'HTNCXDRG')
					   and a.comorb_code = 'HTNWCHF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'HTNCXDRG')
					   and a.comorb_code = 'HTN_COMP'
					   and b.rank_no=@drg_rank_no
					   
				   )
				   and x.comorb_code = 'HTN_COMP'
				   option(hash join)


				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'CARDDRG')
					   and a.comorb_code = 'HTNWCHF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'CARDDRG')
					   and a.comorb_code in ('HTN_COMP','CHF')
					   and b.rank_no=@drg_rank_no
					   

				   )
				   and x.comorb_code in ('HTN_COMP', 'CHF')
				   option(hash join)

				   --IF HRENWORF_ AND (HTNCXFLG OR RENALFLG) THEN HTNCX = 0;
				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'HTNCXDRG', 'RENALDRG')
					   and a.comorb_code = 'HRENWORF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'HTNCXDRG', 'RENALDRG')
					   and a.comorb_code = 'HTN_COMP'
					   and b.rank_no=@drg_rank_no
					   
				   )
				   and x.comorb_code in ('HTN_COMP')
				   option(hash join);


				   --IF HRENWRF_  THEN DO; IF HTNCXFLG THEN HTNCX = 0;  IF RENALFLG THEN DO; HTNCX    = 0;	 RENLFAIL = 0;  END;  END;
				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'HTNCXDRG')
					   and a.comorb_code = 'HRENWRF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'HTNCXDRG')
					   and a.comorb_code = 'HTN_COMP'
					   and b.rank_no=@drg_rank_no
					   
				   )
				   and x.comorb_code in ('HTN_COMP')
				   option(hash join);

				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'RENALDRG')
					   and a.comorb_code = 'HRENWRF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'RENALDRG')
					   and a.comorb_code in ('HTN_COMP','RENLFAIL')
					   and b.rank_no=@drg_rank_no
					   
					) 
					and x.comorb_code in ('HTN_COMP','RENLFAIL')
					option(hash join);


					--IF HHRWOHRF_ AND (HTNCXFLG OR CARDFLG OR RENALFLG) THEN HTNCX = 0;   
				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in (  'HTNCXDRG', 'CARDDRG', 'RENALDRG')
					   and a.comorb_code = 'HHRWOHRF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'HTNCXDRG', 'CARDDRG', 'RENALDRG')
					   and a.comorb_code = 'HTN_COMP'
					   and b.rank_no=@drg_rank_no
					   
					) 
					and x.comorb_code in ('HTN_COMP')
					option(hash join);
					--IF HHRWCHF_ THEN DO; IF HTNCXFLG THEN HTNCX = 0;  IF CARDFLG THEN DO;	HTNCX = 0;CHF   = 0; END;IF RENALFLG THEN HTNCX = 0; END;
				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in (  'HTNCXDRG')
					   and a.comorb_code = 'HHRWCHF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'HTNCXDRG')
					   and a.comorb_code = 'HTN_COMP'
					   and b.rank_no=@drg_rank_no
					   
					) 
					and x.comorb_code in ('HTN_COMP')
					option(hash join);

				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in (  'CARDDRG')
					   and a.comorb_code = 'HHRWCHF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'CARDDRG')
					   and a.comorb_code in ('HTN_COMP','CHF')
					   and b.rank_no=@drg_rank_no
					   
					) 
					and x.comorb_code in ('HTN_COMP', 'CHF')
					option(hash join);

				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in (  'RENALDRG')
					   and a.comorb_code = 'HHRWCHF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ( 'RENALDRG')
					   and a.comorb_code ='HTN_COMP'
					   and b.rank_no=@drg_rank_no
					   
					) 
					and x.comorb_code in ('HTN_COMP')
					option(hash join);

					-- IF HHRWRF_ THEN DO;IF HTNCXFLG OR CARDFLG THEN HTNCX = 0;IF RENALFLG THEN DO;HTNCX    = 0; RENLFAIL = 0;  END;  END;
					update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in (   'HTNCXDRG', 'CARDDRG')
					   and a.comorb_code = 'HHRWRF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in (  'HTNCXDRG', 'CARDDRG')
					   and a.comorb_code ='HTN_COMP'
					   and b.rank_no=@drg_rank_no
					   
					) 
					and x.comorb_code in ('HTN_COMP')
					option(hash join);

				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ('RenalDRG')
					   and a.comorb_code = 'HHRWRF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in (  'RenalDRG')
					   and a.comorb_code in ('HTN_COMP', 'RENLFAIL')
					   and b.rank_no=@drg_rank_no
					   
					)
                    and x.comorb_code in ('HTN_COMP', 'RENLFAIL')
					option(hash join);


				  --IF HHRWHRF_ THEN DO; IF HTNCXFLG THEN HTNCX = 0; IF CARDFLG THEN DO; HTNCX = 0; CHF   = 0;  END; IF RENALFLG THEN DO;
				  --HTNCX    = 0;  END;   END;
				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in (   'HTNCXDRG')
					   and a.comorb_code = 'HHRWRF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in (  'HTNCXDRG')
					   and a.comorb_code ='HTN_COMP'
					   and b.rank_no=@drg_rank_no
					   
					)
					and x.comorb_code in ('HTN_COMP')
					option(hash join);

				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in (   'CARDDRG')
					   and a.comorb_code = 'HHRWHRF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in (  'CARDDRG')
					   and a.comorb_code in ('HTN_COMP', 'CHF')
					   and b.rank_no=@drg_rank_no
					   
					)
					and x.comorb_code in ('HTN_COMP', 'CHF')
					option(hash join);

				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ('RenalDRG')
					   and a.comorb_code = 'HHRWHRF'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ('RenalDRG')
					   and a.comorb_code in ('HTN_COMP', 'RENLFAIL')
					   and b.rank_no=@drg_rank_no
					   
					)
					and x.comorb_code in ('HTN_COMP', 'RENLFAIL')
					option(hash join);

					--IF OHTNPREG_ AND (HTNCXFLG OR CARDFLG OR RENALFLG) THEN HTNCX = 0;
				   update x
				   set x.drg_reject_yn = 'Y'
				   from #patient_comrb_with_dates x
				   where x.encounter_id in
				   (
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ('HTNCXDRG', 'CARDDRG', 'RENALDRG')
					   and a.comorb_code = 'OHTNPREG'
					   and b.rank_no=@drg_rank_no
					   
					   intersect
					   Select distinct a.encounter_id
					   From #patient_comrb_with_dates a
					   join #input_dataset_mult_drg b
					   on a.encounter_id = b.encounter_id
					   and a.patient_id = b.patient_id
					   and b.elix_drg in ('HTNCXDRG', 'CARDDRG', 'RENALDRG')
					   and a.comorb_code = 'HTN_COMP'
					   and b.rank_no=@drg_rank_no
					   
					)
					and x.comorb_code in ('HTN_COMP')
					option(hash join);

				   --IF NEURO AND NERVFLG THEN NEURO = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'NERVDRG'
				   and a.comorb_code = 'NEURO'
				   and b.rank_no=@drg_rank_no
				   
				   
				   --IF NEURO AND NERVFLG THEN NEURO = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'PULMDRG'
				   and a.comorb_code = 'CHRNLUNG'
				   and b.rank_no=@drg_rank_no
				   
				   --IF DM AND DIABFLG THEN DM = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'DIABDRG'
				   and a.comorb_code = 'DM'
				   and b.rank_no=@drg_rank_no
				   
				   --IF DMCX AND DIABFLG THEN DMCX = 0
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'DIABDRG'
				   and a.comorb_code = 'DMCX'
				   and b.rank_no=@drg_rank_no
				   
				   --IF HYPOTHY AND HYPOFLG THEN HYPOTHY = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'HYPODRG'
				   and a.comorb_code = 'HYPOTHY'
				   and b.rank_no=@drg_rank_no
				   
				   --IF RENLFAIL AND RENFFLG THEN   RENLFAIL = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'RENFDRG'
				   and a.comorb_code = 'RENLFAIL'
				   and b.rank_no=@drg_rank_no
				   
				   --IF LIVER AND LIVERFLG THEN LIVER = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'LIVERDRG'
				   and a.comorb_code = 'LIVER'
				   and b.rank_no=@drg_rank_no
				   
				   --IF ULCER AND ULCEFLG THEN  ULCER = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'ULCEDRG'
				   and a.comorb_code = 'ULCER'
				   and b.rank_no=@drg_rank_no
				   -- IF AIDS AND HIVFLG THEN AIDS = 0
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'HIVDRG'
				   and a.comorb_code = 'AIDS'
				   and b.rank_no=@drg_rank_no
				   --  IF LYMPH AND LEUKFLG THEN LYMPH = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'LEUKDRG'
				   and a.comorb_code = 'LYMPH'
				   and b.rank_no=@drg_rank_no
				   --  IF METS AND CANCFLG THEN METS = 0
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'CANCDRG'
				   and a.comorb_code = 'METS'
				   and b.rank_no=@drg_rank_no
				   --  IF TUMOR AND CANCFLG THEN TUMOR = 0
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'CANCDRG'
				   and a.comorb_code = 'TUMOR'
				   and b.rank_no=@drg_rank_no
				   -- IF ARTH AND ARTHFLG THEN ARTH = 0
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'ARTHDRG'
				   and a.comorb_code = 'ARTH'
				   and b.rank_no=@drg_rank_no
				   -- IF COAG AND COAGFLG THEN COAG = 0
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'COAGDRG'
				   and a.comorb_code = 'COAG'
				   and b.rank_no=@drg_rank_no
				   -- IF OBESE AND (NUTRFLG OR OBESEFLG) THEN  OBESE = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg in ('NUTRDRG','OBESEDRG')
				   and a.comorb_code = 'OBESE'
				   and b.rank_no=@drg_rank_no
				   -- IF WGHTLOSS AND NUTRFLG THEN WGHTLOSS = 0
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'NUTRDRG'
				   and a.comorb_code = 'WGHTLOSS'
				   and b.rank_no=@drg_rank_no
				   -- IF LYTES AND NUTRFLG THEN LYTES = 0
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'NUTRDRG'
				   and a.comorb_code = 'LYTES'
				   and b.rank_no=@drg_rank_no
				   -- IF BLDLOSS AND ANEMFLG THEN BLDLOSS = 0
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'ANEMDRG'
				   and a.comorb_code = 'BLDLOSS'
				   and b.rank_no=@drg_rank_no
				 --IF ANEMDEF AND ANEMFLG THEN ANEMDEF = 0;
				   update a 
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'ANEMDRG'
				   and a.comorb_code = 'ANEMDEF'
				   and b.rank_no=@drg_rank_no
				  --IF ALCOHOL AND ALCFLG THEN ALCOHOL = 0;
				   update a 
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'ALCDRG'
				   and a.comorb_code = 'ALCOHOL'
				   and b.rank_no=@drg_rank_no
				  --IF DRUG AND ALCFLG THEN DRUG = 0;
				   update a 
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'ALCDRG'
				   and a.comorb_code = 'DRug'
				   and b.rank_no=@drg_rank_no
				  --IF PSYCH AND PSYFLG THEN PSYCH = 0;
				   update a 
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'PSYDRG'
				   and a.comorb_code = 'PSYCH'
				   and b.rank_no=@drg_rank_no
				  --IF PSYCH AND PSYFLG THEN PSYCH = 0;
				   update a 
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'DEPRSDRG'
				   and a.comorb_code = 'DEPRESS'
				   and b.rank_no=@drg_rank_no
				  --IF PARA AND CEREFLG THEN PARA = 0;
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'CEREDRG'
				   and a.comorb_code = 'PARA'
				   and b.rank_no=@drg_rank_no
				   
				  --remove Arrythm comorbidity if CARDRG is set
				   update a
				   set a.drg_reject_yn = 'Y'
				   From #patient_comrb_with_dates a
				   join #input_dataset_mult_drg b
				   on a.encounter_id = b.encounter_id
				   and a.patient_id = b.patient_id
				   and b.elix_drg = 'CARDDRG'
				   and a.comorb_code = 'ARRHYTH'
				   and b.rank_no=@drg_rank_no

				   FETCH NEXT FROM multi_drg
					INTO @drg_rank_no  
				  End--end @cursor

				CLOSE multi_drg
				DEALLOCATE multi_drg
			END --if @cal_score_by_admission= 1
	
		 EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: remove comorbidites related to DRGs', @@rowcount, @step_ct
		 set @step_ct = @step_ct + 1

		 -- ******************************************************************************************************
		 --  Combine the HTN_COMP and HTN comorbities into HTN_C to align with HCUP reporting
		 -- ******************************************************************************************************
		 if @cal_score_by_admission= 1
			BEGIN
			 update a
			 set a.comorb_flag =0
			 from #patient_comrb_with_dates a
			 join 
			 (
				 Select distinct encounter_id,patient_id
				 from #patient_comrb_with_dates a
				 where comorb_code = 'HTN_C' 
				 and (comorb_flag <>0 and a.drg_reject_yn = 'N')
				 intersect
				 Select distinct z.encounter_id,z.patient_id
				 from
				 (   
					 select distinct x.encounter_id,x.patient_id, y.encounter_id as trg_encounter_id,y.patient_id as trg_patient_id
					 from #patient_comrb_with_dates x
					 left join 
					 (
						Select encounter_id, patient_id 
						from #patient_comrb_with_dates a
						where comorb_code in ('HTN')
						and (comorb_flag <>0 and a.drg_reject_yn = 'N') 
					 )
					 y
					 on x.encounter_id = y.encounter_id
					 and x.patient_id = y.patient_id
				 )
				 z
				 where z.trg_patient_id is null 
				 intersect
				 Select distinct z.encounter_id,z.patient_id
				 from
				 (   
					 select distinct x.encounter_id,x.patient_id, y.encounter_id as trg_encounter_id,y.patient_id as trg_patient_id
					 from #patient_comrb_with_dates x
					 left join 
					 (
						Select encounter_id, patient_id 
						from #patient_comrb_with_dates a
						where comorb_code in ('HTN_COMP')
						and (comorb_flag <>0 and a.drg_reject_yn = 'N') 
					 )
					 y
					 on x.encounter_id = y.encounter_id
					 and x.patient_id = y.patient_id
				 )
				 z
				 where z.trg_patient_id is null 
			 )
			 b
			 on a.encounter_id = b.encounter_id
			 and a.patient_id = b.patient_id
			 and a.comorb_code = 'HTN_C'
			 option (hash join);
             End
		 EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: calculate total comorbidities', @@rowcount, @step_ct
		 set @step_ct = @step_ct + 1
		 -- ******************************************************************************************************
		 --  Calculate the total # of comorbidities
		 -- ******************************************************************************************************
		 IF OBJECT_ID('tempdb..#patient_comrb_op') IS NOT NULL
		 truncate TABLE #patient_comrb_op

		 insert into #patient_comrb_op
		 ( 
		  encounter_id,patient_id,AIDS,ALCOHOL,ANEMDEF,ARTH,BLDLOSS,CHF,CHRNLUNG,COAG,DEPRESS,DM,DMCX,DRUG,HTN_C,HYPOTHY,
		  LIVER,LYMPH,LYTES,METS,NEURO,OBESE,PARA,PERIVASC,PSYCH,PULMCIRC,RENLFAIL,TUMOR,ULCER,VALVE,WGHTLOSS,ARRHYTH
		 )
		 Select encounter_id,patient_id,isnull(AIDS,0) as AIDS,isnull(ALCOHOL,0) as ALCOHOL,isnull(ANEMDEF,0) as ANEMDEF,isnull(ARTH,0) as ARTH,isnull(BLDLOSS,0) as BLDLOSS,
					isnull(CHF,0) as CHF,isnull(CHRNLUNG,0) as CHRNLUNG,isnull(COAG,0) as COAG,isnull(DEPRESS,0) as DEPRESS,isnull(DM,0) as DM,
					isnull(DMCX,0) as DMCX,isnull(DRUG,0) as DRUG,isnull(HTN_C,0) as HTN_C,isnull(HYPOTHY,0) as HYPOTHY,isnull(LIVER,0) as LIVER,
					isnull(LYMPH,0) as LYMPH,isnull(LYTES,0) as LYTES,isnull(METS,0) as METS,isnull(NEURO,0) as NEURO,isnull(OBESE,0) as OBESE,
					isnull(PARA,0) as PARA,isnull(PERIVASC,0) as PERIVASC,isnull(PSYCH,0) as PSYCH,isnull(PULMCIRC,0) as PULMCIRC,isnull(RENLFAIL,0) as RENLFAIL,
					isnull(TUMOR,0) as TUMOR,isnull(ULCER,0) as ULCER,isnull(VALVE,0) as VALVE,isnull(WGHTLOSS,0) as WGHTLOSS, isnull(ARRHYTH,0) as ARRHYTH
		 from
			(
				Select encounter_id, patient_id,comorb_code,comorb_flag
				From #patient_comrb_with_dates a
				join dbo.teci_comorb_codes b
				on a.comorb_code = b.CODE
				and a.comorb_flag = 1
				where a.comorb_code not in ('HTN_COMP','HTN')
				and a.drg_reject_yn = 'N'
			)
			src
			pivot (
					 min(comorb_flag) for comorb_code in (
															 AIDS,ALCOHOL,ANEMDEF,ARTH,BLDLOSS,CHF,CHRNLUNG,COAG,DEPRESS,DM,DMCX,DRUG,HTN_C,HYPOTHY,LIVER,
															 LYMPH,LYTES,METS,NEURO,OBESE,PARA,PERIVASC,PSYCH,PULMCIRC,RENLFAIL,TUMOR,ULCER,VALVE,WGHTLOSS,ARRHYTH

														) 
				  )
				  as pvt
      
	     -- add columns to hold score and index. 
		 Alter table #patient_comrb_op
		 add comorbidity_score int, van_index int;

		 update a
		 set a.comorbidity_score =
								 isnull(AIDS,0)+isnull(ALCOHOL,0)+isnull(ANEMDEF,0)+isnull(ARTH,0)+isnull(BLDLOSS,0)+isnull(CHF,0)+isnull(CHRNLUNG,0)+isnull(COAG,0)+ 
								 isnull(DEPRESS,0)+isnull(DM,0)+isnull(DMCX,0)+isnull(DRUG,0)+isnull(HTN_C,0)+isnull(HYPOTHY,0)+isnull(LIVER,0)+isnull(LYMPH,0)+ 
								 isnull(LYTES,0)+isnull(METS,0)+isnull(NEURO,0)+isnull(OBESE,0)+isnull(PARA,0)+isnull(PERIVASC,0)+isnull(PSYCH,0)+isnull(PULMCIRC,0)+ 
								 isnull(RENLFAIL,0)+isnull(TUMOR,0)+isnull(ULCER,0)+isnull(VALVE,0)+isnull(WGHTLOSS,0)+    
								 case
									WHEN @ARRHYTH_Include = 1 THEN ARRHYTH
									ELSE 0
								 end			
		 from #patient_comrb_op a


		--Select * 
		--into dbo.fn_test_patient_comrb_op1
		--From #patient_comrb_op
         
		 EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: calculate total comorbidities', @@rowcount, @step_ct
		 set @step_ct = @step_ct + 1
		 -- ******************************************************************************************************
		 -- Calculate the weighted van Walvaren coborbidity score
		 -- http://czresearch.com/dropbox/vanWalraven_MedCare_2009v47p626.pdf
		 -- range of scores = -19 to 89 with arrhythmia included, -19 to 84 without
		 -- HCUP removed the ARRHYTH FROM the Elixhauser comorbodity calcuation in 2004 due to concern about
		 -- lack of validity 
		 -- ******************************************************************************************************
		 declare
				@AIDS		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'AIDS'),
				@ALCOHOL	int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'ALCOHOL'),
				@ANEMDEF	int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'ANEMDEF'),
				@ARTH		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'ARTH'),
				@BLDLOSS	int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'BLDLOSS'),
				@CHF		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'CHF'),
				@CHRNLUNG	int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'CHRNLUNG'),
				@COAG		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'COAG'),
				@DEPRESS	int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'DEPRESS'),
				@DM			int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'DM'),
				@DMCX		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'DMCX'),
				@DRUG		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'DRUG'),
				@HTN_C		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'HTN_C'),
				@HYPOTHY	int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'HYPOTHY'),
				@LIVER		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'LIVER'),
				@LYMPH		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'LYMPH'),
				@LYTES		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'LYTES'),
				@METS		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'METS'),
				@NEURO		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'NEURO'),
				@OBESE		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'OBESE'),
				@PARA		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'PARA'),
				@PERIVASC	int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'PERIVASC'),
				@PSYCH		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'PSYCH'),
				@PULMCIRC	int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'PULMCIRC'),
				@RENLFAIL	int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'RENLFAIL'),
				@TUMOR		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'TUMOR'),
				@ULCER		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'ULCER'),
				@VALVE		int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'VALVE'),
				@WGHTLOSS	int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'WGHTLOSS'),
				@ARRHYTH	int = (SELECT indx_weight FROM dbo.teci_vwindex_weights WHERE comorb_code = 'ARRHYTH')


		UPDATE #patient_comrb_op
		SET  
			van_index =
			AIDS		* @AIDS		+
			ALCOHOL		* @ALCOHOL	+
			ANEMDEF		* @ANEMDEF	+
			ARTH		* @ARTH		+
			BLDLOSS		* @BLDLOSS	+
			CHF			* @CHF		+
			CHRNLUNG	* @CHRNLUNG +
			COAG		* @COAG		+
			DEPRESS		* @DEPRESS	+
			DM			* @DM		+
			DMCX		* @DMCX		+
			DRUG		* @DRUG		+
			HTN_C		* @HTN_C	+
			HYPOTHY		* @HYPOTHY	+
			LIVER		* @LIVER	+
			LYMPH		* @LYMPH	+
			LYTES		* @LYTES	+
			METS		* @METS		+
			NEURO		* @NEURO	+
			OBESE		* @OBESE	+
			PARA		* @PARA		+
			PERIVASC	* @PERIVASC +
			PSYCH		* @PSYCH	+
			PULMCIRC	* @PULMCIRC +	
			RENLFAIL	* @RENLFAIL +
			TUMOR		* @TUMOR	+
			ULCER		* @ULCER	+
			VALVE		* @VALVE	+
			WGHTLOSS	* @WGHTLOSS +
			CASE
					WHEN @ARRHYTH_Include = 1 THEN isnull(ARRHYTH,0) * @ARRHYTH
					ELSE 0
					END

		--Select * 
		--into dbo.fn_test_patient_comrb_op2
		--From #patient_comrb_op

		EXEC dbo.teci_spx_write_audit @job_id, @database_name, @procedure_name, 'Start: wiriting to cindex_comorb_van_score_op', @@rowcount, @step_ct
		set @step_ct = @step_ct + 1
		-- ******************************************************************************************************
		-- Output the data to the TABLE dbo.teci_comorb_van_score_op
		-- ******************************************************************************************************
		Exec (
				'INSERT INTO dbo.teci_comorb_van_score_op
				(
					study_name,encounter_id,patient_id,AIDS,ALCOHOL,ANEMDEF,ARTH,BLDLOSS,CHF,CHRNLUNG,COAG,DEPRESS,DM,DMCX,DRUG,HTN_C,HYPOTHY,LIVER,LYMPH,LYTES,
					METS,NEURO,OBESE,PARA,PERIVASC,PSYCH,PULMCIRC,RENLFAIL,TUMOR,ULCER,VALVE,WGHTLOSS,ARRHYTH,comorbidity_score,van_index
				)
				Select distinct study_name,	case 
						when ' + @cal_score_by_admission + ' = 1 then a.encounter_id else ''1'' end as  encounter_id,		
					   a.patient_id,
					   isnull(AIDS,0) as AIDS,isnull(ALCOHOL,0) as ALCOHOL,isnull(ANEMDEF,0) as ANEMDEF,isnull(ARTH,0) as ARTH,isnull(BLDLOSS,0) as BLDLOSS,
					   isnull(CHF,0) as CHF,isnull(CHRNLUNG,0) as CHRNLUNG,isnull(COAG,0) as COAG,isnull(DEPRESS,0) as DEPRESS,isnull(DM,0) as DM,
					   isnull(DMCX,0) as DMCX,isnull(DRUG,0) as DRUG,isnull(HTN_C,0) as HTN_C,isnull(HYPOTHY,0) as HYPOTHY,isnull(LIVER,0) as LIVER,
					   isnull(LYMPH,0) as LYMPH,isnull(LYTES,0) as LYTES,isnull(METS,0) as METS,isnull(NEURO,0) as NEURO,isnull(OBESE,0) as OBESE,
					   isnull(PARA,0) as PARA,isnull(PERIVASC,0) as PERIVASC,isnull(PSYCH,0) as PSYCH,isnull(PULMCIRC,0) as PULMCIRC,isnull(RENLFAIL,0) as RENLFAIL,
					   isnull(TUMOR,0) as TUMOR,isnull(ULCER,0) as ULCER,isnull(VALVE,0) as VALVE,isnull(WGHTLOSS,0) as WGHTLOSS, isnull(ARRHYTH,0) as ARRHYTH,
					   isnull(comorbidity_score,0) comorbidity_score, isnull(van_index, 0) van_index
				from dbo.' + @input_table_name +  ' a
				left join #patient_comrb_op b
				on a.patient_id = b.patient_id
				and b.encounter_id = case when ' + @cal_score_by_admission + ' = 1 then a.encounter_id else ''1'' end
				'
		    );
	
	 

end try

	BEGIN CATCH
		--End Proc
		EXEC dbo.teci_spx_end_audit @job_id, 'FAIL'
	END CATCH

END
GO