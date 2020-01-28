--Use DatabaseName; --Replace with your desired database name

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- The script will walk through the steps to install TECI toolkit and generate score on test data 

--author: Syed et al.
--copyright (C) Dept of Biomedical Informatics, UAMS. 

--Script was built using SQL script provided in the manuscript of 
    --Development and validation of astructured query language implementation of the Elixhauser comorbidity index
	--author: Richard H. Epstein, MD
	--copyright (C) UM Department of Anesthesia
	--Based ON the HCUP SAS Elixhauser Comorbidity Software v3.7
	--https://www.hcup-us.ahrq.gov/toolssoftware/comorbidity/comorbidity.jsp 
-----------------------------------------------------------------------------------------------------------------------------------------------------

--Step 1: Create Audit objects by executing scripts present in file teci_create_audit_object.sql
--Step 2: Create TECI objects by creating stored procedures from file teci_spx_toolkit_setup.sql,
--Step 3: Create stored procedures by executing scripts present in below files,in the mentioned order. 
          --teci_spx_load_icd.sql
		  --teci_spx_load_drg.sql
		  --teci_spx_stg_scorecalc.sql
		  --teci_comrb_index_score.sql
--Step 4: Execute stored procedure teci_spx_toolkit_setup
--Step 5: to create test data, execute script from file test_input_cdr.sql
--Step 6: Run below sql to load test data into table teci_input_source. To capture first instance of diagnosis and DRG records of patients in test data.
            if (object_id('dbo.teci_input_source') is not null) 
			Begin
				Truncate table dbo.teci_input_source;
				insert into dbo.teci_input_source
				(patient_id,code_type,code,dx_drg_prim_sec,first_dx_drg_date)
				select patient_id,code_type,code,dx_type as dx_drg_prim_sec, first_dx_drg_date
				from 
				(
					select patient_id,code_type,code,dx_type, min(dx_date) as first_dx_drg_date
					From dbo.test_input_cdr
					group by patient_id,code_type,code,dx_type
				)
				x
			End
--Step 7: Execute stored procedure teci_spx_stg_scorecalc, this will identify first Elixhauser comorbidities of pateints.
--Step 8: to create date-specific dataset, execute script from file test_date_specific_dataset.sql
--Step 9: Execute stored procedure teci_comrb_index_score, this will calculate time specific Elixhauser comorbidity Index and van walraven scores for test pateints.
		  --Use below parameters to execute SP teci_comrb_index_score
		  ---- exec dbo.teci_spx_comrb_index_score 'teci_date_specf_dataset', '2016-12-31',1,0,1

--Step 10: Check the result in table "teci_comorb_van_score_op" for study_name ="test_teci_installation", column ""comorbidity_score" should have value "4" and van_index as "0"
		   --Select * from teci_comorb_van_score_op where study_name ='test_teci_installation'
