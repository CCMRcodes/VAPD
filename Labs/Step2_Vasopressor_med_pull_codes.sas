/*This is the example code to clean up the vasopressor drugs pulled from step 1 in CDW*/

/*This is the example sql code to pull Other drugs (Sedatives, Analgesics & Paralytics) from CDW*/
/*Download the saved dflt. tables into SAS datsets and do further data manipulations*/

%LET Studyname=   ;/*insert study name*/
%Let data_source= " " ;/*inser data source*/

libname sepsis 'INSERT FOLDER PATH';

/*** download to the dflt tables into SAS datasets */
/*BCMADispensedDrug_Pressor*/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table sepsis.BCMADispensedDrug_Pressor (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[BCMADispensedDrug_Pressor] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*BCMA_Additive_pressor*/
PROC SQL ;  
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ); 

create table sepsis.BCMA_Additive_pressor (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[BCMA_Additive_pressor] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*BCMA_Solution_pressor*/
PROC SQL ; 
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table sepsis.BCMA_Solution_pressor (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[BCMA_Solution_pressor] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 



/*Below are also SAS codes for data management*/
/***** Dispensed Drugs *****/
data check2; /*0 with missing patienticn*/
set sepsis.BCMADispensedDrug_pressor;
if patienticn=.;
run;


/*change patienticn into numeric*/ 
DATA BCMA_BCMADispensedDrug_v3 (rename=patienticn2=patienticn); 
SET sepsis.BCMADispensedDrug_pressor;
patienticn2 = input(patienticn, 10.);
drop patienticn;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
Dispensed=1; /*indicator for the dispensed table*/
RUN;

/*remove duplicates*/
PROC SORT DATA=BCMA_BCMADispensedDrug_v3 nodupkey  out=BCMA_BCMADispensedDrug_v4 (compress=yes); 
BY patientsid patienticn  Sta3n ActionDate LocalDrugSID DrugNameWithoutDose LocalDrugNameWithDose NationalDrugNameWithDose
 NationalDrug UnitDoseMedicationRoute BCMADispensedDrugSID BCMAMedicationLogSID;
RUN;

/*************************************************************************************/
/*IV Additive*/
/*change patienticn into numeric*/ 
DATA BCMA_Additive_v3 (rename=patienticn2=patienticn);  
SET sepsis.BCMA_Additive_pressor;
patienticn2 = input(patienticn, 10.);
drop patienticn;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
Additive=1; /*indicator for the dispensed table*/
RUN;

/*remove duplicates*/
PROC SORT DATA= BCMA_Additive_v3 nodupkey  out=BCMA_Additive_v4 (compress=yes); 
BY patientsid patienticn  Sta3n ActionDate IVAdditiveIngredientSID BCMAAdditiveSID LocalDrugSID 
BCMAMedicationLogSID LocalDrugNameWithDose IVAdditiveIngredientPrintName;
run;

/*************************************************************************************/
/*IV Solutions*/
/*change patienticn into numeric*/ 
DATA BCMA_Solution_v3 (rename=patienticn2=patienticn); 
SET sepsis.BCMA_Solution_pressor;
patienticn2 = input(patienticn, 10.);
drop patienticn;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
Solution=1; /*indicator for the dispensed table*/
RUN;

/*remove duplicates*/
PROC SORT DATA=BCMA_Solution_v3 nodupkey  out=BCMA_Solution_v4 (compress=yes); 
BY patientsid patienticn  Sta3n ActionDate IVSolutionIngredientSID BCMASolutionSID LocalDrugSID 
BCMAMedicationLogSID LocalDrugNameWithDose IVSolutionFirstIngredientPrintNa;
run;

proc freq data=BCMA_Solution_v4 order=freq;
table LocalDrugNameWithDose IVSolutionFirstIngredientPrintNa;
run;


/***************************************************************************/
/*create drug_name field for each localdrugsid, first combine datasets with only the drug names*/
data BCMADispensedDrug_names (compress=yes); 
set BCMA_BCMADispensedDrug_v4;
keep LocalDrugSID DrugNameWithoutDose LocalDrugNameWithDose NationalDrugNameWithDose NationalDrug;
run;

data Additive_names (compress=yes); 
set BCMA_Additive_v4;
keep LocalDrugSID LocalDrugNameWithDose IVAdditiveIngredientPrintName;
run;

data Solution_names (compress=yes);
set BCMA_Solution_v4;
keep LocalDrugSID LocalDrugNameWithDose IVSolutionFirstIngredientPrintNa;
run;

data combined_names (compress=yes); /*combine above 3 datasets, all 3 datasets have field: LocalDrugNameWithDose*/ 
set BCMADispensedDrug_names Additive_names solution_names;
run;

/***************************************************************************/

/*label LocalDrugSIDs with drug_name*/
PROC SQL;
CREATE TABLE pharm3  AS  
SELECT *,
       case when DrugNameWithoutDose like '%DOPAMINE%' or LocalDrugNameWithDose  like '%DOPAMINE%' or NationalDrugNameWithDose  like '%DOPAMINE%' 
	              or NationalDrug  like '%DOPAMINE%' or LocalDrugNameWithDose like '%DEXTROSE%' or LocalDrugNameWithDose like '%ZZDOPamine%'
				  or LocalDrugNameWithDose like '%ZZDOPamine%' or LocalDrugNameWithDose like '%zz dopamine%' or LocalDrugNameWithDose like '%DOPamine%' 
			or LocalDrugNameWithDose like 'ZZZDOPamine'
			  	then 'DOPAMINE'
			when DrugNameWithoutDose like '%NOREPINEPHRINE%' or LocalDrugNameWithDose  like '%NOREPINEPHRINE%' or NationalDrugNameWithDose  like '%NOREPINEPHRINE%' 
	              or NationalDrug  like '%NOREPINEPHRINE%'   
				then 'NOREPINEPHRINE'
			when DrugNameWithoutDose like '%EPINEPHRINE%' or LocalDrugNameWithDose  like '%EPINEPHRINE%' or NationalDrugNameWithDose  like '%EPINEPHRINE%' 
	              or NationalDrug  like '%EPINEPHRINE%'   or LocalDrugNameWithDose like '%EPINEPHrine%'  or LocalDrugNameWithDose like '%EPINephrine%' 
				  or LocalDrugNameWithDose like '%EpinEPHRINE%' or LocalDrugNameWithDose like '%EpinEPHRINE%'  or LocalDrugNameWithDose like '%ZZEPINEPH%' 
				   or LocalDrugNameWithDose like '%ZZEPINEPHrine%' or LocalDrugNameWithDose like '%ZZEpiNEPHRINE%' or LocalDrugNameWithDose like '%ZZZEpiNEPHRINE%'
				 or LocalDrugNameWithDose like '%zz epinephrine %'  or LocalDrugNameWithDose like '%zz epinephrine.%'
				then 'EPINEPHRINE'
			when DrugNameWithoutDose like '%PHENYLEPHRINE%' or LocalDrugNameWithDose  like '%PHENYLEPHRINE%' or NationalDrugNameWithDose  like '%PHENYLEPHRINE%' 
	              or NationalDrug  like '%PHENYLEPHRINE%' or LocalDrugNameWithDose like '%zz phenylephrine %'   
				then 'PHENYLEPHRINE'
			when DrugNameWithoutDose like '%VASOPRESSIN%' or LocalDrugNameWithDose  like '%VASOPRESSIN%' or NationalDrugNameWithDose  like '%VASOPRESSIN%' 
	              or NationalDrug  like '%VASOPRESSIN%'   or LocalDrugNameWithDose like '%zz vasopressin%' or LocalDrugNameWithDose like '%zz vasopressin.%'
				then 'VASOPRESSIN'
			  ELSE 'UNKNOWN'
END AS drug_name
FROM combined_names
GROUP BY drug_name
ORDER BY drug_name;
QUIT;

proc freq data=pharm3 order=freq;
table  drug_name;
run;

/*remove duplicate localdrugsid and drug_name*/
proc sort data=pharm3 nodupkey out=pharm3b; 
by localdrugsid  drug_name;
run;

/*left join drug_name field back to dispensed, additive and solution tables, exclude study or unknown drugs*/
/*dispensed drugs*/
proc sql;
	create table BCMA_BCMADispensedDrug_v5 (compress=yes) as  
	select a.*, b.drug_name
	from BCMA_BCMADispensedDrug_v4 a
	left join pharm3b b on a.localdrugsid =b.localdrugsid;
quit;

proc freq data=BCMA_BCMADispensedDrug_v5; 
table drug_name;
run; 

/*delete those with study/test or unknown drugs*/
data BCMA_BCMADispensedDrug_v6 (compress=yes); 
set BCMA_BCMADispensedDrug_v5;
if drug_name in ('STUDY/TEST DRUG','UNKNOWN')  then delete;
keep patienticn patientsid sta3n drug_name ActionDate;
run;


/*additive*/
proc sql;
	create table BCMA_Additive_v5 (compress=yes) as  
	select a.*, b.drug_name
	from BCMA_Additive_v4 a
	left join pharm3b b on a.localdrugsid =b.localdrugsid;
quit;

proc freq data=BCMA_Additive_v5;
table drug_name;
run; 

/*delete those with study/test or unknown drugs*/
data BCMA_Additive_v6 (compress=yes);
set BCMA_Additive_v5;
if drug_name in ('STUDY/TEST DRUG','UNKNOWN')  then delete;
keep patienticn patientsid sta3n drug_name ActionDate;
run;

/*solution*/
proc sql;
	create table BCMA_Solution_v5 (compress=yes) as 
	select a.*, b.drug_name
	from BCMA_Solution_v4 a
	left join pharm3b b on a.localdrugsid =b.localdrugsid;
quit;

proc freq data=BCMA_Solution_v5; 
table drug_name;
run; 

/*delete those with study/test or unknown drugs*/
data BCMA_Solution_v6 (compress=yes); 
set BCMA_Solution_v5;
if drug_name in ('STUDY/TEST DRUG','UNKNOWN')  then delete;
keep patienticn patientsid sta3n drug_name ActionDate;
run;


/*combine all three together, then remove duplcates by unique patienticn, sta3n, drug_name and action_date*/
data combined (compress=yes);
set BCMA_BCMADispensedDrug_v6 BCMA_Additive_v6  BCMA_Solution_v6;
run;

proc sort data=combined nodupkey out=pressors_2014_2017; 
by patienticn ActionDate drug_name ;
run;




/*manipulate other_drugs_2014_2017 dataset as desired before left join to VAPD cohort by unique patienticn and date*/
/*transpose dataset if needed*/
DATA trans_pressors_2014_2017 (compress=yes); 
SET pressors_2014_2017;
keep patienticn ActionDate drug_name;
RUN;

/*check freq if drug_name field has study/test or unknown drugs*/
proc freq data=trans_pressors_2014_2017;
table drug_name;
run;

PROC TRANSPOSE DATA=trans_pressors_2014_2017 OUT=sepsis.trans_pressors_2014_2017 (DROP=_NAME_) PREFIX= drugname_; 
BY patienticn ActionDate;
VAR  drug_name;
RUN;  
