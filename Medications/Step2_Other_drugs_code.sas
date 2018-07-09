/*This is the example code to clean up the Other drugs (Sedatives, Analgesics & Paralytics) pulled from step 1 in CDW*/
/*Download the saved dflt. tables into SAS datsets and do further data manipulations*/

/* Date Modified: 6/11/2018
   Author: Shirley Wang */

%LET Studyname=   ;/*insert study name*/
%Let data_source= " " ;/*inser data source*/

libname sepsis 'INSERT FOLDER PATH';

/*** download to SAS datasets */
/*BCMADispensedDrug*/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 (PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ); 
create table sepsis.BCMA_BCMADispensedDrug_v2 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[BCMA_BCMADispensedDrug_v2] as t1) as t1;
DISCONNECT FROM CDW1;
quit; 

/*BCMA_Additive*/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 
create table sepsis.BCMA_Additive_v1 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[BCMA_Additive_v1] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit;

/*BCMA_Solution*/
PROC SQL ; 
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ); 
create table sepsis.BCMA_Solution_v1 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[BCMA_Solution_v1] as t1) as t1;
DISCONNECT FROM CDW1;
quit;


/*Below are SAS codes*/
/***** Dispensed Drugs *****/
data check2; /*0 with missing patienticn*/
set sepsis.BCMA_BCMADispensedDrug_v2;
if patienticn=.;
run;

/*change patienticn into numeric*/ 
DATA BCMA_BCMADispensedDrug_v3 (rename=patienticn2=patienticn); 
SET sepsis.BCMA_BCMADispensedDrug_v2;
patienticn2 = input(patienticn, 10.);
drop patienticn;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
Dispensed=1; /*indicator for the dispensed table*/
RUN;

/*check and remove duplicates*/
PROC SORT DATA=BCMA_BCMADispensedDrug_v3 nodupkey  out=BCMA_BCMADispensedDrug_v4 (compress=yes); 
BY patientsid patienticn  Sta3n ActionDate LocalDrugSID DrugNameWithoutDose LocalDrugNameWithDose NationalDrugNameWithDose
 NationalDrug UnitDoseMedicationRoute BCMADispensedDrugSID BCMAMedicationLogSID;
RUN;


/******** IV Additive *********/
/*change patienticn into numeric*/ 
DATA BCMA_Additive_v3 (rename=patienticn2=patienticn);
SET sepsis.BCMA_Additive_V1;
patienticn2 = input(patienticn, 10.);
drop patienticn;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
Additive=1; /*indicator for the dispensed table*/
RUN;

/*check and remove duplicates*/
PROC SORT DATA= BCMA_Additive_v3 nodupkey  out=BCMA_Additive_v4 (compress=yes); 
BY patientsid patienticn  Sta3n ActionDate IVAdditiveIngredientSID BCMAAdditiveSID LocalDrugSID 
BCMAMedicationLogSID LocalDrugNameWithDose IVAdditiveIngredientPrintName;
run;


/****** IV Solutions ******/
DATA BCMA_Solution_v3 (rename=patienticn2=patienticn);
SET sepsis.BCMA_Solution_V1;
patienticn2 = input(patienticn, 10.);
drop patienticn;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
Solution=1; /*indicator for the dispensed table*/
RUN;

/*check and remove duplicates*/
PROC SORT DATA=BCMA_Solution_v3 nodupkey  out=BCMA_Solution_v4 (compress=yes); 
BY patientsid patienticn  Sta3n ActionDate IVSolutionIngredientSID BCMASolutionSID LocalDrugSID 
BCMAMedicationLogSID LocalDrugNameWithDose IVSolutionFirstIngredientPrintNa;
run;

proc freq data=BCMA_Solution_v4 order=freq;
table LocalDrugNameWithDose IVSolutionFirstIngredientPrintNa;
run;


/**************************************************************************************************************/
/*create "drug_name" field for each localdrugsid, first combine datasets with only the drug names*/
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

data combined_names (compress=yes); /*combine all 3 above datasets, all 3 datasets have field: LocalDrugNameWithDose*/ 
set BCMADispensedDrug_names Additive_names solution_names;
run;

/********************************************************************/
/*label each LocalDrugSID with drug_name*/
PROC SQL;
CREATE TABLE pharm3  AS  
SELECT *,
       case when  LocalDrugNameWithDose  like '%STUDY%' or  DrugNameWithoutDose  like '%STUDY%'  
				or LocalDrugNameWithDose  like '%TEST%' or  DrugNameWithoutDose  like '%TEST%'
				then 'STUDY/TEST DRUG'
			when LocalDrugNameWithDose  like '%PROPOFOL%' or LocalDrugNameWithDose  like 'PROPOFOL%' or LocalDrugNameWithDose  like 'ZZ DIPRIVAN%' 
	              or LocalDrugNameWithDose  like 'DIPRIVAN%'  
				then 'PROPOFOL'
	        when  LocalDrugNameWithDose  like '%VECURONIUM%' or LocalDrugNameWithDose  like 'VECURONIUM%' or LocalDrugNameWithDose  like 'VECuronium BR%' 
			     or LocalDrugNameWithDose  like 'VECuronium Br%'  or LocalDrugNameWithDose  like 'VERCURONIUM BROMIDE 10MG'  
				then 'VECURONIUM'
			when LocalDrugNameWithDose  like '%CISATRACURIUM%'  or LocalDrugNameWithDose  like 'CISATRACURIUM%' or LocalDrugNameWithDose  like 'CISATRUCURIUM%'
				 OR  LocalDrugNameWithDose  like 'cisatracurium%' 
                then 'CISATRACURIUM' /*since CISATRACURIUM contains ATRACURIUM*/
			when  LocalDrugNameWithDose  like '%ATRACURIUM%' or  LocalDrugNameWithDose  like 'ATRACURIUM%' or  LocalDrugNameWithDose  like 'TRACRIUM INJ%'
 				 OR LocalDrugNameWithDose  like 'TRACURIUM INJ%'  OR LocalDrugNameWithDose  like 'zz atracurium besylate%' OR LocalDrugNameWithDose  like 'ATRACRIUM BESYLATE%'  
                then 'ATRACURIUM'
			when LocalDrugNameWithDose  like '%ZZMORPHHINE%' OR LocalDrugNameWithDose  like '%MORphiNE%' OR LocalDrugNameWithDose  like '%DURAMORPH%'
	              OR LocalDrugNameWithDose  like '%MORphine%' OR LocalDrugNameWithDose  like '%zzzMORphine%' OR LocalDrugNameWithDose  like '%OPIUM%'
				  OR LocalDrugNameWithDose  like '%MorPHINE SULFATE%' OR LocalDrugNameWithDose  like '%MORPHINE%'  OR LocalDrugNameWithDose  like 'MORPHINE%'
				  OR LocalDrugNameWithDose  like 'MS CONTIN%' OR LocalDrugNameWithDose  like 'zz morphine%' OR LocalDrugNameWithDose  like 'morphine'
				  OR LocalDrugNameWithDose  like 'ORAMORPH 15MG%'  OR LocalDrugNameWithDose  like 'ORAMORPH 30MG%'  
				  OR LocalDrugNameWithDose  like 'ZZMS CONTIN%'  OR LocalDrugNameWithDose  like 'ZZINFUMORPH%' OR LocalDrugNameWithDose  like 'ZZZmorphine%' 
				  OR LocalDrugNameWithDose  like 'MorPHINE%' OR LocalDrugNameWithDose  like 'ORAMORPH SR%' OR LocalDrugNameWithDose  like 'ZZ-ORAMORPH SR%' 
				  OR LocalDrugNameWithDose  like 'ZZ.ORAMORPH SR%' OR LocalDrugNameWithDose  like 'ZZZMORPPHINE SO4%'  OR LocalDrugNameWithDose  like 'ZZZMS CONTIN%'
				  OR LocalDrugNameWithDose  like 'ASTRAMORPH%' OR LocalDrugNameWithDose  like 'AVINZA 60 MG CAP%' OR LocalDrugNameWithDose  like 'BRAND MS CONTIN%'  
				  OR LocalDrugNameWithDose  like 'INFUMORPH%' OR LocalDrugNameWithDose  like 'MOPRHINE CONC%' OR LocalDrugNameWithDose  like 'wi-cache morphine%' 
				 OR LocalDrugNameWithDose  like 'Morphine (PF)%'  OR LocalDrugNameWithDose  like 'ORAMORPH%' OR LocalDrugNameWithDose like 'ASTROMORPH/PF 1MG/ML-2ML INJ' 
				then 'MORPHINE'
			when  LocalDrugNameWithDose  like '%LORazePAM%' OR LocalDrugNameWithDose  like '%LORazepam%' OR LocalDrugNameWithDose  like '%LORazepam%'
			      OR LocalDrugNameWithDose  like '%LORAZEPAM%'  OR LocalDrugNameWithDose  like 'LORAZEPAM%' or LocalDrugNameWithDose  like '%ATIVAN%'
				  OR LocalDrugNameWithDose  like 'LOrazePAM%'  OR LocalDrugNameWithDose  like 'zz ativan%' or LocalDrugNameWithDose  like 'zz lorazepam%' 
                  OR LocalDrugNameWithDose  like 'CACHE LORAZEPM%'   
                then 'LORAZEPAM'
			when  LocalDrugNameWithDose  like '%HYDROmorphone%' OR LocalDrugNameWithDose  like '%HYDROMORphone%' 
                  OR LocalDrugNameWithDose  like '%HYDROMORPHONE%'  OR LocalDrugNameWithDose  like 'HYDROMORPHONE%' OR LocalDrugNameWithDose  like 'HYDROmorPHONE%'
                  OR LocalDrugNameWithDose  like 'HYDROMORPH 5MG/BUPIV%' OR LocalDrugNameWithDose  like 'zz hydromorphone%'  
				  OR LocalDrugNameWithDose  like 'HYDROMOR DRIP%'  OR LocalDrugNameWithDose  like 'HYDROMORPH 10MCG%' OR LocalDrugNameWithDose  like 'HYDROMORPH 20MCG%'
				  OR LocalDrugNameWithDose  like 'HYDROMORP 25MCG%' OR LocalDrugNameWithDose  like 'hydroMORPHONE' OR LocalDrugNameWithDose  like 'HYDROMOR 10MCG/BUPIV%' 
				  OR LocalDrugNameWithDose  like 'HYDROMOR 0.02MG%' OR LocalDrugNameWithDose  like 'HYDROMORP/BUPIV%' OR LocalDrugNameWithDose  like 'HYDROMORPH/BUP EPID%'
				  OR LocalDrugNameWithDose  like 'XXHYDROMORPH/BUP EPID%' OR LocalDrugNameWithDose  like 'DILAUDID PUMP'  
				  OR LocalDrugNameWithDose  like 'DILAUDID 3MG RECTAL%' OR LocalDrugNameWithDose  like 'DILAUDID INJ 50MG%' OR LocalDrugNameWithDose  like 'HYDROMORP 25MCG%' 
				  OR LocalDrugNameWithDose  like 'HDROmorphone 10mg/ml *1*ml INJ' OR LocalDrugNameWithDose  like 'HYDROMOR 9MG/BUPIV%' OR LocalDrugNameWithDose  like 'HYDROMORHONE 2MG/ML%' 
				  OR LocalDrugNameWithDose  like 'HYDROMORHPONE 2MG/ML%' OR LocalDrugNameWithDose  like 'HYDROMORP 10MG%' OR LocalDrugNameWithDose  like 'HYDROMORP 1MG/ML/100ML%' 
				  OR LocalDrugNameWithDose  like 'HYDROMORPH INJ 10MG/ML 5ML VIA%' OR LocalDrugNameWithDose  like 'ZZDILAUDID%' OR LocalDrugNameWithDose  like 'ZZZHDROMORPHONE%'  
				  OR LocalDrugNameWithDose  like 'hydroMORPHONE%' OR LocalDrugNameWithDose  like 'HDROmorphone 10mg/ml *50*ml INJ'  
				  OR LocalDrugNameWithDose  like 'ZZBN: DILAUDID 2MG TAB' OR LocalDrugNameWithDose  like 'hydroMORPHone(DILAUDID) 5MG/ML 40ML SYR' 
				then 'HYDROMORPHONE'
			when  LocalDrugNameWithDose  like '%BUPIVA%'  OR  LocalDrugNameWithDose  like '%fentaNYL%' OR LocalDrugNameWithDose  like '%FENTANYL%' 
				  OR LocalDrugNameWithDose  like 'FentaNYL%'  OR LocalDrugNameWithDose  like 'DURAGESIC PATCH%'  
			      OR LocalDrugNameWithDose  like 'FENTANYL%'  OR LocalDrugNameWithDose  like 'ZZDURAGESIC%'  OR LocalDrugNameWithDose  like 'zz fentanyl citrat%' 
				  OR LocalDrugNameWithDose  like 'ZZZFentaNYL CITRATE%' OR LocalDrugNameWithDose  like 'ZZZ   FENT/BUPIVICAINE%'  
				  OR LocalDrugNameWithDose  like 'XX FENT/BUP%'  OR LocalDrugNameWithDose  like 'CPD-FENTAN%'  OR LocalDrugNameWithDose  like 'FENT 5MCG%'  
				  OR LocalDrugNameWithDose  like 'BRAND ACTIQ%'  OR LocalDrugNameWithDose  like 'DURAGESIC (BRAND)%'  OR LocalDrugNameWithDose  like 'DURAGESIC%'  
				  OR LocalDrugNameWithDose  like 'FENTORA%'  OR LocalDrugNameWithDose  like 'YYEARCH*fentanyl%'  
				  OR LocalDrugNameWithDose  like 'ZZZ  FENT/BUPIV 4MCG%' OR LocalDrugNameWithDose  like 'ZZZ  FENT/BUPIVICAINE%' OR LocalDrugNameWithDose  like 'zzSUBLIMAZE(OR GE)%'  
			    then 'FENTANYL'
			when LocalDrugNameWithDose  like '%PANCURONIUM BROMIDE%' or LocalDrugNameWithDose  like '%PANCURONIUM%' 
                 or LocalDrugNameWithDose  like 'PANCURONIUM BROMIDE%' or LocalDrugNameWithDose  like 'PANCURONIUM%' 
				 or LocalDrugNameWithDose  like 'zz pancuronium%'  or LocalDrugNameWithDose  like 'ZZPAVULON' 
				then 'PANCURONIUM BROMIDE'
			when  LocalDrugNameWithDose  like '%RIFAXIMIN%' or LocalDrugNameWithDose  like 'RIFAXIMIN%' OR LocalDrugNameWithDose  like 'riFAXiMIn%' 
				  OR LocalDrugNameWithDose  like 'RifAXIMin%'  OR LocalDrugNameWithDose  like 'RifaXIMin%'  
                 then 'RIFAXIMIN'
			when  LocalDrugNameWithDose  like '%KETAMINE%' OR LocalDrugNameWithDose  like 'KETAMIN%' OR LocalDrugNameWithDose  like 'zz ketamine%'    
				then 'KETAMINE'
			when LocalDrugNameWithDose  like '%LACTULOSE%' OR LocalDrugNameWithDose  like 'Lactulose Enema%' OR LocalDrugNameWithDose  like 'BRAND ENULOSE%'  
				 OR LocalDrugNameWithDose  like 'xxtestdrugyy' OR LocalDrugNameWithDose  like 'ZZZLACULS%' OR LocalDrugNameWithDose  like 'Lactulose 10g%' 
				 OR LocalDrugNameWithDose  like 'lactulose' OR LocalDrugNameWithDose  like 'ZZZLactulose Enema%' OR LocalDrugNameWithDose  like 'DUPHALAC SYRUP%'
				 OR LocalDrugNameWithDose  like 'XNF  CONSTULOSE%' OR LocalDrugNameWithDose  like 'XNF  ENULOSE%' 
				then 'LACTULOSE'
            when LocalDrugNameWithDose  like '%ETOMIDATE%' then 'ETOMIDATE'
		    when LocalDrugNameWithDose  like '%MIDAZOLAM%' OR LocalDrugNameWithDose  like 'ZZVERSED%' OR LocalDrugNameWithDose  like 'midazolam'
				 OR LocalDrugNameWithDose  like 'zz midazolam%' OR LocalDrugNameWithDose  like 'MIDAZ MDV%' 
				 OR LocalDrugNameWithDose  like 'YYEARCH *midazolam bolus*' OR LocalDrugNameWithDose  like 'mIDAZolam 50MG/D5W 100ML'
				then 'MIDAZOLAM'
			when LocalDrugNameWithDose  like '%DEXMEDETOMIDINE%' OR  LocalDrugNameWithDose  like 'DEXMEDTOMIDINE HCL%' OR  LocalDrugNameWithDose  like 'DEXMEDITOMIDINE 100MCG/ML%' 
			     OR LocalDrugNameWithDose  like 'ZZV-ANZUETO%'  
				then 'DEXMEDETOMIDINE'
			  ELSE 'UNKNOWN'
END AS drug_name
FROM combined_names
GROUP BY drug_name
ORDER BY drug_name;
QUIT;

proc freq data=pharm3 order=freq;
table  drug_name;
run;

/*remove duplicate localdrugsid AND drug_name, so it won't create duplicates when doing left join in below codes*/
proc sort data=pharm3 nodupkey out=pharm3b; 
by localdrugsid  drug_name;
run;

/*left join drug_name field back to dispensed, additive and solution tables, exclude study or unknown drugs*/
/*Dispensed drugs*/
proc sql;
	create table BCMA_BCMADispensedDrug_v5 (compress=yes) as  
	select a.*, b.drug_name
	from BCMA_BCMADispensedDrug_v4 a
	left join pharm3b b on a.localdrugsid =b.localdrugsid;
quit;

/*check if there are any missing drug_name*/
proc freq data=BCMA_BCMADispensedDrug_v5; /*no missing drug_name*/
table drug_name;
run; 

/*delete those with study/test or unknown drugs*/
data BCMA_BCMADispensedDrug_v6 (compress=yes); 
set BCMA_BCMADispensedDrug_v5;
if drug_name in ('STUDY/TEST DRUG','UNKNOWN')  then delete;
keep patienticn patientsid sta3n drug_name ActionDate;
run;

/*do same for IV Additive*/
proc sql;
	create table BCMA_Additive_v5 (compress=yes) as 
	select a.*, b.drug_name
	from BCMA_Additive_v4 a
	left join pharm3b b on a.localdrugsid =b.localdrugsid;
quit;

proc freq data=BCMA_Additive_v5; /*no missing drug_name*/
table drug_name;
run; 

/*delete those with study/test or unknown drugs*/
data BCMA_Additive_v6 (compress=yes); 
set BCMA_Additive_v5;
if drug_name in ('STUDY/TEST DRUG','UNKNOWN')  then delete;
keep patienticn patientsid sta3n drug_name ActionDate;
run;

/*do same for IV Solution*/
proc sql;
	create table BCMA_Solution_v5 (compress=yes) as  
	select a.*, b.drug_name
	from BCMA_Solution_v4 a
	left join pharm3b b on a.localdrugsid =b.localdrugsid;
quit;

proc freq data=BCMA_Solution_v5; /*no missing drug_name*/
table drug_name;
run; 

/*delete those with study/test or unknown drugs*/
data BCMA_Solution_v6 (compress=yes); 
set BCMA_Solution_v5;
if drug_name in ('STUDY/TEST DRUG','UNKNOWN')  then delete;
keep patienticn patientsid sta3n drug_name ActionDate;
run;



/*combine all three together cleaned dispense, additive and solutions tables. 
Then remove duplcates by unique patienticn, sta3n, drug_name and action_date*/
data combined (compress=yes); 
set BCMA_BCMADispensedDrug_v6 BCMA_Additive_v6  BCMA_Solution_v6;
run;

proc sort data=combined nodupkey out=other_drugs_2014_2017;  
by patienticn ActionDate drug_name ;
run;

/************************************************************************************************************************************************/
/*manipulate other_drugs_2014_2017 dataset as desired before left join to VAPD cohort by unique patienticn and date*/
/*transpose dataset if needed*/
DATA trans_all_sedatives_2014_2017 (compress=yes); 
SET other_drugs_2014_2017;
keep patienticn ActionDate drug_name;
RUN;

/*check freq if drug_name field has study/test or unknown drugs*/
proc freq data=trans_all_sedatives_2014_2017;
table drug_name;
run;

PROC TRANSPOSE DATA=trans_all_sedatives_2014_2017 OUT=sepsis.trans_all_sedatives_2014_2017 (DROP=_NAME_)  PREFIX= drugname_ ;
BY patienticn ActionDate;
VAR  drug_name;
RUN;  
