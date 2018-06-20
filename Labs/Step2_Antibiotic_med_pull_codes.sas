/*This is the example code to clean up the antibiotic drugs pulled from step 1 in CDW*/

/*Download the saved dflt. tables into SAS datsets and do further data manipulations*/

%LET Studyname=   ;/*insert study name*/
%Let data_source= " " ;/*inser data source*/

libname sepsis 'INSERT FOLDER PATH';

/*** download dflt tables saved in step 1 into SAS datasets */
/*BCMADispensedDrug_ABX*/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table sepsis.BCMADispensedDrug_ABX (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[BCMADispensedDrug_ABX] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*Additive_ABX*/
PROC SQL ;  
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ); 

create table sepsis.BCMA_Additive_ABX (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[BCMA_Additive_ABX] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*Solution_ABX*/
PROC SQL ;  
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table sepsis.BCMA_Solution_ABX (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[BCMA_Solution_ABX] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*Below are also SAS codes for data management*/
/***** Dispensed Drugs *****/
/*change patienticn into numeric*/ 
DATA BCMA_BCMADispensedDrug_v3 (rename=patienticn2=patienticn); 
SET sepsis.BCMADispensedDrug_ABX;
patienticn2 = input(patienticn, 10.);
drop patienticn;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
Dispensed=1; /*indicator for the dispensed table*/
RUN;

/*remove duplicates*/
PROC SORT DATA=BCMA_BCMADispensedDrug_v3 nodupkey  out=BCMA_BCMADispensedDrug_v4 (compress=yes); /*9980117*/
BY patientsid patienticn  Sta3n ActionDate LocalDrugSID DrugNameWithoutDose LocalDrugNameWithDose NationalDrugNameWithDose
 NationalDrug UnitDoseMedicationRoute BCMADispensedDrugSID BCMAMedicationLogSID;
RUN;


/*************************************************************************************/
/*IV Additive*/
/*change patienticn into numeric*/ 
DATA BCMA_Additive_v3 (rename=patienticn2=patienticn);  
SET  sepsis.BCMA_Additive_ABX ;
patienticn2 = input(patienticn, 10.);
drop patienticn;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
Additive=1; /*indicator for the dispensed table*/
unitdosemedicationroute='IV';
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
SET sepsis.BCMA_Solution_ABX;
patienticn2 = input(patienticn, 10.);
drop patienticn;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
Solution=1; /*indicator for the dispensed table*/
unitdosemedicationroute='IV';
RUN;

/*remove duplicates*/
PROC SORT DATA=BCMA_Solution_v3 nodupkey  out=BCMA_Solution_v4 (compress=yes); 
BY patientsid patienticn  Sta3n ActionDate IVSolutionIngredientSID BCMASolutionSID LocalDrugSID 
BCMAMedicationLogSID LocalDrugNameWithDose IVSolutionFirstIngredientPrintNa;
run;


/*************************************************************************************/
/*only keep certain fields: patienticn, LocalDrugNameWithDose, action date, unitdosemedicationroute*/
data BCMADispensedDrug_names (compress=yes); 
set BCMA_BCMADispensedDrug_v4;
keep patienticn LocalDrugSID  LocalDrugNameWithDose ActionDate unitdosemedicationroute;
run;

data Additive_names (compress=yes); 
set BCMA_Additive_v4;
keep patienticn  LocalDrugSID LocalDrugNameWithDose ActionDate unitdosemedicationroute;
run;

data Solution_names (compress=yes);
set BCMA_Solution_v4;
keep patienticn  LocalDrugSID LocalDrugNameWithDose ActionDate unitdosemedicationroute;
run;

/*combine all three datasets (BCMADispensedDrug_names, Additive_names, & Solution_names) into one dataset*/
data combined_names (compress=yes); /*all 3 datasets have field: LocalDrugNameWithDose*/  
set BCMADispensedDrug_names Additive_names solution_names;
run;

/*delete study/test drugs*/
data combined_names2; 
set combined_names;
if unitdosemedicationroute='BOTH EYES' or unitdosemedicationroute='EACH EYE' or unitdosemedicationroute='EXTERNAL' or 
unitdosemedicationroute='EXTERNALLY' or unitdosemedicationroute='G TUBE' or unitdosemedicationroute='NASAL' or
unitdosemedicationroute='OPHTHALMIC' or unitdosemedicationroute='OPHTHALMIC (BOTH)' or 
unitdosemedicationroute='OPHTHALMIC (DROPS)' or unitdosemedicationroute='OPHTHALMIC (OINT)' or
unitdosemedicationroute='OPHTHALMIC BOTH' or unitdosemedicationroute='OPHTHALMIC TOPICAL' or
unitdosemedicationroute='OPHTHALMIC TOPICAL (BOTH)' or unitdosemedicationroute='OPTHALMIC' or
unitdosemedicationroute='ZZOPHTHALMIC' or unitdosemedicationroute='ZZOPHTHALMIC OINTMENT' or
unitdosemedicationroute='ZZOPHTHALMIC SPACE' or unitdosemedicationroute='ZZOPHTHALMIC TOPICAL' or
unitdosemedicationroute='ZZOPTHALMIC' or unitdosemedicationroute='ZZZOPTHALMIC'or 
index(localdrugnamewithdose, "ACYCLOVIR/HYDROCORTISONE")>0 or 
index(localdrugnamewithdose, "ALLERGENIC EXTRACT,PENICILLIN")>0 or
index(localdrugnamewithdose, "AMOXICILLIN/CLARITHROMYCIN/LANSOPRAZOLE")>0 or 
index(localdrugnamewithdose, "BACITRACIN/HYDROCORTISONE/NEOMYCIN/POLYMYXIN B")>0 or 
index(localdrugnamewithdose, "BACITRACIN/NEOMYCIN/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "BACITRACIN/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "BENZOYL PEROXIDE/CLINDAMYCIN")>0  or 
index(localdrugnamewithdose, "BISMUTH SUBSALICYLATE/METRONIDAZOLE/TETRACYCLINE")>0  or 
index(localdrugnamewithdose, "BISMUTH/METRONIDAZOLE/TETRACYCLINE")>0  or 
index(localdrugnamewithdose, "CIPROFLOXACIN/DEXAMETHASONE")>0 or 
index(localdrugnamewithdose, "CIPROFLOXACIN/HYDROCORTISONE")>0  or 
index(localdrugnamewithdose, "CLINDAMYCIN PHOSPHATE/TRETINOIN")>0  or 
index(localdrugnamewithdose, "COLISTIN/HYDROCORTISONE/NEOMYCIN/THONZONIUM")>0  or
index(localdrugnamewithdose, "DEXAMETHASONE/NEOMYCIN/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "DEXAMETHASONE/TOBRAMYCIN")>0  or 
index(localdrugnamewithdose, "DIPHENHYDRAMINE/HYDROCORTISONE/NYSTATIN/TETRACYCLINE")>0  or
index(localdrugnamewithdose, "ERYTHROMYCIN/SULFISOXAZOLE")>0  or 
index(localdrugnamewithdose, "GENTAMICIN/PREDNISOLONE")>0  or 
index(localdrugnamewithdose, "GRAMICIDIN/NEOMYCIN/POLYMYXIN B")>0  or
index(localdrugnamewithdose, "HYDROCORTISONE/NEOMYCIN/POLYMYXIN B")>0  or
index(localdrugnamewithdose, "LOTEPREDNOL/TOBRAMYCIN")>0  or 
index(localdrugnamewithdose, "NEOMYCIN/POLYMYXIN B")>0 or
index(localdrugnamewithdose, "NEOMYCIN/POLYMYXIN B/PREDNISOLONE")>0  or 
index(localdrugnamewithdose, "OXYTETRACYCLINE")>0  or
index(localdrugnamewithdose, "OXYTETRACYCLINE/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "POLYMYXIN B/TRIMETHOPRIM")>0  or 
index(localdrugnamewithdose, "SILVER SULFADIAZINE")>0  or 
index(localdrugnamewithdose,"STUDY")>0 OR index(localdrugnamewithdose,"UNKNOWN")>0 or
index(localdrugnamewithdose,"TEST")>0 	
	then delete;
run;

data missing_route;
set combined_names2;
if unitdosemedicationroute='*Missing*';
run;

/*indicate those bad from good antibiotic drug names into route=1: IV and route=2: PO*/
data all1   bad ;
set  combined_names2;
if index(unitdosemedicationroute,"IV")>0 or  index(localdrugnamewithdose,"VI")>0 or
index(localdrugnamewithdose,"SOLN")>0 or index(unitdosemedicationroute,"INTRAVENOUS")>0 or
index(localdrugnamewithdose,"IV") > 0 or index(localdrugnamewithdose,"INJ") > 0 or 
index(localdrugnamewithdose,"inj") > 0 or index(localdrugnamewithdose,"premix") > 0 or 
index(localdrugnamewithdose,"PREMIX") > 0 or index(localdrugnamewithdose,"PRE-MIX") > 0 or
index(localdrugnamewithdose,"D5W") > 0 or index(localdrugnamewithdose,"NSS") > 0 or
index(localdrugnamewithdose,"BAG") > 0 or index(localdrugnamewithdose,"CPC") > 0 or
index(localdrugnamewithdose,"vial") > 0 or index(localdrugnamewithdose,"VIAL") > 0 or
index(localdrugnamewithdose,"VL") > 0 or index(localdrugnamewithdose,"SYRINGE") > 0 or
index(localdrugnamewithdose,"SYR") > 0 or index(localdrugnamewithdose,"TUBEX") > 0 or
index(localdrugnamewithdose,"PIGYBK") > 0 or index(localdrugnamewithdose,"PIGGYBACK") > 0 or
index(localdrugnamewithdose,"MINISPIKE") > 0 or index(localdrugnamewithdose,"PMIX") > 0 then route=1;
else if index(unitdosemedicationroute,"ORAL")> 0 or index(unitdosemedicationroute,"PO")> 0 or 
index(localdrugnamewithdose,"TAB") > 0 or index(localdrugnamewithdose,"tab") > 0 or
index(localdrugnamewithdose,"ORAL") > 0 or index(localdrugnamewithdose,"CAP") > 0 or
index(localdrugnamewithdose,"cap") > 0 or index(localdrugnamewithdose,"SUSP") > 0 or
index(localdrugnamewithdose,"susp") > 0 then route=2;

if index(localdrugnamewithdose,"OPH SOLN") > 0 or index(localdrugnamewithdose,"OINT") > 0 
or index(localdrugnamewithdose,"OPHTHALMIC SOLN") > 0 or index(localdrugnamewithdose,"OPHTH SOL") > 0 
or index(localdrugnamewithdose,"GEL") > 0 or index(localdrugnamewithdose,"TOP SOLN") > 0 
or index(localdrugnamewithdose,"TOP. SOLN") > 0 or index(localdrugnamewithdose,"TOP.SOLN") > 0 
or index(localdrugnamewithdose,"TOP SOL") > 0 or index(localdrugnamewithdose,"TOP. SOL.") > 0 
or index(localdrugnamewithdose,"TOPICAL SOL") > 0 or index(localdrugnamewithdose,"TOPICAL SOLN") > 0 or index(localdrugnamewithdose,"TOPICAL SWAB") > 0
or index(localdrugnamewithdose,"TOP PLEDGET") > 0 or index(localdrugnamewithdose,"TOP SWAB") > 0 or index(localdrugnamewithdose,"TOP WIPE") > 0 
or index(localdrugnamewithdose,"SOLN TOP") > 0 or index(localdrugnamewithdose,"SWAB") > 0 
or index(localdrugnamewithdose,"CREAM") > 0 or index(localdrugnamewithdose,"cream") > 0 
or index(localdrugnamewithdose,"SOLN,OPH") > 0 or index(localdrugnamewithdose,"EYE DROP")>0 
or index(localdrugnamewithdose,"EYE SOLN")>0 or index(localdrugnamewithdose,"LOTION")>0 or index(localdrugnamewithdose,"CR,TOP")>0 
or index(localdrugnamewithdose,"CR")>0 or index(localdrugnamewithdose,"CRM,TOP")>0 
or index(localdrugnamewithdose,"CRM")>0 or index(localdrugnamewithdose,"JELLY")>0 
or index(localdrugnamewithdose,"VAG CR")>0 or index(localdrugnamewithdose,"VAGINAL CR")>0 
or index(localdrugnamewithdose,"OPHTH SOLN") > 0 or index(localdrugnamewithdose,"OPTH SOLN") > 0 
or index(localdrugnamewithdose,"OPTH. SOL") > 0 or index(localdrugnamewithdose,"OPHTH") > 0 
or index(localdrugnamewithdose,"OPTHAL DROPS") > 0 or index(localdrugnamewithdose,"SOL,OPH") > 0 
or index(localdrugnamewithdose,"EAR SOLN") > 0 or index(localdrugnamewithdose,"OPH") > 0 
or index(localdrugnamewithdose,"OPH SOL") > 0 or index(localdrugnamewithdose,"OPH/OTIC SOLN") > 0 
or index(localdrugnamewithdose,"OPHT SOLN") > 0 or index(localdrugnamewithdose,"OPHT SOL") > 0 
or index(localdrugnamewithdose,"OTIC SOLN") > 0 or index(localdrugnamewithdose,"otic soln") > 0 
or index(localdrugnamewithdose,"OTIC SOLUTION") > 0 or index(localdrugnamewithdose,"SOLN,OTIC") > 0 
or index(localdrugnamewithdose,"SOLN F/EYE") > 0 or index(localdrugnamewithdose,"OPTHAL SOLN") > 0 or index(localdrugnamewithdose,"EYE SOLN") > 0 
or index(localdrugnamewithdose,"OTIC") > 0 or index(localdrugnamewithdose,"otic") > 0 or index(localdrugnamewithdose,"TOP") > 0 
or index(localdrugnamewithdose,"top") > 0 or index(localdrugnamewithdose,"opth") > 0 
or index(localdrugnamewithdose,"OPTH") > 0 or index(localdrugnamewithdose," OS ") > 0 or index(localdrugnamewithdose," OD ") > 0 
or index(localdrugnamewithdose,"MOUTHWASH") > 0 or index(localdrugnamewithdose,"WATER IRRG") > 0 
or index(localdrugnamewithdose,"INHALATION") > 0or index(localdrugnamewithdose,"INHL SOLN") > 0 
or index(localdrugnamewithdose,"PROSTATE BIOPSY") > 0 then output bad;
else output all1;
run;

PROC FREQ DATA=all1  order=freq;
TABLE  route;
RUN;

proc freq data=missing_route2 order=freq;
table localdrugnamewithdose;
run;


data all2;
set all1;
if index(localdrugnamewithdose,"AMIKACIN")>0 and route=. and route=. then route=1;
else if index(localdrugnamewithdose,"AMPHOTERICIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"AMPHOTERCIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"Amphotericin")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"AMPICIL")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"AMPICILLIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"AZITHROMYCIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"AZTREONAM")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"aztreonam")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CEFAZOLIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"ceFAZolin")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CASPOFUNGIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CEFEPIME")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"cefepime")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CEFOTAXIME")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CEFOTETAN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CEFOXITIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CEFTAROLINE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CEFTAZIDIME")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CEFTRIAXONE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CHLORAMPHENICOL")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CILASTATIN/IMIPENEM")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CIPROFLOXACIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CLINDAMYCIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CefTRIAXone")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CEFTRIAZONE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"COLISTIMETHATE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"COLISTIMETHATE SOD")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"CO-TRIMOXAZOLE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"DAPTOMYCIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"DOXYCYCLINE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"ERTAPENEM")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"FLUCONAZOLE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"FOSCARNET")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"GENTAMICIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"IMIPENEM")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"IMIPEN-CILASTAT")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"IMIPEN-CILISTAT")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"ITRACONAZOLE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"LEVOFLOXACIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"LEVOFOXACIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"LINEZOLID")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"METRONIDAZOLE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"MEROPENEM")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"MICAFUNGIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"MINOCYCLINE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"MOXIFLOXACIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"NAFICILLIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"NAFCILLIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"OXACILLIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"oxacillin")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PENICILLIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PEN-G BENZA")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PEN G BENZA")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PEN G BENZ")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PEN-G POTASSIUM")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PEN-G PROCAINE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"pen-g procaine")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PEN-G SODIUM")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PEN-V POTAS")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PIPERACILLIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"piperacillin")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PIPERACIL/TAZO")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PIPERACILLIN/TAZO")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PIPERCILLIN/TAZO")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PIPERACILL")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PIPRACILLIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"POLYMIXIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"POLYMYXIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"POLYMYXIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"POSACONAZOLE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"PRIMAXIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"QUINUPRISTIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"SMX")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"SMZ")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"STREPTOMYCIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"TICARCILLIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"TOBRAMYCIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"TRIMETH")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"TRIMOXAZOLE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"TIMENTIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"VANCOMYCIN")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"VORICONAZOLE")>0 and route=. then route=1;
else if index(localdrugnamewithdose,"ZOSYN")>0 and route=. then route=1;

else if index(localdrugnamewithdose,"BACTRIM")>0 then route=2;
else if index(localdrugnamewithdose,"AMOX")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"AMOXICILLIN")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"CEFIXIME")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"CEFPODOXIME")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"CEFUROXIME")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"CEPHALEXIN")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"DICLOXACILLIN")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"FOSFOMYCIN")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"NITROFURANTOIN")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"SUFLAMETH")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"TETRACYCLINE")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"SULFADIAZINE")>0 and route=. then route=2;
else if index(localdrugnamewithdose,"SULFAMETH")>0 and route=. then route=2;
if route=. then routeflag=1;
run;

PROC FREQ DATA=all2  order=freq; 
TABLE  route;
RUN;

/*further clean up drug_name into new field named drug*/
data all4;
set all2;
length drug $ 40;
drug_name=upcase(localdrugnamewithdose);
if index(drug_name,'ACYCLOVIR')>0 and route=1 then drug='Acyclovir';
if index(drug_name,'AMIKACIN')>0 and route=1 then drug='Amikacin';
if index(drug_name,'AMPHOTERICIN B')>0 and route=1 then drug='Amphotericin B';
if index(drug_name,'AMPHOTERCIN')>0 and route=1 then drug='Amphotericin B';
if index(drug_name,'AMPHO B')>0 and route=1 then drug='Amphotericin B';
if index(drug_name,'AMPHOTERECIN')>0 and route=1 then drug='Amphotericin B';
if index(drug_name,'AMPHOTERICIN')>0 and route=1 then drug='Amphotericin B';
if index(drug_name,'AMPHOTER')>0 and route=1 then drug='Amphotericin B';
if index(drug_name,'AMPICILLIN')>0 and route=1 then drug='Ampicillin';
if index(drug_name,'AMPICIL')>0 and route=1 then drug='Ampicillin';
if index(drug_name,'AMPICIL')>0 and index(drug_name,'SULB')>0 and route=1 then drug='Ampicillin/Sulbactam';
if index(drug_name,'UNASYN')>0 and route=1 then drug='Ampicillin/Sulbactam';
if index(drug_name,'ANIDULAFUNGIN')>0 and route=1 then drug='Anidulafungin';
if index(drug_name,'ANIDULOFUNGIN')>0 and route=1 then drug='Anidulafungin';
if index(drug_name,'AZITHROMYCIN')>0 and route=1 then drug='Azithromycin';
if index(drug_name,'AZTREONAM')>0 and route=1 then drug='Aztreonam';
if index(drug_name,'AZTREOMAN')>0 and route=1 then drug='Aztreonam';
if index(drug_name,'CASPOFUNGIN')>0 and route=1 then drug='Caspofungin';
if index(drug_name,'CASPOFUNJIN')>0 and route=1 then drug='Caspofungin';
if index(drug_name,'CEFAMANDOLE')>0 and route=1 then drug='Cefamandole';
if index(drug_name,'CEFAZOLIN')>0 and route=1 then drug='Cefazolin';
if index(drug_name,'CEFAZOLN')>0 and route=1 then drug='Cefazolin';
if index(drug_name,'CEFEPIME')>0 and route=1 then drug='Cefepime';
if index(drug_name,'CEFMETAZOLE')>0 and route=1 then drug='Cefmetazole';
if index(drug_name,'CEFONICID')>0 and route=1 then drug='Cefonicid';
if index(drug_name,'CEFOPERAZONE')>0 and route=1 then drug='Cefoperazone';
if index(drug_name,'CEFOTAXIME')>0 and route=1 then drug='Cefotaxime';
if index(drug_name,'CEFOTETAN')>0 and route=1 then drug='Cefotetan';
if index(drug_name,'CEFOXITIN')>0 and route=1 then drug='Cefoxitin';
if index(drug_name,'CEFTAROLINE')>0 and route=1 then drug='Ceftaroline';
if index(drug_name,'CEFTAZIDIME')>0 and route=1 then drug='Ceftazidime';
if index(drug_name,'CEFTAZIDIME')>0 and index(drug_name,'AVIBACTAM')>0 and route=1 then drug='Ceftazidime/Avibactam';
if index(drug_name,'CEFTIZOXIME')>0 and route=1 then drug='Ceftizoxime';
if index(drug_name,'CEFTOLOZANE/TAZOBACTAM')>0 and route=1 then drug='Ceftolozane/Tazobactam';
if index(drug_name,'CEFTOLAZONE/TAZOBACTAM')>0 and route=1 then drug='Ceftolozane/Tazobactam';
if index(drug_name,'CEFTOLOZANE')>0 and route=1 then drug='Ceftolozane/Tazobactam';
if index(drug_name,'CEFTRIAXONE')>0 and route=1 then drug='Ceftriaxone';
if index(drug_name,'CEPHALOTHIN')>0 and route=1 then drug='Cephalothin';
if index(drug_name,'CEPHAPIRIN')>0 and route=1 then drug='Cephapirin';
if index(drug_name,'CHLORAMPHENICOL')>0 and route=1 then drug='Chloramphenicol';
if index(drug_name,'CIDOFOVIR')>0 and route=1 then drug='Cidofovir';
if index(drug_name,'CIPROFLOXACIN')>0 and route=1 then drug='Ciprofloxacin';
if index(drug_name,'CIPRO')>0 and route=1 then drug='Ciprofloxacin';
if index(drug_name,'CLINDAMYCIN')>0 and route=1 then drug='Clindamycin';
if index(drug_name,'CLINDAMY')>0 and route=1 then drug='Clindamycin';
if index(drug_name,'CLOXACILLIN')>0 and route=1 then drug='Cloxacillin';
if index(drug_name,'COLISTIN')>0 and route=1 then drug='Colistin (Colistimethate Sodium)';
if index(drug_name,'COLISTIMETHATE')>0 and route=1 then drug='Colistin (Colistimethate Sodium)';
if index(drug_name,'DALBAVANCIN')>0 and route=1 then drug='Dalbavancin';
if index(drug_name,'DAPTOMYCIN')>0 and route=1 then drug='Daptomycin';
if index(drug_name,'DORIPENEM')>0 and route=1 then drug='Doripenem';
if index(drug_name,'DOXYCYCLINE')>0 and route=1 then drug='Doxycycline';
if index(drug_name,'ERTAPENEM')>0 and route=1 then drug='Ertapenem';
if index(drug_name,'FLUCONAZOLE')>0 and route=1 then drug='Fluconazole';
if index(drug_name,'FOSCARNET')>0 and route=1 then drug='Foscarnet';
if index(drug_name,'GANCICLOVILR')>0 and route=1 then drug='Ganciclovir';
if index(drug_name,'GANCYCLOVIR')>0 and route=1 then drug='Ganciclovir';
if index(drug_name,'GANCICLOVIR')>0 and route=1 then drug='Ganciclovir';
if index(drug_name,'GATIFLOXACIN')>0 and route=1 then drug='Gatifloxacin';
if index(drug_name,'GENTAMICIN')>0 and route=1 then drug='Gentamicin';
if index(drug_name,'GENTAMYCIN')>0 and route=1 then drug='Gentamicin';
if index(drug_name,'IMIPENEM')>0 and route=1 then drug='Imipenem';
if index(drug_name,'IMIPENUM')>0 and route=1 then drug='Imipenem';
if index(drug_name,'IMIPEN-CIL')>0 and route=1 then drug='Imipenem';
if index(drug_name,'ITRACONAZOLE')>0 and route=1 then drug='Itraconazole';
if index(drug_name,'KANAMYCIN')>0 and route=1 then drug='Kanamycin';
if index(drug_name,'LEVOFLOXACIN')>0 and route=1 then drug='Levofloxacin';
if index(drug_name,'LEVOFLOXAC')>0 and route=1 then drug='Levofloxacin';
if index(drug_name,'LEVOFOXACIN')>0 and route=1 then drug='Levofloxacin';
if index(drug_name,'LINCOMYCIN')>0 and route=1 then drug='Lincomycin';
if index(drug_name,'LINEZOLID')>0 and route=1 then drug='Linezolid';
if index(drug_name,'MEROPENEM')>0 and route=1 then drug='Meropenem';
if index(drug_name,'METHICILLIN')>0 and route=1 then drug='Methicillin';
if index(drug_name,'METRONIDAZOLE')>0 and route=1 then drug='Metronidazole';
if index(drug_name,'METRONIDAZ')>0 and route=1 then drug='Metronidazole';
if index(drug_name,'MEZLOCILLIN')>0 and route=1 then drug='Mezlocillin';
if index(drug_name,'MICAFUNGIN')>0 and route=1 then drug='Micafungin';
if index(drug_name,'MINOCYCLINE')>0 and route=1 then drug='Minocycline';
if index(drug_name,'MOXIFLOXACIN')>0 and route=1 then drug='Moxifloxacin';
if index(drug_name,'NAFCILLIN')>0 and route=1 then drug='Nafcillin';
if index(drug_name,'ORITAVANCIN')>0 and route=1 then drug='Oritavancin';
if index(drug_name,'OXACILLIN')>0 and route=1 then drug='Oxacillin';
if index(drug_name,'PENICILLIN')>0 and route=1 then drug='Penicillin';
if index(drug_name,'PEN-G')>0 and route=1 then drug='Penicillin';
if index(drug_name,'PEN G')>0 and route=1 then drug='Penicillin';
if index(drug_name,'PEN-V')>0 and route=1 then drug='Penicillin';
if index(drug_name,'PCN-G')>0 and route=1 then drug='Penicillin';
if index(drug_name,'PEN')>0 and route=1 then drug='Penicillin';
if index(drug_name,'BICILLIN')>0 and route=1 then drug='Penicillin';
if index(drug_name,'WYCILLIN')>0 and route=1 then drug='Penicillin';
if index(drug_name,'PERAMIVIR')>0 and route=1 then drug='Peramivir';
if index(drug_name,'PIPERACILLIN')>0 and route=1 then drug='Piperacillin';
if index(drug_name,'PIPERACIL')>0 and route=1 then drug='Piperacillin';
if index(drug_name,'PIPERCIL')>0 and route=1 then drug='Piperacillin';
if index(drug_name,'PIPERACIL')>0 and index(drug_name,'TAZO')>0 and route=1 then drug='Piperacillin/Tazobactam';
if index(drug_name,'PIPERAC')>0 and index(drug_name,'TAZO')>0 and route=1 then drug='Piperacillin/Tazobactam';
if index(drug_name,'ZOSYN')>0 and route=1 then drug='Piperacillin/Tazobactam';
if index(drug_name,'POLYMYXIN B')>0 and route=1 then drug='Polymyxin B';
if index(drug_name,'POLYMIXIN')>0 and route=1 then drug='Polymyxin B';
if index(drug_name,'POLYMYXIN')>0 and route=1 then drug='Polymyxin B';
if index(drug_name,'POSACONAZOLE')>0 and route=1 then drug='Posaconazole';
if index(drug_name,'PRIMAXIN')>0 and route=1 then drug='Imipenem';
if index(drug_name,'QUINUPRISTIN')>0 and route=1 then drug='Quinupristin/Dalfopristin';
if index(drug_name,'SYNERCID')>0 and route=1 then drug='Quinupristin/Dalfopristin';
if index(drug_name,'DALFOPISTIN')>0 and index(drug_name,'QUINUPRIS')>0 and route=1 then drug='Quinupristin/Dalfopristin';
if index(drug_name,'DALFOPRIS')>0 and index(drug_name,'QUINUPRIS')>0 and route=1 then drug='Quinupristin/Dalfopristin';
if index(drug_name,'STREPTOMYCIN')>0 and route=1 then drug='Streptomycin';
if index(drug_name,'TEDIZOLID')>0 and route=1 then drug='Tedizolid';
if index(drug_name,'TELAVANCIN')>0 and route=1 then drug='Telavancin';
if index(drug_name,'TICARCILLIN')>0 and route=1 then drug='Ticarcillin';
if index(drug_name,'TICARCILLIN/CLAVULANATE')>0 and route=1 then drug='Ticarcillin/Clavulanate';
if index(drug_name,'TICAR')>0 and index(drug_name,'CLAV')>0  and route=1 then drug='Ticarcillin/Clavulanate';
if index(drug_name,'TIMENTIN')>0 and route=1 then drug='Ticarcillin/Clavulanate';
if index(drug_name,'TIGECYCLINE')>0 and route=1 then drug='Tigecycline';
if index(drug_name,'TOBRAMYCIN')>0 and route=1 then drug='Tobramycin';
if index(drug_name,'TRIMOXAZOLE')>0 and route=1 then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'CO-TRIMOXAZOLE')>0 and route=1 then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'SMX')>0 and route=1 then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'SMZ')>0 and route=1 then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'TRIMETH')>0 and route=1 then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'TRIM')>0 and index(drug_name,'SULFA')>0 and route=1 then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'VANCOMYCIN')>0 and route=1 then drug='Vancomycin';
if index(drug_name,'VAMCOMYCIN')>0 and route=1 then drug='Vancomycin';
if index(drug_name,'VANCOMYCN')>0 and route=1 then drug='Vancomycin';
if index(drug_name,'VORICONAZOLE')>0 and route=1 then drug='Voriconazole';

if index(drug_name,'AMOXICILLIN')>0 and route=2 then drug='Amoxicillin';
if index(drug_name,'AMOX')>0 and route=2 then drug='Amoxicillin';
if index(drug_name,'AMOX')>0 and index(drug_name,'CLAV')>0 and route=2 then drug='Amoxicillin/Clavulanate';
if index(drug_name,'AUGMENTIN')>0 and route=2 then drug='Amoxicillin/Clavulanate';
if index(drug_name,'AMPICILLIN')>0 and route=2 then drug='Ampicillin';
if index(drug_name,'AZITHROMYCIN')>0 and route=2 then drug='Azithromycin';
if index(drug_name,'CEFACLOR')>0 and route=2 then drug='Cefaclor';
if index(drug_name,'CEFADROXIL')>0 and route=2 then drug='Cefadroxil';
if index(drug_name,'CEFDINIR')>0 and route=2 then drug='Cefdinir';
if index(drug_name,'CEFDITOREN')>0 and route=2 then drug='Cefditoren';
if index(drug_name,'CEFIXIME')>0 and route=2 then drug='Cefixime';
if index(drug_name,'CEFPODOXIME')>0 and route=2 then drug='Cefpodoxime';
if index(drug_name,'CEFPROZIL')>0 and route=2 then drug='Cefprozil';
if index(drug_name,'CEFTIBUTEN')>0 and route=2 then drug='Ceftibuten';
if index(drug_name,'CEFUROXIME')>0 and route=2 then drug='Cefuroxime';
if index(drug_name,'CEPHALEXIN')>0 and route=2 then drug='Cephalexin';
if index(drug_name,'CEPHALEX')>0 and route=2 then drug='Cephalexin';
if index(drug_name,'CEPHRADINE')>0 and route=2 then drug='Cephradine';
if index(drug_name,'CHLORAMPHENICOL')>0 and route=2 then drug='Chloramphenicol';
if index(drug_name,'CINOXACIN')>0 and route=2 then drug='Cinoxacin';
if index(drug_name,'CIPROFLOXACIN')>0 and route=2 then drug='Ciprofloxacin';
if index(drug_name,'CLINDAMYCIN')>0 and route=2 then drug='Clindamycin';
if index(drug_name,'CLOXACILLIN')>0 and route=2 then drug='Cloxacillin';
if index(drug_name,'DICLOXACILLIN')>0 and route=2 then drug='Dicloxacillin';
if index(drug_name,'DOXYCYCLINE')>0 and route=2 then drug='Doxycycline';
if index(drug_name,'FIDAXOMICIN')>0 and route=2 then drug='Fidaxomicin';
if index(drug_name,'FLUCONAZOLE')>0 and route=2 then drug='Fluconazole';
if index(drug_name,'FOSFOMYCIN')>0 and route=2 then drug='Fosfomycin';
if index(drug_name,'GATIFLOXACIN')>0 and route=2 then drug='Gatifloxacin';
if index(drug_name,'ITRACONAZOLE')>0 and route=2 then drug='Itraconazole';
if index(drug_name,'LEVOFLOXACIN')>0 and route=2 then drug='Levofloxacin';
if index(drug_name,'LINCOMYCIN')>0 and route=2 then drug='Lincomycin';
if index(drug_name,'LINEZOLID')>0 and route=2 then drug='Linezolid';
if index(drug_name,'METRONIDAZOLE')>0 and route=2 then drug='Metronidazole';
if index(drug_name,'MINOCYCLINE')>0 and route=2 then drug='Minocycline';
if index(drug_name,'MOXIFLOXACIN')>0 and route=2 then drug='Moxifloxacin';
if index(drug_name,'NITROFURANTOIN')>0 and route=2 then drug='Nitrofurantoin';
if index(drug_name,'NORFLOXACIN')>0 and route=2 then drug='Norfloxacin';
if index(drug_name,'OFLOXACIN')>0 and route=2 then drug='Ofloxacin';
if index(drug_name,'OSELTAMIVIR')>0 and route=2 then drug='Oseltamivir';
if index(drug_name,'PENICILLIN')>0 and route=2 then drug='Penicillin';
if index(drug_name,'PIVAMPICILLIN')>0 and route=2 then drug='Pivampicillin';
if index(drug_name,'POSACONAZOLE')>0 and route=2 then drug='Posaconazole';
if index(drug_name,'SULFADIAZINE')>0 and route=2 then drug='Sulfadiazine';
if index(drug_name,'SULFADIAZINE')>0 and index(drug_name,'TRIMETH')>0 and route=2 then drug='Sulfadiazine-trimethoprim';
if index(drug_name,'SULFAMETHOXAZOLE')>0 and route=2 then drug='Sulfamethoxazole';
if index(drug_name,'SUFLAMETH')>0 and route=2 then drug='Sulfamethoxazole';
if index(drug_name,'SULFISOXAZOLE')>0 and route=2 then drug='Sulfisoxazole';
if index(drug_name,'TEDIZOLID')>0 and route=2 then drug='Tedizolid';
if index(drug_name,'TELITHROMYCIN')>0 and route=2 then drug='Telithromycin';
if index(drug_name,'TETRACYCLINE')>0 and route=2 then drug='Tetracycline';
if index(drug_name,'TRIMETHOPRIM')>0 and route=2 then drug='Trimethoprim';
if index(drug_name,'TRIMETHOPRIM')>0 and index(drug_name,'SULFA')>0 and route=2 then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'BACTRIM')>0 and route=2 then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'SEPTRA DS')>0 and route=2 then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'SULFA')>0 and index(drug_name,'TMP')>0 and route=2 then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'SULFA')>0 and index(drug_name,'TRIM')>0 and route=2 then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'VANCOMYCIN')>0 and route=2 then drug='Vancomycin';
if index(drug_name,'VORICONAZOLE')>0 and route=2 then drug='Voriconazole';

if index(drug_name,'ACYCLOVIR')>0  then drug='Acyclovir';
if index(drug_name,'AMIKACIN')>0  then drug='Amikacin';
if index(drug_name,'AMPHOTERICIN B')>0  then drug='Amphotericin B';
if index(drug_name,'AMPHOTERCIN')>0  then drug='Amphotericin B';
if index(drug_name,'AMPHO B')>0  then drug='Amphotericin B';
if index(drug_name,'AMPHOTERECIN')>0  then drug='Amphotericin B';
if index(drug_name,'AMPHOTERICIN')>0  then drug='Amphotericin B';
if index(drug_name,'AMPHOTER')>0  then drug='Amphotericin B';
if index(drug_name,'AMPICILLIN')>0  then drug='Ampicillin';
if index(drug_name,'AMPICIL')>0  then drug='Ampicillin';
if index(drug_name,'AMPICIL')>0 and index(drug_name,'SULB')>0  then drug='Ampicillin/Sulbactam';
if index(drug_name,'UNASYN')>0  then drug='Ampicillin/Sulbactam';
if index(drug_name,'ANIDULAFUNGIN')>0  then drug='Anidulafungin';
if index(drug_name,'ANIDULOFUNGIN')>0  then drug='Anidulafungin';
if index(drug_name,'AZITHROMYCIN')>0  then drug='Azithromycin';
if index(drug_name,'AZTREONAM')>0  then drug='Aztreonam';
if index(drug_name,'AZTREOMAN')>0  then drug='Aztreonam';
if index(drug_name,'CASPOFUNGIN')>0  then drug='Caspofungin';
if index(drug_name,'CASPOFUNJIN')>0  then drug='Caspofungin';
if index(drug_name,'CEFAMANDOLE')>0  then drug='Cefamandole';
if index(drug_name,'CEFAZOLIN')>0  then drug='Cefazolin';
if index(drug_name,'CEFAZOLN')>0  then drug='Cefazolin';
if index(drug_name,'CEFEPIME')>0  then drug='Cefepime';
if index(drug_name,'CEFMETAZOLE')>0  then drug='Cefmetazole';
if index(drug_name,'CEFONICID')>0  then drug='Cefonicid';
if index(drug_name,'CEFOPERAZONE')>0  then drug='Cefoperazone';
if index(drug_name,'CEFOTAXIME')>0  then drug='Cefotaxime';
if index(drug_name,'CEFOTETAN')>0  then drug='Cefotetan';
if index(drug_name,'CEFOXITIN')>0  then drug='Cefoxitin';
if index(drug_name,'CEFTAROLINE')>0  then drug='Ceftaroline';
if index(drug_name,'CEFTAZIDIME')>0  then drug='Ceftazidime';
if index(drug_name,'CEFTAZIDIME')>0 and index(drug_name,'AVIBACTAM')>0  then drug='Ceftazidime/Avibactam';
if index(drug_name,'CEFTIZOXIME')>0  then drug='Ceftizoxime';
if index(drug_name,'CEFTOLOZANE/TAZOBACTAM')>0  then drug='Ceftolozane/Tazobactam';
if index(drug_name,'CEFTOLAZONE/TAZOBACTAM')>0  then drug='Ceftolozane/Tazobactam';
if index(drug_name,'CEFTOLOZANE')>0  then drug='Ceftolozane/Tazobactam';
if index(drug_name,'CEFTRIAXONE')>0  then drug='Ceftriaxone';
if index(drug_name,'CEPHALOTHIN')>0  then drug='Cephalothin';
if index(drug_name,'CEPHAPIRIN')>0  then drug='Cephapirin';
if index(drug_name,'CHLORAMPHENICOL')>0  then drug='Chloramphenicol';
if index(drug_name,'CIDOFOVIR')>0  then drug='Cidofovir';
if index(drug_name,'CIPROFLOXACIN')>0  then drug='Ciprofloxacin';
if index(drug_name,'CIPRO')>0  then drug='Ciprofloxacin';
if index(drug_name,'CLINDAMYCIN')>0  then drug='Clindamycin';
if index(drug_name,'CLINDAMY')>0  then drug='Clindamycin';
if index(drug_name,'CLOXACILLIN')>0  then drug='Cloxacillin';
if index(drug_name,'COLISTIN')>0  then drug='Colistin (Colistimethate Sodium)';
if index(drug_name,'COLISTIMETHATE')>0  then drug='Colistin (Colistimethate Sodium)';
if index(drug_name,'DALBAVANCIN')>0  then drug='Dalbavancin';
if index(drug_name,'DAPTOMYCIN')>0  then drug='Daptomycin';
if index(drug_name,'DORIPENEM')>0  then drug='Doripenem';
if index(drug_name,'DOXYCYCLINE')>0  then drug='Doxycycline';
if index(drug_name,'ERTAPENEM')>0  then drug='Ertapenem';
if index(drug_name,'FLUCONAZOLE')>0  then drug='Fluconazole';
if index(drug_name,'FOSCARNET')>0  then drug='Foscarnet';
if index(drug_name,'GANCICLOVILR')>0  then drug='Ganciclovir';
if index(drug_name,'GANCYCLOVIR')>0  then drug='Ganciclovir';
if index(drug_name,'GANCICLOVIR')>0  then drug='Ganciclovir';
if index(drug_name,'GATIFLOXACIN')>0  then drug='Gatifloxacin';
if index(drug_name,'GENTAMICIN')>0  then drug='Gentamicin';
if index(drug_name,'GENTAMYCIN')>0  then drug='Gentamicin';
if index(drug_name,'IMIPENEM')>0  then drug='Imipenem';
if index(drug_name,'IMIPENUM')>0  then drug='Imipenem';
if index(drug_name,'IMIPEN-CIL')>0  then drug='Imipenem';
if index(drug_name,'ITRACONAZOLE')>0  then drug='Itraconazole';
if index(drug_name,'KANAMYCIN')>0  then drug='Kanamycin';
if index(drug_name,'LEVOFLOXACIN')>0  then drug='Levofloxacin';
if index(drug_name,'LEVOFLOXAC')>0  then drug='Levofloxacin';
if index(drug_name,'LEVOFOXACIN')>0  then drug='Levofloxacin';
if index(drug_name,'LINCOMYCIN')>0  then drug='Lincomycin';
if index(drug_name,'LINEZOLID')>0  then drug='Linezolid';
if index(drug_name,'MEROPENEM')>0  then drug='Meropenem';
if index(drug_name,'METHICILLIN')>0  then drug='Methicillin';
if index(drug_name,'METRONIDAZOLE')>0  then drug='Metronidazole';
if index(drug_name,'METRONIDAZ')>0  then drug='Metronidazole';
if index(drug_name,'MEZLOCILLIN')>0  then drug='Mezlocillin';
if index(drug_name,'MICAFUNGIN')>0  then drug='Micafungin';
if index(drug_name,'MINOCYCLINE')>0  then drug='Minocycline';
if index(drug_name,'MOXIFLOXACIN')>0  then drug='Moxifloxacin';
if index(drug_name,'NAFCILLIN')>0  then drug='Nafcillin';
if index(drug_name,'ORITAVANCIN')>0  then drug='Oritavancin';
if index(drug_name,'OXACILLIN')>0  then drug='Oxacillin';
if index(drug_name,'PENICILLIN')>0  then drug='Penicillin';
if index(drug_name,'PEN-G')>0  then drug='Penicillin';
if index(drug_name,'PEN G')>0  then drug='Penicillin';
if index(drug_name,'PEN-V')>0  then drug='Penicillin';
if index(drug_name,'PCN-G')>0  then drug='Penicillin';
if index(drug_name,'PEN')>0  then drug='Penicillin';
if index(drug_name,'BICILLIN')>0  then drug='Penicillin';
if index(drug_name,'WYCILLIN')>0  then drug='Penicillin';
if index(drug_name,'PERAMIVIR')>0  then drug='Peramivir';
if index(drug_name,'PIPERACILLIN')>0  then drug='Piperacillin';
if index(drug_name,'PIPERACIL')>0  then drug='Piperacillin';
if index(drug_name,'PIPERCIL')>0  then drug='Piperacillin';
if index(drug_name,'PIPERACIL')>0 and index(drug_name,'TAZO')>0  then drug='Piperacillin/Tazobactam';
if index(drug_name,'PIPERAC')>0 and index(drug_name,'TAZO')>0  then drug='Piperacillin/Tazobactam';
if index(drug_name,'ZOSYN')>0  then drug='Piperacillin/Tazobactam';
if index(drug_name,'POLYMYXIN B')>0  then drug='Polymyxin B';
if index(drug_name,'POLYMIXIN')>0  then drug='Polymyxin B';
if index(drug_name,'POLYMYXIN')>0  then drug='Polymyxin B';
if index(drug_name,'POSACONAZOLE')>0  then drug='Posaconazole';
if index(drug_name,'PRIMAXIN')>0  then drug='Imipenem';
if index(drug_name,'QUINUPRISTIN')>0  then drug='Quinupristin/Dalfopristin';
if index(drug_name,'SYNERCID')>0  then drug='Quinupristin/Dalfopristin';
if index(drug_name,'DALFOPISTIN')>0 and index(drug_name,'QUINUPRIS')>0  then drug='Quinupristin/Dalfopristin';
if index(drug_name,'DALFOPRIS')>0 and index(drug_name,'QUINUPRIS')>0  then drug='Quinupristin/Dalfopristin';
if index(drug_name,'STREPTOMYCIN')>0  then drug='Streptomycin';
if index(drug_name,'TEDIZOLID')>0  then drug='Tedizolid';
if index(drug_name,'TELAVANCIN')>0  then drug='Telavancin';
if index(drug_name,'TICARCILLIN')>0  then drug='Ticarcillin';
if index(drug_name,'TICARCILLIN/CLAVULANATE')>0  then drug='Ticarcillin/Clavulanate';
if index(drug_name,'TICAR')>0 and index(drug_name,'CLAV')>0   then drug='Ticarcillin/Clavulanate';
if index(drug_name,'TIMENTIN')>0  then drug='Ticarcillin/Clavulanate';
if index(drug_name,'TIGECYCLINE')>0  then drug='Tigecycline';
if index(drug_name,'TOBRAMYCIN')>0  then drug='Tobramycin';
if index(drug_name,'TRIMOXAZOLE')>0  then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'CO-TRIMOXAZOLE')>0  then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'SMX')>0  then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'SMZ')>0  then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'TRIMETH')>0  then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'TRIM')>0 and index(drug_name,'SULFA')>0  then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'VANCOMYCIN')>0  then drug='Vancomycin';
if index(drug_name,'VAMCOMYCIN')>0  then drug='Vancomycin';
if index(drug_name,'VANCOMYCN')>0  then drug='Vancomycin';
if index(drug_name,'VORICONAZOLE')>0  then drug='Voriconazole';

if index(drug_name,'AMOXICILLIN')>0   then drug='Amoxicillin';
if index(drug_name,'AMOX')>0   then drug='Amoxicillin';
if index(drug_name,'AMOX')>0 and index(drug_name,'CLAV')>0   then drug='Amoxicillin/Clavulanate';
if index(drug_name,'AUGMENTIN')>0   then drug='Amoxicillin/Clavulanate';
if index(drug_name,'AMPICILLIN')>0   then drug='Ampicillin';
if index(drug_name,'AZITHROMYCIN')>0   then drug='Azithromycin';
if index(drug_name,'CEFACLOR')>0   then drug='Cefaclor';
if index(drug_name,'CEFADROXIL')>0   then drug='Cefadroxil';
if index(drug_name,'CEFDINIR')>0   then drug='Cefdinir';
if index(drug_name,'CEFDITOREN')>0   then drug='Cefditoren';
if index(drug_name,'CEFIXIME')>0   then drug='Cefixime';
if index(drug_name,'CEFPODOXIME')>0   then drug='Cefpodoxime';
if index(drug_name,'CEFPROZIL')>0   then drug='Cefprozil';
if index(drug_name,'CEFTIBUTEN')>0   then drug='Ceftibuten';
if index(drug_name,'CEFUROXIME')>0   then drug='Cefuroxime';
if index(drug_name,'CEPHALEXIN')>0   then drug='Cephalexin';
if index(drug_name,'CEPHALEX')>0   then drug='Cephalexin';
if index(drug_name,'CEPHRADINE')>0   then drug='Cephradine';
if index(drug_name,'CHLORAMPHENICOL')>0   then drug='Chloramphenicol';
if index(drug_name,'CINOXACIN')>0   then drug='Cinoxacin';
if index(drug_name,'CIPROFLOXACIN')>0   then drug='Ciprofloxacin';
if index(drug_name,'CLINDAMYCIN')>0   then drug='Clindamycin';
if index(drug_name,'CLOXACILLIN')>0   then drug='Cloxacillin';
if index(drug_name,'DICLOXACILLIN')>0   then drug='Dicloxacillin';
if index(drug_name,'DOXYCYCLINE')>0   then drug='Doxycycline';
if index(drug_name,'FIDAXOMICIN')>0   then drug='Fidaxomicin';
if index(drug_name,'FLUCONAZOLE')>0   then drug='Fluconazole';
if index(drug_name,'FOSFOMYCIN')>0   then drug='Fosfomycin';
if index(drug_name,'GATIFLOXACIN')>0   then drug='Gatifloxacin';
if index(drug_name,'ITRACONAZOLE')>0   then drug='Itraconazole';
if index(drug_name,'LEVOFLOXACIN')>0   then drug='Levofloxacin';
if index(drug_name,'LINCOMYCIN')>0   then drug='Lincomycin';
if index(drug_name,'LINEZOLID')>0   then drug='Linezolid';
if index(drug_name,'METRONIDAZOLE')>0   then drug='Metronidazole';
if index(drug_name,'MINOCYCLINE')>0   then drug='Minocycline';
if index(drug_name,'MOXIFLOXACIN')>0   then drug='Moxifloxacin';
if index(drug_name,'NITROFURANTOIN')>0   then drug='Nitrofurantoin';
if index(drug_name,'NORFLOXACIN')>0   then drug='Norfloxacin';
if index(drug_name,'OFLOXACIN')>0   then drug='Ofloxacin';
if index(drug_name,'OSELTAMIVIR')>0   then drug='Oseltamivir';
if index(drug_name,'PENICILLIN')>0   then drug='Penicillin';
if index(drug_name,'PIVAMPICILLIN')>0   then drug='Pivampicillin';
if index(drug_name,'POSACONAZOLE')>0   then drug='Posaconazole';
if index(drug_name,'SULFADIAZINE')>0   then drug='Sulfadiazine';
if index(drug_name,'SULFADIAZINE')>0 and index(drug_name,'TRIMETH')>0   then drug='Sulfadiazine-trimethoprim';
if index(drug_name,'SULFAMETHOXAZOLE')>0   then drug='Sulfamethoxazole';
if index(drug_name,'SUFLAMETH')>0   then drug='Sulfamethoxazole';
if index(drug_name,'SULFISOXAZOLE')>0   then drug='Sulfisoxazole';
if index(drug_name,'TEDIZOLID')>0   then drug='Tedizolid';
if index(drug_name,'TELITHROMYCIN')>0   then drug='Telithromycin';
if index(drug_name,'TETRACYCLINE')>0   then drug='Tetracycline';
if index(drug_name,'TRIMETHOPRIM')>0   then drug='Trimethoprim';
if index(drug_name,'TRIMETHOPRIM')>0 and index(drug_name,'SULFA')>0   then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'BACTRIM')>0   then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'SEPTRA DS')>0   then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'SULFA')>0 and index(drug_name,'TMP')>0   then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'SULFA')>0 and index(drug_name,'TRIM')>0   then drug='Trimethoprim/Sulfamethoxazole';
if index(drug_name,'VANCOMYCIN')>0   then drug='Vancomycin';
if index(drug_name,'VORICONAZOLE')>0   then drug='Voriconazole';
run;

PROC FREQ DATA=all4  order=freq;
table drug_name/missing;
RUN;

PROC FREQ DATA=all4  order=freq;
table drug/missing;  
RUN;

data missing_drug;
set all4;
if drug = '';
run;

PROC FREQ DATA=missing_drug  order=freq;
table drug_name / out=freq;
RUN;


/*keep routes IV or PO field*/
DATA  all_abx_2014_2017;
SET  all4;
if  route=2 then route2="PO";
else route2="IV";
keep patienticn  actiondate drug route2;
RUN;

/*remove duplicates*/
PROC SORT DATA= all_abx_2014_2017  out=all_abx2 nodupkey; 
BY  patienticn  actiondate drug route2;
RUN;

PROC FREQ DATA=all_abx2  order=freq;
TABLE route2;
RUN;


/*combine drug_name and route*/
data all_abx3 (compress=yes drop=Underscore);  
set all_abx2;
if drug='' then delete;
Underscore='_';
if route2 NE '' then do;
med_route=cats(drug,Underscore,route2); end; /*new variable created*/
run;

/*group med_route into different antibiotic class*/
DATA  all_abx_class; 
SET  all_abx3;
if med_route in ('Penicillin_IV', 'Amoxicillin_PO', 'Amoxicillin/Clavulanate_PO', 'Amoxicillin/Clavulanate_IV', 
'Ticarcillin/Clavulanate_IV', 'Ampicillin/Sulbactam_IV', 'Ampicillin_IV', 'Ampicillin_PO', 'Nafcillin_IV', 'Piperacillin_IV', 
'Penicillin_PO', 'Dicloxacillin_IV', 'Dicloxacillin_PO', 'Oxacillin_IV', 'Amoxicillin_IV') then class=1; 
else if med_route = 'Piperacillin/Tazobactam_IV' then  class=2; 
else if med_route in ('Cefazolin_IV','Cephalexin_PO','Cefadroxil_PO', 'Cephalexin_IV') then class=3;
else if med_route in ('Cefoxitin_IV', 'Cefuroxime_IV', 'Cefuroxime_PO', 'Cefaclor_PO', 'Cefprozil_PO', 'Cefotetan_IV') then class=4;
else if med_route in ('Cefixime_PO', 'Ceftibuten_PO', 'Ceftriaxone_IV', 'Ceftazidime_IV', 'Cefdinir_PO', 'Cefotaxime_IV', 
'Ceftazidime/Avibactam_IV', 'Cefpodoxime_PO' ) then class=5;
else if med_route ='Cefepime_IV' then class=6;
else if med_route in ('Ofloxacin_PO', 'Ofloxacin_IV', 'Ciprofloxacin_IV','Levofloxacin_IV', 
'Moxifloxacin_PO', 'Moxifloxacin_IV', 'Norfloxacin_PO') then class=7;
else if med_route in ('Telavancin_IV', 'Dalbavancin_IV', 'Oritavancin_IV', 'Vancomycin_IV') then class=8;
else if med_route in ('Vancomycin_PO','Fidaxomicin_PO','Fidaxomicin_IV') then class=9;
else if med_route in ('Acyclovir_IV', 'Acyclovir_PO', 'Peramivir_IV', 'Ganciclovir_PO', 'Foscarnet_IV', 'Ganciclovir_IV') then class=10;
else if med_route in ('Azithromycin_PO','Azithromycin_IV') then class=11;
else if med_route in ('Metronidazole_PO','Metronidazole_IV') then class=12;
else if med_route in ('Trimethoprim/Sulfamethoxazole_PO', 'Sulfamethoxazole_IV', 'Sulfadiazine_PO', 'Trimethoprim_PO', 'Trimethoprim/Sulfamethoxazole_IV', 'Tetracycline_PO') then class=13;
else if med_route in ('Fluconazole_PO', 'Fluconazole_IV', 'Micafungin_IV', 'Voriconazole_PO', 'Voriconazole_IV', 'Posaconazole_IV', 
'Posaconazole_PO', 'Itraconazole_IV','Itraconazole_PO', 'Amphotericin B_IV', 'Amphotericin B_PO', 'Caspofungin_IV', 'Anidulafungin_IV') then class=14;
else if med_route='Aztreonam_IV' then class=15;
else if med_route in ('Clindamycin_IV','Clindamycin_PO' ) then class=16;
else if med_route in ('Daptomycin_IV', 'Tigecycline_IV', 'Linezolid_IV', 'Linezolid_PO', 'Ceftaroline_IV', 'Tedizolid_PO', 'Tedizolid_IV', 
'Colistin (Colistimethate Sodium)_IV', 'Colistin (Colistimethate Sodium)_PO', 'Polymyxin B_IV', 'Ceftaroline_IV', 'Ceftolozane/Tazobactam_IV', 'Quinupristin/Dalfopristin_IV') then class=17;
else if med_route in ('Gentamicin_IV', 'Amikacin_IV', 'Streptomycin_IV', 'Tobramycin_PO', 'Tobramycin_IV' ) then class=18;
else if med_route in ('Doxycycline_PO', 'Doxycycline_IV', 'Minocycline_PO', 'Minocycline_IV') then class=19;
else if med_route in ('Nitrofurantoin_PO', 'Fosfomycin_PO') then class=20;
    else class=.; /*other/missing*/
RUN;

/*remove duplicate abx*/
PROC SORT DATA=all_abx_class out=sepsis.all_abx_06012018  nodupkey; 
BY patienticn  actiondate med_route;
RUN;

/*remove duplicate classes*/
PROC SORT DATA=all_abx_class out=sepsis.all_abx_class_06012018 (keep= patienticn actiondate class) nodupkey; 
BY patienticn  actiondate class;
RUN;

DATA all_abx_class_V3;
SET sepsis.all_abx_class_06012018;
if class=1 then abx1a=1; else abx1a=0;
if class=2 then abx2a=1; else abx2a=0;
if class=3 then abx3a=1; else abx3a=0;
if class=4 then abx4a=1; else abx4a=0;
if class=5 then abx5a=1; else abx5a=0;
if class=6 then abx6a=1; else abx6a=0;
if class=7 then abx7a=1; else abx7a=0;
if class=8 then abx8a=1; else abx8a=0;
if class=9 then abx9a=1; else abx9a=0;
if class=10 then abx10a=1; else abx10a=0;
if class=11 then abx11a=1; else abx11a=0;
if class=12 then abx12a=1; else abx12a=0;
if class=13 then abx13a=1; else abx13a=0;
if class=14 then abx14a=1; else abx14a=0;
if class=15 then abx15a=1; else abx15a=0;
if class=16 then abx16a=1; else abx16a=0;
if class=17 then abx17a=1; else abx17a=0;
if class=18 then abx18a=1; else abx18a=0;
if class=19 then abx19a=1; else abx19a=0;
if class=20 then abx20a=1; else abx20a=0;
RUN;

/*create a single row with all abx by patient and actiondate*/
PROC SQL;
CREATE TABLE all_abx_class_V4  AS 
SELECT *, sum(abx1a) as abx1, sum(abx2a) as abx2,sum(abx3a) as abx3,sum(abx4a) as abx4,sum(abx5a) as abx5,sum(abx6a) as abx6,
sum(abx7a) as abx7,sum(abx8a) as abx8,sum(abx9a) as abx9,sum(abx10a) as abx10,sum(abx11a) as abx11,sum(abx12a) as abx12,
sum(abx13a) as abx13,sum(abx14a) as abx14,sum(abx15a) as abx15,sum(abx16a) as abx16,sum(abx17a) as abx17,sum(abx18a) as abx18,
sum(abx19a) as abx19,sum(abx20a) as abx20
FROM all_abx_class_V3
GROUP BY  patienticn,actiondate
ORDER BY patienticn, actiondate;
QUIT;

/*remove duplicate patient and actiondate*/
PROC SORT DATA=all_abx_class_V4 nodupkey OUT=sepsis.all_abx_num_06012018 (keep=patienticn actiondate abx1 - abx20);
BY patienticn actiondate;
RUN;

/*****************************************************************************/
/*transpose med_route into patient-days*/
PROC TRANSPOSE DATA=all_abx3 out=sepsis.trans_all_medroute_2014_2017 (drop=_NAME_ )   PREFIX=abx_  ; 
BY PatientICN actiondate;
VAR med_route;
RUN;


