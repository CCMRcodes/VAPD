/******** THIS EXAMPLE SAS CODE INCLUDES LACTATE LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1 SQL CODE. THE GOAL WAS TO 
CREATE A HIGH AND LOW LACTATE VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

/* Date Modified: 9/12/2018
   Author: Shirley Wang */

libname final ''; /*insert file path/directory*/
/*Note, update code to your own study name in step 3: [INSERT STUDY NAME]*/

/*update all macro variables*/
%let studyname= ; /*insert your own study name path in datawarehouse (CDW)*/
%let data_source= " " ;/*insert your own data source*/

/**** DOWNLOAD DFLT TABLE INTO SAS DATASET FROM VINCI ****/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table lactate_all_2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[Lactate2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs by patient, time of specimen and result*/
PROC SORT DATA=lactate_all_2014_2017 nodupkey ; 
BY  PatientSID Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*convert LabChemSpecimenDateTime to LabSpecimenDate*/
data lactate_all_2014_2017;
set lactate_all_2014_2017;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
keep Sta3n LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID Units RefHigh RefLow Topography LabSpecimenDate patienticn;
run;

data lactate_all_2014_2017;
set lactate_all_2014_2017;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units ;
run;

PROC FREQ DATA=lactate_all_2014_2017  order=freq;
TABLE topography  clean_unit;
RUN;

/*change patienticn into numeric*/ 
DATA lactate_all_2014_2017 (rename=patienticn2=patienticn);
SET  lactate_all_2014_2017;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*keep only those with result value >0, blood topography and acceptable unit*/
DATA lactate_all_2014_2017_V2;  
SET  lactate_all_2014_2017;
if topography notin ('PLASMA','ARTERIAL BLOOD','BLOOD','VENOUS BLOOD','SERUM','BLOOD, VENOUS',
'VENOUS BLD','WHOLE BLOOD','BLOOD VENOUS','ARTERIAL BLD','BLOOD, ARTERIAL','PLAS','SER/PLA') 
or clean_unit notin ('MMOL/L','MEQ/L','MMOLE/L','MG/DL','MMOLS/L','MML/L','MMOLES/L', '') 
or LabChemResultNumericValue <0
	then delete;
RUN;

/*look at missing units*/
data missing_units; 
set lactate_all_2014_2017_V2;
if clean_unit ='';
run;
PROC MEANS DATA= missing_units  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*mean: 1.3*/
RUN;

/*look at those with MG/DL unit*/
data mg_dl_units; 
set lactate_all_2014_2017_V2;
if clean_unit ='MG/DL';
run;
PROC MEANS DATA=mg_dl_units  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*median: 13.4*/
RUN;

/*Convert MG/DL to mmol/L  (9 mg/dL=1 mmol/L)*/
/*permissible range: 0-50 mmol/l*/  
data lactate_all_2014_2017_V3 (rename=new_clean_unit=clean_unit rename=new_lab_value=LabChemResultNumericValue);
set lactate_all_2014_2017_V2;
if clean_unit='MG/DL' then new_lab_value=LabChemResultNumericValue/9;
else new_lab_value=LabChemResultNumericValue;
length new_clean_unit $6;
if clean_unit='' then new_clean_unit=''; else new_clean_unit='MMOL/L';
if  new_lab_value>50 then delete;
drop clean_unit LabChemResultNumericValue;
run;

PROC MEANS DATA=lactate_all_2014_2017_V3  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*median: 1.5*/
RUN;

/*create HIGH & LOW values by patient and date*/
PROC SQL;
CREATE TABLE all_lactate_hi_lo_2014_2017  AS   
SELECT *, max(LabChemResultNumericValue) as hi_lactate_daily, min(LabChemResultNumericValue) as lo_lactate_daily
FROM lactate_all_2014_2017_V3
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA=all_lactate_hi_lo_2014_2017  nodupkey out=final.all_lactate_hi_lo_2014_2017; 
BY  patienticn LabSpecimenDate  lo_lactate_daily  hi_lactate_daily;
RUN;

