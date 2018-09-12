/******** THIS EXAMPLE SAS CODE INCLUDES CREATININE LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1 SQL CODE. THE GOAL WAS TO 
CREATE A HIGH AND LOW CREATININE VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

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

create table creatinine_all_2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[Creatinine2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs by patient, time of specimen and result*/
PROC SORT DATA=creatinine_all_2014_2017 nodupkey; 
BY PatientSID  Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*convert LabChemSpecimenDateTime to LabSpecimenDate*/
data creatinine_all_2014_2017; 
set creatinine_all_2014_2017;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
keep Sta3n LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID Units RefHigh RefLow Topography LabSpecimenDate  patienticn;
run;

/*create clean unit*/
data creatinine_all_2014_2017;
set creatinine_all_2014_2017;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop  units2 units3 units;
run;

/*change patienticn into numeric*/
DATA  creatinine_all_2014_2017 (rename=patienticn2=patienticn);
SET  creatinine_all_2014_2017;
patienticn2 = input(patienticn, 10.);
year=year(LabSpecimenDate);
drop patienticn;
RUN;

PROC FREQ DATA=creatinine_all_2014_2017  order=freq;
TABLE topography  clean_unit;
RUN;

/*look at frequency and delete labs that are non-blood topography and with incorrect units*/
DATA creatinine_all_2014_2017_V2; 
SET  creatinine_all_2014_2017;
if topography notin ('PLASMA','SERUM','BLOOD','SER/PLA','BLOOD*','VENOUS BLOOD','ARTERIAL BLOOD','BLOOD, VENOUS',
'BLOOD.','VENOUS BLD','BLOOD VENOUS','serum','SER/PLAS','PLAS','WS-PLASMA','WHOLE BLOOD') 
OR  units notin ('MG/DL','MG?DL','MGDL','MG\DL','') 
or LabChemResultNumericValue <0 
	then delete;
RUN;

/*check missing units*/
data missing_units; 
set creatinine_all_2014_2017_V2;
if clean_unit = '';
run;

PROC MEANS DATA= missing_units  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*mean: 7.4272795, makes sense, will keep them*/
RUN;

/*permissible range 0.1-28.3 mg/dL*/
data creatinine_all_2014_2017_V3; 
set creatinine_all_2014_2017_V2;
if LabChemResultNumericValue <0.1 or LabChemResultNumericValue>28.3 then delete;
run;

PROC MEANS DATA= creatinine_all_2014_2017_V3  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; 
RUN;

/*create HIGH & LOW values by patient and date*/
PROC SQL;
CREATE TABLE all_creat_hi_lo_2014_2017   AS  
SELECT *, max(LabChemResultNumericValue) as hi_creat_daily, min(LabChemResultNumericValue) as lo_creat_daily
FROM creatinine_all_2014_2017_V3
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA=all_creat_hi_lo_2014_2017  nodupkey out=final.all_creat_hi_lo_2014_2017 ; 
BY  patienticn LabSpecimenDate  lo_creat_daily  hi_creat_daily;
RUN;

