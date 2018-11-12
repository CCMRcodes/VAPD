/******** THIS EXAMPLE SAS CODE INCLUDES PLATELET LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1 SQL CODE. THE GOAL WAS TO 
CREATE A HIGH AND LOW PLATELET VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

/* Date Modified: 11/12/2018
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

create table platelet_all_2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[Platelets2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs by patient, facility, time of specimen and result*/
PROC SORT DATA=platelet_all_2014_2017 nodupkey; 
BY PatientSID  Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*create new year and datevalue variable*/
data platelet_all_2014_2017;
set platelet_all_2014_2017;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
keep Sta3n LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID Units RefHigh RefLow Topography LabSpecimenDate patienticn;
run;


data platelet_all_2014_2017;
set platelet_all_2014_2017;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units ;
run;

/*change patienticn into numeric*/  
DATA platelet_all_2014_2017 (rename=patienticn2=patienticn);
SET platelet_all_2014_2017;
patienticn2 = input(patienticn, 10.);
year=year(LabSpecimenDate);
drop patienticn;
RUN;

PROC FREQ DATA=platelet_all_2014_2017  order=freq;
TABLE topography  clean_unit;
RUN;

/*keep only those with result value >0, blood topography and acceptable unit*/
DATA platelet_all_2014_2017_v2; 
SET platelet_all_2014_2017;
if topography notin ('BLOOD','WHOLE BLOOD','PLASMA','BLOOD,CAPILLARY','SERUM','WS-BLOOD','BLOOD - SM',
'ARTERIAL BLOOD','BLOOD, VENOUS','PLASMA+SERUM','SER/PLA','SERUM/BLOOD') or clean_unit notin ('K/CMM','K/UL','K/MM3','10*3/UL',
'10E3/UL','X1000/UL','10E9/L','X10-3','K/MCL','X10-3/UL','K/CUMM','BILL/L','THOUS/CMM','10(3)/MCL','THOU/CUMM','1000/UL',
'THOU/UL','T/CMM','1.00E+04','X103','K/CCM','10X3/CMM','103/UL','K/MICROL','X10E3/UL','X(10)3',"10'3/UL",'X1000','10X3/UL',
'THOU','TH/MM3','1000/MCL','TH/UL','THOUS/UL','X10(9)/L','10*9/L','X10(3)/UL','K/CM','10*3UL','1000/MM3','','10**3') 
or LabChemResultNumericValue <0
	then delete;
RUN;

/*look at missing units*/
data missing_units; 
set platelet_all_2014_2017_v2;
if clean_unit ='';
run;
PROC MEANS DATA= missing_units  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*median: 1*/
RUN;
proc freq data=missing_units order=freq; /*60% from 674, 30% from 590*/
table sta3n;
run;

/*exclude these missing unit labs*/
data platelet_all_2014_2017_v3; 
set platelet_all_2014_2017_v2;
if clean_unit ='' then delete;
run;

/*Only keep those labs within permissible range 1-1500 (1 10^9/L=1000/MCL, no conversions needed) */
data platelet_all_2014_2017_v3; 
set platelet_all_2014_2017_v3;
if LabChemResultNumericValue <1 or LabChemResultNumericValue>1500 then delete;
run;
PROC MEANS DATA= platelet_all_2014_2017_v3  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; 
RUN;

/*create HIGH & LOW values by patient and date*/
PROC SQL;
CREATE TABLE all_plate_hi_lo_2014_2017  AS   
SELECT *, max(LabChemResultNumericValue) as hi_plate_daily, min(LabChemResultNumericValue) as lo_plate_daily
FROM  platelet_all_2014_2017_v3
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA= all_plate_hi_lo_2014_2017   nodupkey out=final.all_plate_hi_lo_2014_2017; 
BY  patienticn LabSpecimenDate  lo_plate_daily  hi_plate_daily;
RUN;

