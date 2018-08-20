/******** THIS EXAMPLE SAS CODE INCLUDES PLATELET LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1 SQL CODE. THE GOAL WAS TO 
CREATE A HIGH AND LOW PLATELET VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

/* Date Modified: 8/20/2018
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

/*creat new year and datevalue variable*/
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

PROC FREQ DATA=platelet_all_2014_2017  order=freq;
TABLE topography  clean_unit;
RUN;

/*keep only those with result value >0, blood topography and acceptable unit*/
DATA platelet_all_2014_2017_v2; /*25804823*/
SET platelet_all_2014_2017;
if topography notin ('BLOOD','WHOLE BLOOD','PLASMA','SERUM','WS-BLOOD','PLATELET RICH PLASMA','ARTERIAL BLOOD','BLOOD, VENOUS','PLASMA (QUINCY)','PLASMA+SERUM',
'SER/PLA','SERUM/BLOOD') or clean_unit in ('%','&','"K/uL"','"x10E3/uL"','130-400','K/iL','K/uL.','K/u/','CONC','FL','#','&','CUMM','ADEQ') or LabChemResultNumericValue <0
	then delete;
RUN;

PROC FREQ DATA=platelet_all_2014_2017_v2  order=freq;
TABLE units;
RUN;

/*change patienticn into numeric*/  
DATA all_plate_2014_2017 (rename=patienticn2=patienticn);
SET platelet_all_2014_2017_v2 ;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*create HIGH & LOW values by patient and date*/
PROC SQL;
CREATE TABLE all_plate_hi_lo_2014_2017  AS   
SELECT *, max(LabChemResultNumericValue) as hi_plate_daily, min(LabChemResultNumericValue) as lo_plate_daily
FROM all_plate_2014_2017 
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA= all_plate_hi_lo_2014_2017   nodupkey out=final.all_plate_hi_lo_2014_2017; 
BY  patienticn LabSpecimenDate  lo_plate_daily  hi_plate_daily;
RUN;

