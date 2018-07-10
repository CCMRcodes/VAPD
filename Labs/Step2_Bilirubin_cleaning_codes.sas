/******** THIS EXAMPLE SAS CODE INCLUDES BILIRUBIN LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1 SQL CODE. THE GOAL WAS TO 
CREATE A HIGH AND LOW BILIRUBIN VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

/* Date Modified: 6/29/2018
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

create table Bilirubin_all_2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[Bilirubin2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs by patient, facility, time of specimen and result*/
PROC SORT DATA=bilirubin_all_2014_2017 nodupkey; 
BY  PatientSID Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*create new date values*/
data bilirubin_all_2014_2017;
set bilirubin_all_2014_2017;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
keep Sta3n LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID Units RefHigh RefLow Topography LabSpecimenDate patienticn;
run;

/*no need to create new variable "clean_units", not that messy*/
PROC FREQ DATA=bilirubin_all_2014_2017  order=freq;
TABLE topography  units;
RUN;

/*keep only those with result value >0, blood topography and acceptable units*/
DATA bilirubin_all_2014_2017_v2; 
SET bilirubin_all_2014_2017;
if Topography notin ('PLASMA','SERUM','BLOOD','SER/PLA','BLOOD*','BLOOD.','serum','SER/PLAS','WS-PLASMA','SERUM 2','PLASMA - SM','BLOOD & SERUM','BLOOD, VENOUS',
'PLASMA & WHOLE BLOOD','WHOLE BLOOD') OR units notin ('mg/dL','mg/dl','MG/DL') or LabChemResultNumericValue <0 or LabChemResultNumericValue >100
	then delete;
RUN;

/*change patienticn into numeric*/  
DATA all_bili_2014_2017_V2 (rename=patienticn2=patienticn);
SET bilirubin_all_2014_2017_v2;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*create HIGH & LOW values by patient and date*/
PROC SQL;
CREATE TABLE all_bili_hi_lo_2014_2017 (compress=yes)  AS   
SELECT *, max(LabChemResultNumericValue) as hi_bili_daily, min(LabChemResultNumericValue) as lo_bili_daily
FROM all_bili_2014_2017_V2
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA=all_bili_hi_lo_2014_2017    nodupkey out=final.all_bili_hi_lo_2014_2017 ; 
BY  patienticn LabSpecimenDate hi_bili_daily lo_bili_daily;
RUN;
