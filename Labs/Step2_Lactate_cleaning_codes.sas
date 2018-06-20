/******** THIS EXAMPLE SAS CODE INCLUDES LACTATE LOINC CODES AND FACILITY LAB TEST NAMES TO PULLED FROM THE VA CDW. THE GOAL WAS TO 
CREATE A HIGH AND LOW LACTATE VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

libname final ''; /*insert file path/directory*/
/*Note, update code to your own study name in step 3: [INSERT STUDY NAME]*/

/*update all macro variables*/
%let studyname= ; /*insert your own study name path in datawarehouse (CDW)*/
%let data_source= " " ;/*insert your own data source*/

/**** DOWNLOAD DFLT TABLE INTO SAS DATASET FROM VINCE ****/
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

PROC FREQ DATA=lactate_all_2014_2017  order=freq;
TABLE topography  units;
RUN;

/*keep only those with result value >0, blood topography and acceptable unit*/
DATA lactate_all_2014_2017; 
SET  lactate_all_2014_2017;
if topography notin ('ART BLOOD','BLOOD UNSPECIFIED','BLOOD, ARTERIAL','ARTERIAL BLD','BLOOD VENOUS','WHOLE BLOOD','PLASMA','ARTERIAL BLOOD',
'BLOOD','VENOUS BLOOD','SERUM','BLOOD, VENOUS','VENOUS BLD') or units in ('%','MG/DL','mml/L','2.1','6','MMOL/l') or LabChemResultNumericValue <0
	then delete;
RUN;

/*change patienticn into numeric*/ 
DATA all_lactate_2014_2017_V2 (rename=patienticn2=patienticn);
SET  all_lactate_2014_2017;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*create HIGH & LOW values by patient and date*/
PROC SQL;
CREATE TABLE all_lactate_hi_lo_2014_2017  AS   
SELECT *, max(LabChemResultNumericValue) as hi_lactate_daily, min(LabChemResultNumericValue) as lo_lactate_daily
FROM all_lactate_2014_2017_V2
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA=all_lactate_hi_lo_2014_2017  nodupkey out=final.all_lactate_hi_lo_2014_2017; 
BY  patienticn LabSpecimenDate  lo_lactate_daily  hi_lactate_daily;
RUN;

