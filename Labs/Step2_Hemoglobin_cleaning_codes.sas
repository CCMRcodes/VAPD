/******** THIS EXAMPLE SAS CODE INCLUDES HEMOGLOBIN LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1 SQL CODE. THE GOAL WAS TO 
CREATE A HIGH AND LOW HEMOGLOBIN VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

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

create table Hemoglobin_all_2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[Hemoglobin2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

PROC SORT DATA=Hemoglobin_all_2014_2017 nodupkey; 
BY  PatientSID Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*convert LabChemSpecimenDateTime to LabSpecimenDate*/
DATA Hemoglobin_all_2014_2017_V2;
SET Hemoglobin_all_2014_2017;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
keep Sta3n LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID Units RefHigh RefLow Topography LabSpecimenDate patienticn;
RUN;

/*change patienticn into numeric*/   
DATA Hemoglobin_2014_2017_v4 (rename=patienticn2=patienticn);
SET Hemoglobin_all_2014_2017_V2;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*clean up units and create new field: clean_unit*/
DATA Hemoglobin_2014_2017_v4;
SET Hemoglobin_2014_2017_v4;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units;
RUN;

PROC FREQ DATA=Hemoglobin_2014_2017_v4  order=freq;
TABLE topography clean_unit;
RUN;

/*keep only those with blood topography and acceptable clean_unit*/
DATA Hemoglobin_2014_2017_v5;
SET Hemoglobin_2014_2017_v4;
if Topography notin ('PLASMA','SERUM','BLOOD','SER/PLA','VENOUS BLOOD','BLOOD*','BLOOD, VENOUS','ARTERIAL BLD','BLOOD VENOUS',
'VENOUS BLD','BLOOD, ARTERIAL','WS-PLASMA','BLOOD & SERUM','SERUM & BLOOD','ARTERIAL BLOOD','VENOUS BLOOD','WHOLE BLOOD') 
or  clean_unit notin ('G/DL','GM/DL') or LabChemResultNumericValue <0
    then delete;
RUN;

/*check lab value ranges*/
PROC MEANS DATA=Hemoglobin_2014_2017_v5 MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue; 
RUN;

/*create HI & LO values by patient and date*/
PROC SQL;
CREATE TABLE all_Hemoglobin_hi_lo_2014_2017 (compress=yes)  AS
SELECT *, max(LabChemResultNumericValue) as hi_Hemoglobin_daily, min(LabChemResultNumericValue) as lo_Hemoglobin_daily
FROM Hemoglobin_2014_2017_v5
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA= all_Hemoglobin_hi_lo_2014_2017   nodupkey out=final.all_Hemoglobin_hi_lo_2014_2017 ; 
BY  patienticn LabSpecimenDate hi_Hemoglobin_daily lo_Hemoglobin_daily;
RUN;
