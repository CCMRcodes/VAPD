/******** THIS EXAMPLE SAS CODE INCLUDES BICARBONATE LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1. THE GOAL WAS TO 
CREATE A HIGH AND LOW BICARBONATE VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

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

create table bicarb_all_2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[Bicarbonate2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 


/*remove duplicate labs by patient, time of specimen and result*/
PROC SORT DATA=bicarb_all_2014_2017  nodupkey; 
BY PatientSID Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*convert LabChemSpecimenDateTime to LabSpecimenDate*/
DATA bicarb_all_2014_2017_V2;
SET bicarb_all_2014_2017;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
keep Sta3n LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID Units RefHigh RefLow Topography LabSpecimenDate patienticn;
RUN;

/*change patienticn into numeric*/
DATA bicarb_all_2014_2017_v4 (rename=patienticn2=patienticn);
SET bicarb_all_2014_2017_V2;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*clean up units*/
DATA bicarb_all_2014_2017_v4;
SET bicarb_all_2014_2017_v4;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units;
RUN;

PROC FREQ DATA=bicarb_all_2014_2017_v4 order=freq;
TABLE topography clean_unit;
RUN;

/*keep only those with blood topography, acceptable clean_unit */
DATA  bicarb_all_2014_2017_v6; 
SET  bicarb_all_2014_2017_v4;
if LabChemResultNumericValue <0  or Topography notin ('PLASMA','SERUM','BLOOD','ARTERIAL BLOOD','SER/PLA','VENOUS BLOOD','BLOOD*',
'ARTERIAL BLD','BLOOD, VENOUS','BLOOD, ARTERIAL','BLOOD.','BLOOD VENOUS','serum','ART BLOOD',
'VENOUS BLD','BLOOD (VENOUS)','SER/PLAS','BLOOD UNSPECIFIED','BLOOD, MIXED VENOUS','WHOLE BLOOD',
'MIXED VEN/ART BLD','BLOOD,ARTERIAL','ABLD','CENTRAL LINE','BLOOD, PULMONARY ARTERY','WS-PLASMA')
OR  clean_unit notin ('MMOL/L','MEQ/L','MM/L','MMOLE/L','MMOI/L','') then delete; /*mmol/l=meq/l, no unit conversion needed*/
RUN;

/*Check labs with missing units only*/
data missing_unit; 
set bicarb_all_2014_2017_v6;
if clean_unit = '';
run;
PROC MEANS DATA=missing_unit MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue; 
RUN;   /*if they look okay then keep*/

/*Keep only those in permissible range 1-60 mmol/L, N= */
data bicarb_all_2014_2017_v6; 
set bicarb_all_2014_2017_v6;
if LabChemResultNumericValue <1  or LabChemResultNumericValue >60 then delete; 
run;

/*check lab value IQR to make sure they're in the acceptable ranges*/
PROC MEANS DATA=bicarb_all_2014_2017_v6 MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue; 
RUN;

/*create HIGH & LOW values by patient and date*/
PROC SQL;
CREATE TABLE all_bicarb_hi_lo_2014_2017 (compress=yes)  AS   
SELECT *, max(LabChemResultNumericValue) as hi_Bicarb_daily, min(LabChemResultNumericValue) as lo_Bicarb_daily
FROM bicarb_all_2014_2017_v6
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA=all_bicarb_hi_lo_2014_2017  nodupkey out=final.all_bicarb_hi_lo_2014_2017; 
BY  patienticn LabSpecimenDate hi_Bicarb_daily lo_Bicarb_daily;
RUN;
