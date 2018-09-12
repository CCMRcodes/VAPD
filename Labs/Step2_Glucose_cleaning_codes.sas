/******** THIS EXAMPLE SAS/SQL CODE INCLUDES GLUCOSE LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1 SQL CODE. THE GOAL WAS TO 
CREATE A HIGH AND LOW GLUCOSE VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

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

create table Glucose_2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[Glucose2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs by patient, facility, time of specimen and result*/
proc sort data=Glucose_2014_2017 nodupkey; 
by PatientSID Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
run;

/*change patienticn into numeric, clean up units and create new labspecimen date variable*/   
DATA glucose_2014_2017_v4 ( compress=yes rename=patienticn2=patienticn);
SET Glucose_2014_2017;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
patienticn2 = input(patienticn, 10.);
drop patienticn units2 units3 units;
RUN;

/*check units and topography*/
PROC FREQ DATA=glucose_2014_2017_v4  order=freq;
TABLE topography clean_unit;
RUN;

/*look at FINGER STICK (GLUCOSE) topography, is it only few sta3ns and what's value range*/
data finger_stick; 
set glucose_2014_2017_v4;
if Topography = 'FINGER STICK (GLUCOSE)';
run;

proc freq data=finger_stick order=freq;
table Sta3n; /*only from sta3n: 619*/
run;
PROC MEANS DATA=finger_stick MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue; /*median=160, keep*/
RUN;


/*keep only those with result value >0, blood topography and acceptable clean_unit*/
DATA glucose_2014_2017_v5 (compress=yes); 
SET glucose_2014_2017_v4;
if Topography notin ('BLOOD','PLASMA','SERUM','CAPILLARY','BLOOD,CAPILLARY','CAPILLARY BLOOD','WHOLE BLOOD',
'FINGER STICK (BLOOD)','ARTERIAL BLOOD','FINGERSTICK','SER/PLA','FINGER STICK (GLUCOSE)','BLOOD..',
'BLOOD*','BLOOD CAPILLARY','VENOUS BLOOD','BLOOD, VENOUS','ARTERIAL BLD','BLOOD.','BLOOD VENOUS',
'VENOUS BLD','serum','BLOOD, ARTERIAL','SER/PLAS') or  clean_unit notin ('MG/DL','MMOL/L','')
or (LabChemResultNumericValue <0)  then delete;
RUN;

/*check missing labs*/
data missing_unit; 
set glucose_2014_2017_v5;
if clean_unit='';
run;
PROC MEANS DATA=missing_unit MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue; /*median=30*/
RUN;

/*Convert mmol/L to mg/dL (1 mmol/L =18 mg/dL), permissible range 0-1620 mg/dL*/
data glucose_2014_2017_v6 (rename=new_clean_unit=clean_unit rename=new_lab_value=LabChemResultNumericValue);
set glucose_2014_2017_v5; 
if clean_unit='MMOL/L' then new_lab_value=LabChemResultNumericValue*18;
else new_lab_value=LabChemResultNumericValue;
length new_clean_unit $5;
if clean_unit='' then new_clean_unit=''; else new_clean_unit='MG/DL';
if  new_lab_value>1620 then delete;
drop clean_unit LabChemResultNumericValue;
run;

/*double-check lab value ranges*/
PROC MEANS DATA=glucose_2014_2017_v6 MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue; 
RUN;

/*create HIGH & LOW values by patient and date*/
PROC SQL;
CREATE TABLE all_Glucose_hi_lo_2014_2017 (compress=yes)  AS  
SELECT *, max(LabChemResultNumericValue) as hi_Glucose_daily, min(LabChemResultNumericValue) as lo_Glucose_daily
FROM glucose_2014_2017_v6
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA=all_Glucose_hi_lo_2014_2017  nodupkey out=final.all_Glucose_hi_lo_2014_2017; 
BY  patienticn LabSpecimenDate hi_Glucose_daily lo_Glucose_daily;
RUN;
