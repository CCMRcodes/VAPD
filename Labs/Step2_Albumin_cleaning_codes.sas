/******** THIS EXAMPLE SAS CODE INCLUDES ALBUMIN LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1. THE GOAL WAS TO 
CREATE A HIGH AND LOW ALBUMIN VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

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

create table Albumin_all_2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[Albumin2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs by patient, facility, time of specimen and result*/
PROC SORT DATA=Albumin_all_2014_2017 nodupkey; 
BY  PatientSID Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*convert LabChemSpecimenDateTime to LabSpecimenDate*/
DATA  Albumin_all_2014_2017_V2;
SET  Albumin_all_2014_2017;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
keep Sta3n LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID Units RefHigh RefLow Topography LabSpecimenDate patienticn;
RUN;

/*change the character patienticn into numeric patienticn*/  
DATA Albumin_2014_2017_v4 (rename=patienticn2=patienticn);
SET Albumin_all_2014_2017_V2;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*clean up units*/
DATA Albumin_2014_2017_v4;
SET Albumin_2014_2017_v4;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units;
RUN;

/*look at frequency and delete non-blood topography and incorrect clean_units*/
PROC FREQ DATA=Albumin_2014_2017_v4  order=freq;
TABLE topography clean_unit;
RUN;

/*keep only those with result value >0, blood topography and acceptable clean_unit*/
DATA Albumin_2014_2017_v5; 
SET  Albumin_2014_2017_v4;
	if Topography notin ('PLASMA','SERUM','BLOOD','SER/PLA','BLOOD*','BLOOD.','serum',
'SER/PLAS','ARTERIAL BLOOD') OR  clean_unit notin ('G/DL','GM/DL','G/L','%','MG/DL','GR/DL','') 
or LabChemResultNumericValue <0
   		then delete;
RUN;

/*check units again, see if 'G/L' unit is still in dataset*/
proc freq data=Albumin_2014_2017_v5;
table clean_unit;
run;

/*check those with missing units only*/
data missing_unit; 
set Albumin_2014_2017_v5;
if clean_unit='';
run;
PROC MEANS DATA=missing_unit MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue; 
RUN;

/*convert G/L --> g/dL by dividing by 10*/
/*only keep permissible range: 0.5-6.5 g/dL*/  
data Albumin_2014_2017_v6 (rename=new_clean_unit=clean_unit rename=new_lab_value=LabChemResultNumericValue);
set Albumin_2014_2017_v5;
if clean_unit='G/L' then new_lab_value=LabChemResultNumericValue/10;
else new_lab_value=LabChemResultNumericValue;
length new_clean_unit $4;
if clean_unit='' then new_clean_unit=''; else new_clean_unit='G/DL';
if new_lab_value <0.5 or new_lab_value>6.5 then delete;
drop clean_unit LabChemResultNumericValue;
run;

/*check lab value IQR to make sure they're in the acceptable ranges*/
PROC MEANS DATA=Albumin_2014_2017_v6 MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue; 
RUN;

/*create HIGH & LOW values by patient and date*/
PROC SQL;
CREATE TABLE all_Albumin_hi_lo_2014_2017 (compress=yes)  AS   
SELECT *, max(LabChemResultNumericValue) as hi_Albumin_daily, min(LabChemResultNumericValue) as lo_Albumin_daily
FROM Albumin_2014_2017_v6
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort, save the final dataset*/
PROC SORT DATA=all_Albumin_hi_lo_2014_2017   nodupkey out=final.all_Albumin_hi_lo_2014_2017; 
BY  patienticn LabSpecimenDate hi_Albumin_daily lo_Albumin_daily;
RUN;


