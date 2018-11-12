/******** THIS EXAMPLE SAS CODE INCLUDES UREA LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1 SQL CODE. THE GOAL WAS TO 
CREATE A HIGH AND LOW UREA VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

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

create table Urea2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[Urea2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs by patient, facility, time of specimen and result*/
PROC SORT DATA=Urea2014_2017  nodupkey; 
BY  PatientSID Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*convert LabChemSpecimenDateTime to LabSpecimenDate*/
DATA Urea_all_2014_2017_V2;
SET Urea2014_2017;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
keep Sta3n LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID Units RefHigh RefLow Topography LabSpecimenDate patienticn;
RUN;

/*change patienticn into numeric*/
DATA Urea_2014_2017_v4 (rename=patienticn2=patienticn);
SET Urea_all_2014_2017_V2;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*clean up units*/
DATA Urea_2014_2017_v4;
SET Urea_2014_2017_v4;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units;
RUN;

PROC FREQ DATA= Urea_2014_2017_v4  order=freq;
TABLE topography clean_unit;
RUN;

/*keep only those with result value >0, blood topography and acceptable clean_unit*/

/*Separately, accessed lab with unit of 'MG/ML' and looked at it's distribution. Conclusion: incluse lab with unit 'MG/ML', no need 
for conversion to 'MG/DL' because the distribution of 'MG/ML' looks right, think this unit was a typo by sta3n 596*/

DATA  Urea_2014_2017_v6; 
SET   Urea_2014_2017_v4;
if LabChemResultNumericValue <0  or Topography notin ('PLASMA','SERUM','BLOOD','SER/PLA','BLOOD*',
'BLOOD.','BLOOD, VENOUS','VENOUS BLD','VENOUS BLOOD','BLOOD VENOUS','serum','ARTERIAL BLOOD',
'SER/PLAS','ARTERIAL BLD')
   or  clean_unit notin ('MG/ML','MG/DL','MMOL/L','') then delete; 
RUN;

/*check missing labs*/
data missing_unit; 
set Urea_2014_2017_v6;
if clean_unit='';
run;
PROC MEANS DATA=missing_unit MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue; /*median=30*/
RUN;

/*•Decided to exclude missing units */
/*Convert mmol/L to mg/dL  (6 mg/dL=1 mmol/L), keep only those within permissible range 3-600 MG/DL*/
data Urea_2014_2017_v7 (rename=new_clean_unit=clean_unit rename=new_lab_value=LabChemResultNumericValue);
set Urea_2014_2017_v6; 
if clean_unit='MMOL/L' then new_lab_value=LabChemResultNumericValue*6;
else new_lab_value=LabChemResultNumericValue;
length new_clean_unit $5; /*creat new clean unit*/
new_clean_unit='MG/DL';
if new_lab_value <3 or new_lab_value>600 or clean_unit='' then delete;
drop clean_unit LabChemResultNumericValue;
run;

PROC MEANS DATA=Urea_2014_2017_v7 MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue; /*median=17*/
RUN;


/*create HIGH & LOW values by patient and date*/
PROC SQL;
CREATE TABLE all_urea_hi_lo_2014_2017 (compress=yes)  AS   
SELECT *, max(LabChemResultNumericValue) as hi_Urea_daily, min(LabChemResultNumericValue) as lo_Urea_daily
FROM urea_2014_2017_v7
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA=all_urea_hi_lo_2014_2017  nodupkey out=final.all_urea_hi_lo_2014_2017; 
BY  patienticn LabSpecimenDate hi_urea_daily lo_urea_daily;
RUN;
