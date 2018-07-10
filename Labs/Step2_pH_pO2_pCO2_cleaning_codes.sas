/******** THIS EXAMPLE SAS CODE INCLUDES pH, pO2 and pCO2 LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1. THE GOAL WAS TO 
CREATE A HIGH AND LOW pH, pO2 and pCO2 VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

/* Date Modified: 6/29/2018
   Author: Shirley Wang */

libname final ''; /*insert file path/directory*/
/*Note, update code to your own study name in step 3: [INSERT STUDY NAME]*/

/*update all macro variables*/
%let studyname= ; /*insert your own study name path in datawarehouse (CDW)*/
%let data_source= " " ;/*insert your own data source*/

/************** pH Labs **************/

/**** DOWNLOAD DFLT TABLE INTO SAS DATASET FROM VINCI ****/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table pH2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[pH2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs by patient, facility, time of specimen and result*/
PROC SORT DATA=pH2014_2017 nodupkey; 
BY  PatientSID Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*convert Units Topography*/
DATA pH_labs_all_2014_2017_V2 (compress=yes); 
SET  pH2014_2017;
Topography2=upcase(Topography); /*turn all units into uppercase*/
Topography3=compress(Topography2,'.'); /*removes '.' in units*/
clean_Topography = compress(Topography3); /*removes all blanks (by default - specify options to remove other chars)*/
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units Topography Topography2 Topography3;
run;

PROC FREQ DATA=pH_labs_all_2014_2017_V2  order=freq;
TABLE clean_Unit  clean_Topography;  /*have some with missing units*/
RUN;

PROC MEANS DATA=pH_labs_all_2014_2017_V2   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  LabChemResultNumericValue ;
RUN;


/*keep those with correct units and topography only*/
PROC SQL;
CREATE TABLE pH_labs_all_2014_2017_V3 (compress=yes) AS  /*  2233897*/
SELECT *
FROM  pH_labs_all_2014_2017_V2
WHERE  clean_Topography  in ('ARTERIALBLOOD','BLOOD','VENOUSBLOOD','ARTERIALBLD','BLOOD,VENOUS','SERUM','BLOOD,ARTERIAL',
'BLOODVENOUS','PLASMA','ARTBLOOD','VENOUSBLD','WHOLEBLOOD','BLOOD(VENOUS)') and 0< LabChemResultNumericValue <100 and
clean_unit in ('PH','PHUNITS','UNITS','MPH','MMHG','VENOUS','LOGM/ML','LOGMM/L','UNIT','') ; /*also include those with missing units*/
QUIT;


/* Access labs with and without units*/
/*those with non-missing units*/
data nonmissing_units; 
set pH_labs_all_2014_2017_V3;
if clean_unit NE '';
run;

PROC MEANS DATA= nonmissing_units  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*mean: 7.4*/
RUN;

/*most with missing units*/  
data missing_units; 
set pH_labs_all_2014_2017_V3;
if clean_unit = '';
run;

PROC MEANS DATA= missing_units  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*mean: 7.4272795, makes sense, will keep them*/
RUN;

/*change patienticn into numeric*/   /*  2233897*/
DATA pH_labs_all_2014_2017_V4 (rename=patienticn2=patienticn);
SET pH_labs_all_2014_2017_V3;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*create HI & LO values by date*/
PROC SQL;
CREATE TABLE final.all_pH_hi_lo_2014_2017 (compress=yes)  AS   
SELECT *, max(LabChemResultNumericValue) as hi_pH_daily, min(LabChemResultNumericValue) as lo_pH_daily
FROM  pH_labs_all_2014_2018_V4
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

PROC SORT DATA= final.all_pH_hi_lo_2014_2017   nodupkey; 
BY  patienticn LabSpecimenDate hi_pH_daily lo_pH_daily;
RUN;



/************** pO2 Labs **************/

/**** DOWNLOAD DFLT TABLE INTO SAS DATASET FROM VINCI ****/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table PO2_labs_all_2014_2017_V1 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[pO22014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs by patient, facility, time of specimen and result*/
PROC SORT DATA=PO2_labs_all_2014_2017_V1 nodupkey; 
BY  PatientSID Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

PROC FREQ DATA=PO2_labs_all_2014_2017_V1  order=freq;
TABLE  topography ;
RUN;
PROC MEANS DATA=PO2_labs_all_2014_2017_V1   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;
RUN;

DATA PO2_labs_all_2014_2017_V3; 
SET PO2_labs_all_2014_2017_V1;
if Topography not in ('ARTERIAL BLOOD','BLOOD','VENOUS BLOOD','ARTERIAL BLD','BLOOD, VENOUS','SERUM','BLOOD,ARTERIAL',
'BLOOD VENOUS','PLASMA','ART BLOOD','VENOUS BLD','WHOLE BLOOD','BLOOD (VENOUS)') or LabChemResultNumericValue <0 then delete;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units;
RUN;

PROC FREQ DATA=PO2_labs_all_2014_2017_V3  order=freq;
TABLE clean_unit; /*some with missing units*/
RUN;

/*those labs with units*/
data nonmissing_units; 
set PO2_labs_all_2014_2017_V3;
if clean_unit NE '';
run;

PROC MEANS DATA=nonmissing_units  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*mean: 99.5278818, median: 77.5000000*/
RUN;

/*most with missing units*/  /*how to clean up units?*/
data missing_units; 
set PO2_labs_all_2014_2017_V3;
if clean_unit = '';
run;

PROC MEANS DATA= missing_units  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*mean: 74.86, median: 51.4, doesn't really make sense, so delete. There should only be very few*/
RUN;

PROC SQL;
CREATE TABLE PO2_labs_all_2014_2018_V4 (compress=yes)  AS 
SELECT *
FROM nonmissing_units
WHERE  clean_unit in ('MMHG','MM/HG', 'TORR') ; /*1 mmhg = 1.0000001618554 torr*/
QUIT;

/*change patienticn into numeric*/   /* 1998001*/
DATA PO2_labs_all_2014_2018_V6 (rename=patienticn2=patienticn);
SET PO2_labs_all_2014_2018_V4;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*create HI & LO values by date*/
PROC SQL;
CREATE TABLE final.all_PO2_hi_lo_2014_2017 (compress=yes)  AS  
SELECT *, max(LabChemResultNumericValue) as hi_PO2_daily, min(LabChemResultNumericValue) as lo_PO2_daily
FROM  PO2_labs_all_2014_2018_V6 
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

PROC SORT DATA= final.all_PO2_hi_lo_2014_2017   nodupkey ; 
BY  patienticn LabSpecimenDate hi_PO2_daily lo_PO2_daily;
RUN;


/************** pCO2 Labs **************/

/**** DOWNLOAD DFLT TABLE INTO SAS DATASET FROM VINCI ****/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table PCO2_labs_all_2014_2017_V1 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[pCO22014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs by patient, facility, time of specimen and result*/
PROC SORT DATA=PCO2_labs_all_2014_2017_V1 nodupkey; 
BY  PatientSID Sta3n LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

PROC FREQ DATA=PCO2_labs_all_2014_2017_V1  order=freq;
TABLE  units topography;
RUN;

/*convert units*/
DATA PCO2_labs_all_2014_2017_V2; 
SET  PCO2_labs_all_2014_2017_V1;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units;
RUN;

PROC FREQ DATA=PCO2_labs_all_2014_2017_V2 order=freq;
TABLE clean_unit;
RUN;
PROC MEANS DATA=PCO2_labs_all_2014_2017_V2   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  LabChemResultNumericValue;
RUN;

PROC SQL;
CREATE TABLE  PCO2_labs_all_2014_2017_V3 (compress=yes) AS 
SELECT *
FROM PCO2_labs_all_2014_2017_V2
WHERE   clean_unit in ('MMOL/L','MMOL/DL','MEQ/L','MMHG','MM/HG','MM/L','TORR','')  /*also include those with missing units*/
and  0<LabChemResultNumericValue <1000 and 
topography in ('PLASMA','SERUM','ARTERIAL BLOOD','BLOOD','SER/PLA','VENOUS BLOOD','ARTERIAL BLD',
'BLOOD*','BLOOD, VENOUS','BLOOD VENOUS','BLOOD, ARTERIAL','VENOUS BLD','BLOOD.','ART BLOOD','serum','BLOOD (VENOUS)','SER/PLAS',
'BLOOD,ARTERIAL','WHOLE BLOOD');
QUIT;

PROC FREQ DATA=PCO2_labs_all_2014_2017_V3  order=freq;
TABLE  topography clean_unit;
RUN;
PROC MEANS DATA=PCO2_labs_all_2014_2017_V3   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  LabChemResultNumericValue;
RUN;

/*those labs with units*/
data nonmissing_units; 
set PCO2_labs_all_2014_2017_V3;
if clean_unit NE '';
run;

PROC MEANS DATA= nonmissing_units  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*mean: 27.5460558, median: 27*/
RUN;

/*most with missing units*/ 
data missing_units; 
set PCO2_labs_all_2014_2017_V3;
if clean_unit = '';
run;

PROC MEANS DATA=missing_units  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue; /*mean: 43.2, median: 41, not normal, delete*/
RUN;

/*change patienticn into numeric*/  
DATA pco2_labs_all_2014_2017_V4 (rename=patienticn2=patienticn);
SET nonmissing_units;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*create HI & LO values by date*/
PROC SQL;
CREATE TABLE final.all_pCO2_hi_lo_2014_2017 (compress=yes)  AS  
SELECT *, max(LabChemResultNumericValue) as hi_pCO2_daily, min(LabChemResultNumericValue) as lo_pCO2_daily
FROM  pco2_labs_all_2014_2017_V4
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

PROC SORT DATA=final.all_pCO2_hi_lo_2014_2017   nodupkey;  
BY  patienticn LabSpecimenDate hi_pCO2_daily lo_pCO2_daily;
RUN;




