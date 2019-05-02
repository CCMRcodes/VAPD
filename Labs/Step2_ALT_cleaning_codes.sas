/******** THIS EXAMPLE SAS CODE INCLUDES ALT LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1. 
THE GOAL WAS TO CREATE A HIGH AND LOW ALT VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

/* Date Modified: 4/17/2019
   Author: Shirley Wang */

libname final ''; /*insert file path/directory*/
/*Note, update code to your own study name: [INSERT STUDY NAME]*/

/*update all macro variables*/
%let studyname= ; /*insert your own study name path in datawarehouse (CDW)*/
%let data_source= " " ;/*insert your own data source*/

/**** DOWNLOAD DFLT TABLE INTO SAS DATASET FROM VINCI ****/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table Alt_all_2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[ALT2014_2017 ] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 



/*remove duplicate labs by patient, facility, time of specimen and result*/
PROC SORT DATA=Alt_all_2014_2017 out=alt2014_2017_20190409 nodupkey;
BY  PatientSID Sta3n LabChemSpecimenDateTime LabChemResultNumericValue LabChemResultValue;
RUN;

/*change patienticn into numeric*/   
DATA alt2014_2017_20190409 (rename=patienticn2=patienticn);
SET alt2014_2017_20190409;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

data alt2014_2017_20190409_V1; 
set alt2014_2017_20190409;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units ;
run;

PROC FREQ DATA=alt2014_2017_20190409_V1  order=freq;
TABLE  topography clean_unit;
RUN;

/*	Check are there any lab values <0*/
data check_value; 
SET alt2014_2017_20190409_V1;
if LabChemResultNumericValue <0 and LabChemResultNumericValue NE . ;
run;
PROC FREQ DATA=check_value  order=freq;
TABLE  LabChemResultNumericValue;
RUN;
proc means data=check_value MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run; 

/*keep only those blood topography and acceptable unit*/
DATA alt2014_2017_20190409_V2; 
SET alt2014_2017_20190409_V1;
if LabChemResultNumericValue  in (-59.0000, -99.0000, -18.0000) /*got these negative values from prior check "heck_value"*/ or 
Topography notin ('PLASMA','SERUM','BLOOD','SER/PLA','BLOOD*','BLOOD.','serum','SER/PLAS','SERUM+PLASMA',
'WS-PLASMA','SERUM 2','SERUM & PLASMA','ARTERIAL BLOOD','PLASMA - SM','BLOOD & SERUM','BLOOD VENOUS',
'BLOOD, VENOUS','PLASMA & WHOLE BLOOD','SERUM/BLOOD','WHOLE BLOOD') 
or clean_unit notin ('U/L','IU/L','UNITS/L','INTUNIT/L','INTUNITS/L','UNIT/L','IU/ML',
'U/I','"U/L','?U/L','INTUNT/L','') then delete; 
RUN;  /*convert IU/ML to IU/L: 1000ML=1L*/

/*check topography & clean_unit once again*/
PROC FREQ DATA=alt2014_2017_20190409_V2  order=freq;
TABLE  topography clean_unit;
RUN;

/*decide on those with missing units and without a numeric lab value*/
/*look at descriptive for those with missing unit*/
data missing_labs_units; 
set alt2014_2017_20190409_V2;
if clean_unit = '';
run;
proc means data=missing_labs_units MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run;  /**Principle Investigator thinks okay to keep**/

/*convert IU/ML to IU/L: 1000ML=1L*/
data ML (compress=yes); 
set  alt2014_2017_20190409_V2 ;
if clean_unit='IU/ML' ;
run ;
proc means data=ML MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue; /*mean/median seems like it's per Liter unit*/
run; 

/*check any lab value > 87*/
data check_lab; 
set ML;
if LabChemResultNumericValue > 100;
run;

proc means data=check_lab MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue; 
run; 
/*conclusion, I think the unit is a typo, the lab value desciptives look like it's in Liter. No conversion needed!*/

/*change clean_unit to equal units/L*/
data alt2014_2017_20190409_V3 (compress=yes); 
set alt2014_2017_20190409_V2;
length clean_unit2 $7;
clean_unit2='units/L';
run;

proc freq data=alt2014_2017_20190409_V3 ;
table clean_unit2;
run;


/*check for lab value missingness*/
data missings; /*0.7% missing lab value*/
set alt2014_2017_20190409_V2;
if LabChemResultNumericValue=. or  LabChemResultValue='';
run;

proc freq data=missings order=freq;
table LabChemResultValue LabChemResultNumericValue ;
run;

/*Principle investigator made decision:
Any of the "<" to equal 5, any of the ">" to equal 2000, if no "<" & no ">" then delete.*/
data test (compress=yes);
set alt2014_2017_20190409_V2;
if LabChemResultValue in ('<6', '<5', '<10', '<15', '< 6', '<3', '<5.0', '<4', '<7', '<6.0', '< 5', '< 5.0', 
'<6.00', '<8', '<1', '<5.', '<30', '<2', '<60', '< 1', '<14', '<21', '<20', '<9', '< 30', '<11', '<120', 
'<2.00', '<13', '<25', '<4.00', '<5.00', '<', '< 60', '<10.0', '<5.0', '< 12', '< 120', '< 18', '< 21', 
'< 3.', '<0', '<0.5', '<1.0', '<12.0', '<17', '<2.4', '<3.0', '<7.0', '<8.0', '<=5',

'>2600', '>2500', '>1000', '>5000', '>4113', '>2000', '>3039', '>7000', '>4000', '>942', '> 4113', '>5', 
'>4400', '>325', '>5200', '> 3872', '>700', '>1100', '>3850', '>400', '>5.0', '>750', '>7800', '> 942', 
'>100', '>1500', '>1600', '>19280', '>1945', '>200', '>20000', '>3086', '>3200', '>3500', '>375', 
'>4100', '>4200', '>480', '>6', '>6000', '>7', '>78.0', '>8000', '>8400', '>8800') ;
run;

data alt2014_2017_20190409_V4 (compress=yes);
set alt2014_2017_20190409_V3;
if LabChemResultValue in ('<6', '<5', '<10', '<15', '< 6', '<3', '<5.0', '<4', '<7', '<6.0', '< 5', '< 5.0', 
'<6.00', '<8', '<1', '<5.', '<30', '<2', '<60', '< 1', '<14', '<21', '<20', '<9', '< 30', '<11', '<120', 
'<2.00', '<13', '<25', '<4.00', '<5.00', '<', '< 60', '<10.0', '<5.0', '< 12', '< 120', '< 18', '< 21', 
'< 3.', '<0', '<0.5', '<1.0', '<12.0', '<17', '<2.4', '<3.0', '<7.0', '<8.0', '<=5') then LabChemResultNumericValue=5;
if LabChemResultValue in ('>2600', '>2500', '>1000', '>5000', '>4113', '>2000', '>3039', '>7000', '>4000', '>942', '> 4113', '>5', 
'>4400', '>325', '>5200', '> 3872', '>700', '>1100', '>3850', '>400', '>5.0', '>750', '>7800', '> 942', 
'>100', '>1500', '>1600', '>19280', '>1945', '>200', '>20000', '>3086', '>3200', '>3500', '>375', 
'>4100', '>4200', '>480', '>6', '>6000', '>7', '>78.0', '>8000', '>8400', '>8800') then LabChemResultNumericValue=2000;
run;

proc means data=alt2014_2017_20190409_V4 MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue; 
run; 

/*create HI & LO values by date*/
PROC SQL;
CREATE TABLE all_ALT_hi_lo_2014_2017 (compress=yes)  AS  
SELECT *, max(LabChemResultNumericValue) as hi_ALT_daily, min(LabChemResultNumericValue) as lo_ALT_daily
FROM  alt2014_2017_20190409_V4
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

PROC SORT DATA=all_ALT_hi_lo_2014_2017  out=final.all_ALT_hi_lo_2014_2017 nodupkey; 
BY  patienticn LabSpecimenDate hi_ALT_daily lo_ALT_daily;
RUN;
