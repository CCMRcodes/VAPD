/******** THIS EXAMPLE SAS CODE INCLUDES pH, pO2 and pCO2 LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1. THE GOAL WAS TO 
CREATE A HIGH AND LOW pH, pO2 and pCO2 VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

/* Date Modified: 9/18/2018
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

data Ph2014_2017_V1;
set Ph2014_2017;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units ;
run;

/*change patienticn into numeric*/   
DATA Ph2014_2017_V1 (rename=patienticn2=patienticn);
SET Ph2014_2017_V1;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

PROC FREQ DATA=Ph2014_2017_V1  order=freq;
TABLE  topography clean_unit;
RUN;

/*check some lab values by unit*/
data nul; 
set Ph2014_2017_V1;
if clean_unit='NUL';
run;
proc means data=nul MIN MAX MEAN MEDIAN Q1 Q3; /*median=7.4, keep*/
var LabChemResultNumericValue;
run;

data n_a; /*1515*/ 
set Ph2014_2017_V1;
if clean_unit='N/A';
run;
proc means data=n_a MIN MAX MEAN MEDIAN Q1 Q3; /*median=7.4, keep*/
var LabChemResultNumericValue;
run;

data LOGM_ML; 
set Ph2014_2017_V1;
if clean_unit='LOGM/ML';
run;
proc means data=LOGM_ML MIN MAX MEAN MEDIAN Q1 Q3; /*median=7.4, keep*/
var LabChemResultNumericValue;
run;

data num; 
set Ph2014_2017_V1;
if clean_unit='#';
run;
proc means data=num MIN MAX MEAN MEDIAN Q1 Q3; /*median=7.4, keep*/
var LabChemResultNumericValue;
run;

/*keep only those with result value in range, blood topography and acceptable unit*/
DATA Ph2014_2017_V2; 
SET Ph2014_2017_V1;
if LabChemResultNumericValue <0 
or Topography notin ('ARTERIAL BLOOD','BLOOD','VENOUS BLOOD','ARTERIAL BLD','BLOOD, VENOUS','SERUM',
'BLOOD, ARTERIAL','BLOOD VENOUS','PLASMA','ART BLOOD','VENOUS BLD','WHOLE BLOOD','BLOOD (VENOUS)') 
or clean_unit notin ('PH','PHUNITS','UNITS','MPH','NUL','MMHG','N/A','VENOUS','LOGM/ML','LOGMM/L','#','UNIT','')
	then delete; 
RUN;

/*Check labs with missing units only*/
data missing_labs; 
set Ph2014_2017_V2;
if clean_unit = '';
run;
proc means data=missing_labs MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run;

data Ph2014_2017_V2; 
set Ph2014_2017_V2;
if LabChemResultNumericValue <6.3 or  LabChemResultNumericValue >8.5 then delete;/*permissible range: 6.3-8.5*/
run;

proc means data=Ph2014_2017_V2 MIN MAX MEAN MEDIAN Q1 Q3; /*median=7.4*/
var LabChemResultNumericValue;
run;


/*create HI & LO values by date*/
PROC SQL;
CREATE TABLE final.all_pH_hi_lo_2014_2017 (compress=yes)  AS   
SELECT *, max(LabChemResultNumericValue) as hi_pH_daily, min(LabChemResultNumericValue) as lo_pH_daily
FROM  Ph2014_2017_V2
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

/*change patienticn into numeric*/   
DATA PO2_labs_all_2014_2017_V1 (rename=patienticn2=patienticn);
SET PO2_labs_all_2014_2017_V1;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

data PO2_labs_all_2014_2017_V1;
set PO2_labs_all_2014_2017_V1;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units ;
run;

PROC FREQ DATA=PO2_labs_all_2014_2017_V1  order=freq;
TABLE  topography clean_unit;
RUN;

/*keep only those with  blood topography and acceptable unit*/
DATA PO2_labs_all_2014_2017_V3;
SET PO2_labs_all_2014_2017_V1;
if LabChemResultNumericValue <0 
or Topography notin ('ARTERIAL BLOOD','BLOOD','VENOUS BLOOD','ARTERIAL BLD','BLOOD, VENOUS',
'BLOOD, ARTERIAL','BLOOD VENOUS','ART BLOOD','VENOUS BLD','BLOOD (VENOUS)','BLOOD,ARTERIAL','WHOLE BLOOD','PLASMA',
'SERUM') or clean_unit notin ('MMHG','MM/HG','TORR','') then delete; 
lab_id=_N_; /*assign each lab an unique ID to use in later step*/
RUN;

/*check "blood"*/
data blood ; 
set PO2_labs_all_2014_2017_V3;
if Topography in ('BLOOD','WHOLE BLOOD','PLASMA','SERUM');
run;

proc means data=blood MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run;

/*looks like a mixture of arterial and venous blood, try to debug more to decide what to include or exclude*/
proc freq data=blood order=freq;
table LabChemTestSID loincsid / out=freqcount;
run;

proc sort data=sepsis.ALL_LABTESTNAMES_09112018 nodupkey out=ALL_LABTESTNAMES; 
by Labchemtestsid LabChemTestName sta3n;
run;
proc sort data=sepsis.ALL_LOINCSIDS_09112018 nodupkey out=ALL_loincsid; 
by LOINC Component LOINCSID sta3n;
run;

/*left join the loinc and labtestnames back to dataset blood*/
proc sql;
create table blood_v2 (compress=yes) as 
select a.*, b.LabChemTestName, c.LOINC, c.Component as LOINC_Component
from blood a
left join ALL_LABTESTNAMES b on a.sta3n=b.sta3n and a.Labchemtestsid=b.Labchemtestsid /*ALL_LABTESTNAMES pulled from CDW: [CDWWork].[Dim].[LabChemTest] */
left join ALL_loincsid c on a.sta3n=c.sta3n and a.LOINCSID=c.LOINCSID; /*ALL_loincsid pulled from CDW:  [CDWWork].[Dim].[loinc]*/
quit;

proc freq data=blood_v2 order=freq;
table LabChemTestName / out=pO2_labtestnames ;
table LOINC_Component / out=pO2_LOINC_Component;
table loinc /out=pO2_loinc;
run;

data missing_loinc;
set blood_v2;
if loinc='';
run;
proc freq data=missing_loinc order=freq;
table LabChemTestName;
run;
proc means data=missing_loinc MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run;

data one ;
set blood_v2;
if loinc='19218-7'; /*change loinc code for each, to look at distribution and access*/
run;
proc means data=one MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run;

data blood_keep  blood_exclude; 
set blood_v2;
if loinc in ('11556-8','2703-7','19254-2') then output blood_keep; /*those three are what we decided to keep from looking at distributions*/
else output blood_exclude;
run;

data blood_exclude;
set blood_exclude;
exclude=1;
run;

/********************************************************************/
/*check "arterial blood"*/
data art_blood ; 
set PO2_labs_all_2014_2017_V3;
if Topography in ('ARTERIAL BLOOD','ARTERIAL BLD','BLOOD, ARTERIAL','ART BLOOD','BLOOD,ARTERIAL');
run;

proc means data=art_blood MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run;  /**KEEP**/

/*check venous blood*/
data ven_blood; 
set PO2_labs_all_2014_2017_V3;
if Topography in ('VENOUS BLOOD','BLOOD, VENOUS','BLOOD VENOUS','VENOUS BLD','BLOOD (VENOUS)');
run;

proc means data=ven_blood MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run; /**EXCLUDE**/

/*check missing labs*/
data missing_labs; 
set PO2_labs_all_2014_2017_V3;
if clean_unit = '';
run;
proc means data=missing_labs MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run;/**KEEP**/



/*	ONLY keep blood with LOINC code 11556-8, 2703-7, & 19254-2, 
arterial blood, those with missing units, and those in permissible range 15-720 mmHg*/
/*left join blood_exclude indicator to dataset*/
proc sql;
create table PO2_labs_all_2014_2017_V3b (compress=yes) as 
select a.*, b.exclude
from PO2_labs_all_2014_2017_V3 a
left join blood_exclude b on a.lab_id=b.lab_id;
quit;

DATA PO2_labs_all_2014_2017_V4; 
SET PO2_labs_all_2014_2017_V3b;
if LabChemResultNumericValue <15 or LabChemResultNumericValue > 720 
or Topography in ('VENOUS BLOOD','BLOOD, VENOUS','BLOOD VENOUS','VENOUS BLD','BLOOD (VENOUS)')
or exclude=1 then delete; 
RUN;

proc means data=pO2_labs_all_2014_2017_V4 MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run;


/*create HI & LO values by date*/
PROC SQL;
CREATE TABLE final.all_PO2_hi_lo_2014_2017 (compress=yes)  AS  
SELECT *, max(LabChemResultNumericValue) as hi_PO2_daily, min(LabChemResultNumericValue) as lo_PO2_daily
FROM  pO2_labs_all_2014_2017_V4
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

/*change patienticn into numeric*/   
DATA pCO2_2014_2017 (rename=patienticn2=patienticn);
SET PCO2_labs_all_2014_2017_V1;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

data PCo22014_2017_V1;
set pCO2_2014_2017;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop units2 units3 units ;
run;

PROC FREQ DATA=PCo22014_2017_V1  order=freq;
TABLE  topography clean_unit;
RUN;

/*keep only those with result value in range, blood topography and acceptable unit*/
DATA PCo22014_2017_V2; 
SET PCo22014_2017_V1;
if LabChemResultNumericValue <0
or Topography notin ('ARTERIAL BLOOD','BLOOD','VENOUS BLOOD','ARTERIAL BLD','BLOOD, VENOUS',
'BLOOD VENOUS','BLOOD, ARTERIAL','VENOUS BLD','PLASMA','ART BLOOD','BLOOD (VENOUS)',
'PERIPHERAL VENOUS BLOOD','BLOOD,ARTERIAL','WHOLE BLOOD','SERUM') 
or clean_unit notin ('MMHG','MMOL/L','MEQ/L','MM/HG','TORR','MM/L','') then delete; 
RUN;

/*check missing labs*/
data missing_labs; 
set PCo22014_2017_V2;
if clean_unit = '';
run;
proc means data=missing_labs MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run;

/*permissible range: 5-250 mmHg*/
data PCo22014_2017_V2; 
set PCo22014_2017_V2;
if LabChemResultNumericValue <5 or LabChemResultNumericValue > 250 then delete;
run;

proc means data=PCo22014_2017_V2 MIN MAX MEAN MEDIAN Q1 Q3;
var LabChemResultNumericValue;
run;

/*create HI & LO values by date*/
PROC SQL;
CREATE TABLE final.all_pCO2_hi_lo_2014_2017 (compress=yes)  AS  
SELECT *, max(LabChemResultNumericValue) as hi_pCO2_daily, min(LabChemResultNumericValue) as lo_pCO2_daily
FROM  PCo22014_2017_V2
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

PROC SORT DATA=final.all_pCO2_hi_lo_2014_2017   nodupkey;  
BY  patienticn LabSpecimenDate hi_pCO2_daily lo_pCO2_daily;
RUN;




