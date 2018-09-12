/******** THIS EXAMPLE SAS CODE INCLUDES WHITE BLOOD CELL LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1 SQL CODE. THE GOAL WAS TO 
CREATE A HIGH AND LOW WHITE BLOOD CELL VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

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

create table WBC2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[WBC2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs*/
PROC SORT DATA=wbc_2014_2017 nodupkey; 
BY  patientSID  sta3n LabChemTestSID  LOINCSID Units LabChemResultNumericValue LabChemSpecimenDateTime;
RUN;

/*create new date values*/
data wbc_2014_2017;
set wbc_2014_2017;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
year=year(LabSpecimenDate);
format LabSpecimenDate mmddyy10.;
keep Sta3n year LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID Units RefHigh RefLow Topography LabSpecimenDate patienticn;
run;

/*creat clean unit*/
data wbc_2014_2017;
set wbc_2014_2017;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
drop  units2 units3 units;
run;

PROC FREQ DATA=wbc_2014_2017  order=freq;
TABLE topography  clean_unit;
RUN;

data wbc_2014_2017_V2 (compress=yes); 
set wbc_2014_2017; 
if topography notin ('BLOOD','WHOLE BLOOD','PLASMA','SERUM','WS-BLOOD','BLOOD - SM','BLOOD*',
'BLOOD, VENOUS','PLASMA+SERUM','SER/PLA') 
or clean_unit notin ('K/CMM','K/UL','K/MM3','10*3/UL','10E3/UL','X10-3/UL','X1000/UL','10E9/L',
'K/MCL','BILL/L','10X3CUMM','THOUS/CMM','10(3)/MCL','/UL','THOU/CUMM','10E3/MCL','1000/UL','THOU/UL',
'T/CMM','K/MM-3','K/CUMM','X103','K/CCM','103/UL','10X3/CMM','#/CMM','K/ML','X10E3/UL','#/UL','CUMM',
'10X3/CCM','X1000','/CUMM','/CUM',"10'3/UL",'UL','THOUCMM','/CMM','10X3/UL','CMM','THOU','TH/MM3',
'CELLS/UL','1000/MCL','TH/UL','THOUS/UL','X10(9)/L','WBC/CMM','X10(3)/UL','THO/MM3','THOUS/MM3','X10E9/L',
'/MM3','1000/MM3','') or LabChemResultNumericValue <0 
   then delete;
run;

/*look at descriptive for missing unit*/
data misisng_unit_blood; 
set wbc_2014_2017_V2;
if clean_unit='';
run;

PROC MEANS DATA=misisng_unit_blood  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;/*median=5900*/ /*need to divide by 1000*/
RUN;

/*permissible range 0-300 X 10^9/L*/

/*conversions: 1 MCL=10^6 L, so it's 1000/MCL, so 0-300 thou/mcl is the range*/
/*mm3=10^6L=uL */

/*look at descriptive for /UL*/
data UL; 
set wbc_2014_2017_V2;
if clean_unit in ('UL');
run;

PROC MEANS DATA=UL   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;/*median=5900*/ /*need to divide by 1000*/
RUN;

data UL2;
set wbc_2014_2017_V2;
if clean_unit in ('CELLS/UL');
run;

PROC MEANS DATA=UL2   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;/*median=6700*//*need to divide by 1000*/
RUN;


/*look at descriptives for /L*/
data L; 
set wbc_2014_2017_V2;
if clean_unit in ('10E9/L','BILL/L','X10(9)/L','X10E9/L');
run;

PROC MEANS DATA=L   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;/*median=7.4*/
RUN;


/*look at descriptives for thousand/ uL, MM3, MCL*/
data thousands; 
set wbc_2014_2017_V2;
if clean_unit in ('K/CMM','K/UL','K/MM3','10*3/UL','10E3/UL','X10-3/UL','X1000/UL',
'K/MCL','10X3CUMM','THOUS/CMM','10(3)/MCL','THOU/CUMM','10E3/MCL','1000/UL','THOU/UL',
'T/CMM','K/MM-3','K/CUMM','X103','K/CCM','103/UL','10X3/CMM','K/ML','X10E3/UL',
'10X3/CCM','X1000',"10'3/UL",'THOUCMM','10X3/UL','THOU','TH/MM3','1000/MCL',
'TH/UL','THOUS/UL','X10(3)/UL','THO/MM3','THOUS/MM3','1000/MM3');
run;

PROC MEANS DATA=thousands   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*median=7.4*/
RUN;


/*look at descriptives for CUMM, CCM, CUM, CMM, etc.*/
data cumm; 
set wbc_2014_2017_V2;
if clean_unit in ('#/CMM','CUMM','/CUMM','/CUM','/CMM','CMM','WBC/CMM','/MM3');
run;

PROC MEANS DATA=cumm   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*mean=7377, median=5700*/ /*need to divide by 1000*/
RUN;

/*convert units*/
data wbc_2014_2017_V3 (rename=new_clean_unit=clean_unit rename=new_lab_value=LabChemResultNumericValue); /*25803475*/
set wbc_2014_2017_V2;
if clean_unit in ('#/CMM','CUMM','/CUMM','/CUM','/CMM','CMM','WBC/CMM','/MM3','UL','CELLS/UL') 
	then new_lab_value=LabChemResultNumericValue/1000;
else new_lab_value=LabChemResultNumericValue;
length new_clean_unit $4;
if clean_unit='' then new_clean_unit=''; else new_clean_unit='K/uL';
if new_lab_value <0 or new_lab_value>300 then delete;
drop clean_unit LabChemResultNumericValue;
run;

PROC MEANS DATA=wbc_2014_2017_V3   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;
RUN;

/*change patienticn into numeric*/   
DATA wbc_2014_2017_V6b (rename=patienticn2=patienticn);
SET wbc_2014_2017_V3;
year=year(LabSpecimenDate);
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;


/*create HIGH & LOW values by patient and date*/
PROC SQL;
CREATE TABLE all_WBC_hi_lo_2014_2017 (compress=yes)  AS   
SELECT *, max(LabChemResultNumericValue) as hi_WBC_daily, min(LabChemResultNumericValue) as lo_WBC_daily
FROM wbc_2014_2017_V6b
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA=all_WBC_hi_lo_2014_2017   nodupkey OUT=FINAL.all_WBC_hi_lo_2014_2017; 
BY  patienticn LabSpecimenDate hi_WBC_daily lo_WBC_daily;
RUN;

