/******** THIS EXAMPLE SAS CODE INCLUDES WHITE BLOOD CELL LOINC CODES AND FACILITY LAB TEST NAMES PULLED FROM THE VA CDW IN STEP 1 SQL CODE. THE GOAL WAS TO 
CREATE A HIGH AND LOW WHITE BLOOD CELL VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

/* Date Modified: 6/29/2018
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


/*keep only those with result value >0, blood topography and acceptable clean_unit*/
data wbc_2014_2017_V2 (compress=yes); 
set wbc_2014_2017; 
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
format LabSpecimenDate mmddyy10.;
if topography notin ('BLOOD', 'WHOLE BLOOD', 'BLOOD,CAPILLARY', 'PLASMA','WS-BLOOD','SERUM') or  LabChemResultNumericValue <0 
   then delete;
keep Sta3n LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID Units RefHigh RefLow topography LabSpecimenDate patienticn;
run;

/*convert units*/
DATA wbc_2014_2017_V3 (compress=yes);
SET wbc_2014_2017_V2; 
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
units4 = compress(units3); /*removes all blanks (by default - specify options to remove other chars)*/
if units4 ='PERCENT' or units4 ='"%"' or units4 ="%'" or units4 ='%30' 
	then units4='%';
if units4 ='THOU' or units4='K' or units4='X10-3' or units4='X(10)3' or units4='X1000' or units4='X10E3' 
	then units4='THOUSAND';
if units4 ='10*3/UL' or units4='X10-3/UL' or units4='X1000/UL' or units4='x1000/uL' or units4='10E3/UL' or units4='10e3/uL' or units4='X103/UL'
or units4='1000/UL' or units4='X10E3/UL' or units4 ='THOU/UL' or units4="10'3/UL" or units4='10X3/UL' or units4='K/UL' or units4='103/UL' 
or units4='TH/UL' or units4='X10(3)/UL' or units4='10.e3/uL' or units4='K/ul' or units4='10*3/uL' or units4='K/uL' or units4='10E9/L'
or units4='x10-3/uL' or units4='X10-3/ul' or units4='10e3/uL' or units4='thou/uL' or units4='1000/uL' or units4='x1000/ul' or units4='Thous./ul'
or units4='10 3/uL' or units4='X10 3/UL' or units4='10 3/ uL' or units4='K/UL'
	then units4='THOUS/UL';
if units4 ='10X3CUMM' or units4='T/CUMM' or units4='K/CUMM' or units4='K/cumm' or units4='10x3cumm' then units4='THOU/CUMM';
if units4 ='X103' then units4='X(10)3';
if units4 ='THOUCMM' or units4='K/CMM' or units4='10X3/CMM' or units4='K/CCM' or units4='K/cmm' or units4='t/cmm' or units4='Thous./cmm' 
	or units4='k/cmm' or units4='K/Cmm' or units4='THOUS/CMM'
	then units4='THOUS/CMM';
if units4 ='X1000/MM3' or units4='THOUS/MM3' or units4='TH/MM3' or units4='K/MM-3' or units4='K/mm3'  or units4='K/MM3' or units4='K/mm-3'
	then units4='1000/MM3';
if units4='10E3/MCL'  then units4='10(3)/MCL';
if units4='10*9/L' then units4='X10(9)/L';
if units4='/CUMM' or units4='/CUM' then units4='CUMM';
drop units2 units3 units;
RUN;

PROC FREQ DATA= wbc_2014_2017_V3   order=freq;
TABLE units4;
RUN;

DATA wbc_2014_2017_V4 (rename=units4=clean_unit compress=yes) ;
SET  wbc_2014_2017_V3;
if units4 notin ('THOUS/UL','THOUS/CMM','1000/MM3','K/MCL','/UL','THOUSAND','THOU/CUMM','X10(9)/L','10(3)/MCL', 'T/CMM','1000/MCL')
  then delete; 
run;

PROC FREQ DATA= wbc_2014_2017_V4 order=freq;
TABLE  clean_unit topography;
RUN;

PROC MEANS DATA=wbc_2014_2017_V4   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;
RUN;

/*change patienticn into numeric*/   
DATA wbc_2014_2017_V6b (rename=patienticn2=patienticn);
SET wbc_2014_2017_V4;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

PROC FREQ DATA=wbc_2014_2017_V6b  order=freq;
TABLE  LabChemTestName clean_unit;
RUN;

/******************************************************************/

/*look at histogram of LabChemResultNumericValue*/;
proc sgplot data=wbc_2014_2017_V6b noautolegend;
 histogram LabChemResultNumericValue/ binwidth = 100;
run;

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

