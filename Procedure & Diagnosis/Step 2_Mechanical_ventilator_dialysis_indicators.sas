/*Step 1 was to pull all procedure codes between 2014-2017 from CDW. Below code will use those procedure codes to identify Mechanical Ventilation
and Dialysis by patient-day*/

/* Date Modified: 7/2/2018
   Author: Shirley Wang */

libname final ''; /*insert file path/directory*/

/*update all macro variables*/
%let studyname= ; /*insert your own study name path in datawarehouse (CDW)*/
%let data_source= " " ;/*insert your own data source*/

/**** DOWNLOAD DFLT TABLE INTO SAS DATASET FROM VINCI ****/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table proc2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[proc2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*get proc_date and keep only certain fields*/
DATA proc2014_2017_V2 (compress=yes); 
retain patienticn Sta3n procdate ICD9ProcedureCode ICD10ProcedureCode CPTCode;
SET proc2014_2017;
procdate=datepart(ICDProcedureDateTime); /*create Procedure date variable*/
format procdate mmddyy10.;
ICD9ProcedureCode=compress(ICD9ProcedureCode,'.'); /*take out the decimal*/
ICD10ProcedureCode=compress(ICD10ProcedureCode,'.'); /*take out the decimal*/
keep patienticn Sta3n procdate ICD9ProcedureCode ICD10ProcedureCode;
RUN;

/*change patienticn into numeric*/ 
DATA proc2014_2017_V2 (rename=patienticn2=patienticn);
SET proc2014_2017_V2;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*get mechanical ventilator indicator*/
DATA  mech_vent_2014_2017;
SET  proc2014_2017_V2;
if ICD9ProcedureCode  in  ('9670', '9671', '9672') OR ICD10ProcedureCode in ('5A1935Z', '5A1945Z', '5A1955Z', '5A1935Z', '5A1945Z', '5A1955Z') 
then mechvent_daily=1 ; 
	else mechvent_daily=0;
if mechvent_daily=1; /*ONLY KEEP THOSE WITH MECHVENT=1 IN THE DATASET*/
RUN;

PROC SORT DATA=mech_vent_2014_2017 out=FINAL.mech_vent_2014_2017 (keep=patienticn Sta3n procdate mechvent) nodupkey; 
BY  patienticn procdate mechvent_daily;
RUN;


/********************** DIALYSIS DAILY INDICATOR ********************/
/*get dialysis indicator*/
DATA  dialysis_2014_2017; 
SET  proc2014_2017_V2;
if ICD9ProcedureCode  in  ("3995", "5498") OR ICD10ProcedureCode in ("5A1D00Z", "3E1M39Z")  then Dialysis_daily=1 ; 
	else Dialysis_daily=0;
if Dialysis_daily=1; /*ONLY KEEP THOSE WITH DIALYSIS_DAILY=1 IN THE DATASET*/
RUN;

PROC SORT DATA=dialysis_2014_2017 out=FINAL.dialysis_2014_2017 (keep=patienticn Sta3n procdate dialysis_daily) nodupkey; 
BY  patienticn procdate Dialysis_daily;
RUN;

/*CAN LEFT JOIN MECHANICAL VENTILATION AND DIALYSIS DAILY INDICATORS TO VAPD BY PATIENTICN AND PRODATE (DATEVALUE)*/
