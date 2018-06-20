/******** THIS EXAMPLE SAS CODE INCLUDES ABG_MECHANICAL VENTILATOR CODES AND FACILITY LAB TEST NAMES TO PULL FROM THE VA CDW. THE GOAL WAS TO 
CREATE AN INDICATOR FOR ABG_MECHANICAL VENTILATOR FOR EACH PATIENT-DAY WHILE INPATIENT *********/

/*note: evaluating these for the concept of “evidence of a blood gas that would be used to manage a mechanically ventilated patient” 
As such, excluding blood gases from PFTs, the OR, and outpatient settings. Including diverse and not necessarily directly 
combinable lab tests of pH, pCO2, and pO2.*/

libname final ''; /*insert file path/directory*/
/*Note, update code to your own study name in step 3: [INSERT STUDY NAME]*/

/*update all macro variables*/
%let studyname= ; /*insert your own study name path in datawarehouse (CDW)*/
%let data_source= " " ;/*insert your own data source*/

/**** DOWNLOAD DFLT TABLE INTO SAS DATASET FROM VINCE ****/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table abg_2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [&Studyname.].[dflt].[ABG2014_2017] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*remove duplicate labs */
PROC SORT DATA=abg_2014_2017 nodupkey out=abg_mechvent_2014_2017; 
BY  patientSID  sta3n  LabChemResultNumericValue LabChemSpecimenDateTime;
RUN;

/*change patienticn into numeric*/ 
DATA abg_mechvent_2014_2017_V3  (rename=patienticn2=patienticn compress=yes);
SET abg_mechvent_2014_2017;
patienticn2 = input(patienticn, 10.);
drop patienticn;
abg_mechvent=1; /*ABG for mechanical ventilator indicator, 1=yes, 0=no*/
LabSpecimenDate=datepart(LabChemSpecimenDateTime); /*convert datetime to date*/
format LabSpecimenDate mmddyy10.;
RUN;

/*remove duplicates by patient and date*/
PROC SORT DATA=abg_mechvent_2014_2017_V3 nodupkey   
	OUT=final.abg_mechvent_2014_2017_V4 (keep=patienticn sta3n LabSpecimenDate abg_mechvent compress=yes); 
BY patienticn sta3n LabSpecimenDate abg_mechvent;
RUN;
