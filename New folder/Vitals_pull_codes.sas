/******** THIS EXAMPLE SAS CODE INCLUDES vitals TO PULL FROM THE VA CDW. THE GOAL WAS TO 
CREATE A HIGH AND LOW vital sign VALUE FOR EACH PATIENT-DAY WHILE INPATIENT *********/

libname final ''; /*insert file path/directory*/
/*Note, update code to your own study name in step 3: [INSERT STUDY NAME]*/

/*update all macro variables*/
%let studyname= ; /*insert your own study name path in datawarehouse (CDW)*/
%let data_source= " " ;/*insert your own data source*/
%let startdate = '20140101'; /*indicate start date*/
%let enddate = '20180101';/*indicate end date*/

%macro vitals (set, var);

PROC SQL;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ); 
CREATE TABLE  &set._2014_2017 AS
SELECT * FROM CONNECTION TO CDW1 (
SELECT  a.Sta3n, a.VitalSignTakenDateTime, a.PatientSID, 
        a.VitalResult, a.VitalResultNumeric,  B.VitalType, c.patienticn
FROM  [INSERT STUDY NAME].[Src].[Vital_VitalSign] as   A
Inner JOIN [CDWWork].[dim].[VitalType] as  B ON A.VitalTypeSID =B.VitalTypeSID
LEFT JOIN  [INSERT STUDY NAME].[src].[CohortCrosswalk] as C ON a.PatientSID=c.PatientSID
    WHERE a.VitalSignTakenDateTime >= &startdate. and a.VitalSignTakenDateTime < &enddate. and b.vitaltype =&VAR.; 
);
DISCONNECT FROM CDW1;
QUIT;

/*change patienticn into numeric*/  
DATA &set._2014_2017_V2 (rename=patienticn2=patienticn);
SET &set._2014_2017;
 patienticn2 = input(patienticn, 10.);
 Vital_date=datepart(VitalSignTakenDateTime); /*convert datetime to date*/
format Vital_date mmddyy10.;
drop patienticn;
RUN;

/*create max and min vital values for each patient and date*/
PROC SQL;
CREATE TABLE &set._2014_2017_V3 (compress=yes) as
SELECT patienticn, vital_date, max(VitalResultNumeric) as hi_&set._daily, 
							   min(VitalResultNumeric) as lo_&set._daily
FROM &set._2014_2017_V2
Group by patienticn,  vital_date
order by patienticn,  vital_date;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT data=&set._2014_2017_V3 nodupkey out=final.&set._2014_2017;
BY patienticn vital_date lo_&set._daily hi_&set._daily;
RUN;

%mend vitals;
%vitals (pulse, 'PULSE');
%vitals (temp, 'TEMPERATURE');
%vitals (rr, 'RESPIRATION');


/**************************************************************************************************************************/
/******* Pull Blood pressure data 2014-2017 and calculate mean arterial pressure ********/
PROC SQL;  
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ); 
CREATE TABLE  BP_2014_2017 AS
SELECT * FROM CONNECTION TO CDW1 (
SELECT a.Sta3n, a.VitalSignTakenDateTime, a.PatientSID, c.patienticn,
   		a.VitalResult, a.VitalResultNumeric, a.Systolic, a.Diastolic, B.VitalType
FROM  [INSERT STUDY NAME].[Src].[Vital_VitalSign] as   A
Inner JOIN [CDWWork].[dim].[VitalType] as  B ON A.VitalTypeSID =B.VitalTypeSID
LEFT JOIN  [INSERT STUDY NAME].[src].[CohortCrosswalk] as C ON a.PatientSID=c.PatientSID
   WHERE (a.VitalSignTakenDateTime >= '20140101' and a.VitalSignTakenDateTime < '20180101') and b.vitaltype ='Blood Pressure';
QUIT;

/*change patienticn into numeric*/  
DATA BP_2014_2017_V3 (rename=patienticn2=patienticn);
SET BP_2014_2017;
 patienticn2 = input(patienticn, 10.);
 Vital_date=datepart(VitalSignTakenDateTime);
format Vital_date mmddyy10.;
drop patienticn;
RUN;

/*calculate lo and hi MAP daily before doing min and max values per patient-day*/
DATA BP_2014_2017_V4 (compress=yes);
SET BP_2014_2017_V3;
MAP_daily=((2*Diastolic)+Systolic)/3;
format MAP_daily 9.1;
RUN;

/*create HI & LO values by patient and date*/
PROC SQL;
CREATE TABLE all_BP_hi_lo_2014_2017 (compress=yes)  AS  
SELECT *, max(Systolic) as hi_Systolic_daily, min(Systolic) as lo_Systolic_daily,
          max(Diastolic) as hi_Diastolic_daily, min(Diastolic) as lo_Diastolic_daily,
		  max(MAP_daily) as hi_MAP_daily, min(MAP_daily) as lo_MAP_daily
FROM BP_2014_2017_V4
GROUP BY patienticn, Vital_date
ORDER BY patienticn, Vital_date;
QUIT;

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA=all_BP_hi_lo_2014_2017  nodupkey out=final.all_BP_hi_lo_2014_2017; 
BY  patienticn Vital_date hi_Systolic_daily lo_Systolic_daily hi_Diastolic_daily lo_Diastolic_daily 
hi_MAP_daily lo_MAP_daily;
RUN;


