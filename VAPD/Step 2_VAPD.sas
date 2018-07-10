/*Step 2: turning the acute hospitalizations into daily rows*/

/* Date Modified: June 2018
   Author: Brenda Vincent & Shirley Wang */

libname icu " "; /*Insert folder path where other VAPD datasets are saved, for example, labs, and pharmacy datasets*/

/*http://vaww.virec.research.va.gov/Notebook/RNB/RNB3-CDW-BedSection-CY15.pdf*/

options compress=yes;
%let studyname= ; /*insert your own study name path in datawarehouse (CDW)*/
%let data_source= " " ;/*insert your own data source*/
%let year=20142017;
%let todaysDate = %sysfunc(today(), mmddyyn8.);
%put &todaysDate;

/*********************************DAILY VA Patient Dataset 2014-2017********************************************/

data icu.allacute&year.;
set icu.allacute&year.;
format specialtytransferdate specialtydischargedate date9.;
run;

/*Create a row for each calendar day at a given STA6A*/
data icu.vapd_daily&year.;
set icu.allacute&year.;
do hospitalization_calendarday=specialtytransferdate to specialtydischargedate;
	hospitalization_calendarday=hospitalization_calendarday; output;
end;
format hospitalization_calendarday date9.;
run;

proc sort data=icu.vapd_daily&year. nodupkey dupout=check; 
by patienticn sta6a icu hospitalization_calendarday newadmitdate; 
run; 

options compress=yes;
data icu.vapd_daily&year.;
set icu.vapd_daily&year.
(keep=patienticn scrssn inpatientsid patientsid sta3n sta6a
specialty acute icu dod admityear icdtype icd10code1--icd9code25 date_of_pull
cdw_admitdatetime cdw_dischargedatetime newdischargedatetime newadmitdatetime
newadmitdate newdischargedate specialtytransferdatetime specialtytransferdate specialtydischargedatetime
specialtydischargedate inhospmort mort30  hospitalization_calendarday);
run;

/*Count for ICU day within a hospitalization (this can be non-consecutive)*/
proc sort data= icu.vapd_daily&year.; by patienticn sta6a newadmitdatetime icu hospitalization_calendarday; run;

data icu.vapd_daily&year.;
set  icu.vapd_daily&year.;
by patienticn sta6a newadmitdatetime;
if icu=1 then do;
if first.newadmitdatetime then icu_day=0;
icu_day+1;
end;
else if icu=0 then icu_day=.;
run;

/*Count for hospital day within a hospitalization (this can be non-consecutive)*/
proc sort data= icu.vapd_daily&year. ; by patienticn sta6a newadmitdatetime descending icu hospitalization_calendarday; run;

proc sort data= icu.vapd_daily&year. nodupkey; by patienticn sta6a newadmitdatetime hospitalization_calendarday; run; 

data icu.vapd_daily&year.;
set  icu.vapd_daily&year.;
by patienticn sta6a newadmitdatetime;
if first.newadmitdatetime then hospital_day=0;
hospital_day+1;
run;

data icu.vapd_daily&year.;
set  icu.vapd_daily&year.;
admityear=year(datepart(newadmitdatetime));
run;

/*Separate into years, check Ns*/
data icu.vapd_daily2014 icu.vapd_daily2015 icu.vapd_daily2016 icu.vapd_daily2017;
set icu.vapd_daily&year.;
if admityear=2014 then output icu.vapd_daily2014;
if admityear=2015 then output icu.vapd_daily2015;
if admityear=2016 then output icu.vapd_daily2016;
if admityear=2017 then output icu.vapd_daily2017;
run;

/*delete admityear=2018*/
DATA  vapd_daily&year._v2; /* does it add up from annualy daily datasets?*/
SET  icu.vapd_daily&year.;
if admityear=2018 then delete;
RUN;

/*remove duplicates patient-facility days one more time*/
PROC SORT DATA=vapd_daily&year._v2  nodupkey; 
BY  patienticn sta6a newadmitdate newdischargedate hospitalization_calendarday;
RUN;

/*Total number of patient-facility-days*/
proc freq data=vapd_daily&year._v2;
table admityear;
run;

/*Total number of ICU-patient-facility-days*/
proc freq data=vapd_daily&year._v2;
table admityear;
where icu=1;
run;

/*get unique hospitalizations only, don't need to include facility*/
PROC SORT DATA=vapd_daily&year._v2 nodupkey  OUT=unique_hosp_2014_2017; 
BY  patienticn newadmitdate newdischargedate;
RUN;

PROC FREQ DATA= unique_hosp_2014_2017  order=freq;
TABLE  admityear;
RUN;

PROC SORT DATA=vapd_daily&year._v2 nodupkey  OUT=distinct_pat; 
BY  patienticn;
RUN;

DATA icu; 
SET  vapd_daily&year._v2;
if icu=1;
RUN;
PROC SORT DATA=icu nodupkey  OUT=icu_hosps; 
BY  patienticn  newadmitdate newdischargedate ;
RUN;

/*separately calculate 30-day readmit rate*/
proc sort data=unique_hosp_2014_2017  out=unique_hosp_readmit30 (keep=patienticn newadmitdate newdischargedate); 
by patienticn newadmitdate newdischargedate;
run;

data part1;
retain patienticn newadmitdate newdischargedate;
set unique_hosp_readmit30;
by PatientICN newadmitdate newdischargedate;
lag_discharge=lag(newdischargedate); /*retrieve the value of the previous discharge date*/
format lag_discharge mmddyy10.;
gap=newadmitdate-lag_discharge; /*calculte gap between the current admission date and the previous discharge date*/

if first.patienticn then do; /*reset the values of the following fields in case of a new patienticn*/
lag_discharge=.;
gap=.;
readmit30=.;
end;
if 0 <= gap <=30 then readmit30=1; /*identify a readmit and assign value of 1 to readmit30*/
run;

/*left join lag_discharge, gap and readmit30 fields back to original unique hosp dataset*/
PROC SQL;
	CREATE TABLE icu.vapd_daily&year._v2 (compress=yes)  AS 
	SELECT A.*, B.lag_discharge, b.gap as gap_previous_admit, b.readmit30
	FROM vapd_daily&year._v2  A
	LEFT JOIN part1  B
	ON A.PatientICN =B.PatientICN and a.newadmitdate=b.newadmitdate and a.newdischargedate=b.newdischargedate ;
QUIT;

/*USE icu.vapd_daily&year._v2 TO RUN THE ELIXHAUSER COMORBIDITIES CODE */

/*data check_diag; */
/*set icu.vapd_daily&year._v2;*/
/*if icd10code1 ='' and icd9code1 ='';*/
/*run;*/


/*combine icd9 and icd10 diagnosis fields, shorten the number of diagnosis fields*/
/*want diagnosis on a daily level*/
data diag_daily (compress=yes);
set icu.vapd_daily&year._v2;
keep patienticn sta3n sta6a hospitalization_calendarday specialtytransferdate  specialtydischargedate icdtype icd9code1-icd9code25  icd10code1-icd10code26;
run;

/*take out decimals in icd10 and icd9 diag codes*/
data  diag_daily2 (compress=yes); 
set  diag_daily;
icd10code1=compress(icd10code1,'.'); icd10code2=compress(icd10code2,'.'); icd10code3=compress(icd10code3,'.');icd10code4=compress(icd10code4,'.');icd10code5=compress(icd10code5,'.');
icd10code6=compress(icd10code6,'.');icd10code7=compress(icd10code7,'.');icd10code8=compress(icd10code8,'.');icd10code9=compress(icd10code9,'.');icd10code10=compress(icd10code10,'.');
icd10code11=compress(icd10code11,'.'); icd10code12=compress(icd10code12,'.'); icd10code13=compress(icd10code13,'.');icd10code14=compress(icd10code14,'.');icd10code15=compress(icd10code15,'.');
icd10code16=compress(icd10code16,'.');icd10code17=compress(icd10code17,'.');icd10code18=compress(icd10code18,'.');icd10code19=compress(icd10code19,'.');icd10code20=compress(icd10code20,'.');
icd10code21=compress(icd10code21,'.'); icd10code22=compress(icd10code22,'.'); icd10code23=compress(icd10code23,'.');icd10code24=compress(icd10code24,'.');icd10code25=compress(icd10code25,'.');
icd10code26=compress(icd10code26,'.');
icd9code1=compress(icd9code1,'.'); icd9code2=compress(icd9code2,'.'); icd9code3=compress(icd9code3,'.');icd9code4=compress(icd9code4,'.');icd9code5=compress(icd9code5,'.');
icd9code6=compress(icd9code6,'.');icd9code7=compress(icd9code7,'.');icd9code8=compress(icd9code8,'.');icd9code9=compress(icd9code9,'.');icd9code10=compress(icd9code10,'.');
icd9code11=compress(icd9code11,'.'); icd9code12=compress(icd9code12,'.'); icd9code13=compress(icd9code13,'.');icd9code14=compress(icd9code14,'.');icd9code15=compress(icd9code15,'.');
icd9code16=compress(icd9code16,'.');icd9code17=compress(icd9code17,'.');icd9code18=compress(icd9code18,'.');icd9code19=compress(icd9code19,'.');icd9code20=compress(icd9code20,'.');
icd9code21=compress(icd9code21,'.'); icd9code22=compress(icd9code22,'.'); icd9code23=compress(icd9code23,'.');icd9code24=compress(icd9code24,'.');icd9code25=compress(icd9code25,'.');
run;

/*separate data into icd9 and icd10*/
DATA icd9 (keep=patienticn sta6a icdtype specialtytransferdate hospitalization_calendarday icd9code1-icd9code25)  
     icd10 (keep=patienticn sta6a  icdtype specialtytransferdate hospitalization_calendarday  icd10code1-icd10code26) ;
SET  diag_daily2;
if icdtype='ICD10' then output icd10; 
if icdtype='ICD9' then output icd9; 
RUN;

/*rename fields*/
DATA icd9_V2;
SET  icd9;
rename icd9code1=DiagnosisCode_1 icd9code2=DiagnosisCode_2 icd9code3=DiagnosisCode_3 icd9code4=DiagnosisCode_4 icd9code5=DiagnosisCode_5
       icd9code6=DiagnosisCode_6 icd9code7=DiagnosisCode_7 icd9code8=DiagnosisCode_8 icd9code9=DiagnosisCode_9 icd9code10=DiagnosisCode_10
       icd9code11=DiagnosisCode_11 icd9code12=DiagnosisCode_12 icd9code13=DiagnosisCode_13 icd9code14=DiagnosisCode_14 icd9code15=DiagnosisCode_15
       icd9code16=DiagnosisCode_16 icd9code17=DiagnosisCode_17 icd9code18=DiagnosisCode_18 icd9code19=DiagnosisCode_19 icd9code20=DiagnosisCode_20
	   icd9code21=DiagnosisCode_21 icd9code22=DiagnosisCode_22 icd9code23=DiagnosisCode_23 icd9code24=DiagnosisCode_24 icd9code25=DiagnosisCode_25;
RUN;

DATA icd10_V2;
SET  icd10;
rename icd10code1=DiagnosisCode_1 icd10code2=DiagnosisCode_2 icd10code3=DiagnosisCode_3 icd10code4=DiagnosisCode_4 icd10code5=DiagnosisCode_5
       icd10code6=DiagnosisCode_6 icd10code7=DiagnosisCode_7 icd10code8=DiagnosisCode_8 icd10code9=DiagnosisCode_9 icd10code10=DiagnosisCode_10
       icd10code11=DiagnosisCode_11 icd10code12=DiagnosisCode_12 icd10code13=DiagnosisCode_13 icd10code14=DiagnosisCode_14 icd10code15=DiagnosisCode_15
       icd10code16=DiagnosisCode_16 icd10code17=DiagnosisCode_17 icd10code18=DiagnosisCode_18 icd10code19=DiagnosisCode_19 icd10code20=DiagnosisCode_20
	   icd10code21=DiagnosisCode_21 icd10code22=DiagnosisCode_22 icd10code23=DiagnosisCode_23 icd10code24=DiagnosisCode_24 icd10code25=DiagnosisCode_25
       icd10code26=DiagnosisCode_26;
RUN;

DATA  all_diag;
SET icd9_V2 icd10_V2;
RUN;

PROC SORT DATA=all_diag nodupkey;
BY patienticn sta6a icdtype specialtytransferdate hospitalization_calendarday;
RUN;

/*left join renamed fields back to dataset icu.vapd_daily&year._v2, compare/check the renamed fields with the original field to make sure they're
right before dropping the original/old diag fields*/
PROC SQL;
	CREATE TABLE vapd_daily&year._v3  (compress=yes)  AS 
	SELECT A.*, B.DiagnosisCode_1, b.DiagnosisCode_2, b.DiagnosisCode_3, B.DiagnosisCode_4, b.DiagnosisCode_5, b.DiagnosisCode_6, B.DiagnosisCode_7, b.DiagnosisCode_8, b.DiagnosisCode_9,
		B.DiagnosisCode_10, B.DiagnosisCode_11, b.DiagnosisCode_12, b.DiagnosisCode_13, B.DiagnosisCode_14, b.DiagnosisCode_15, b.DiagnosisCode_16, B.DiagnosisCode_17, b.DiagnosisCode_18, b.DiagnosisCode_19,
		B.DiagnosisCode_20,B.DiagnosisCode_21,B.DiagnosisCode_22,B.DiagnosisCode_23,B.DiagnosisCode_24,B.DiagnosisCode_25,B.DiagnosisCode_26
	FROM icu.vapd_daily&year._v2  A
	LEFT JOIN all_diag  B
	ON A.patienticn=B.patienticn and a.sta6a=b.sta6a and a.specialtytransferdate=b.specialtytransferdate 
	 and a.hospitalization_calendarday=b.hospitalization_calendarday and a.icdtype=b.icdtype;
QUIT;

DATA  vapd_daily&year._v3 (compress=yes rename=patienticn2=patienticn);
SET  vapd_daily&year._v3;
patienticn2 = input(patienticn, 10.); /*change patienticn into numeric*/
drop patienticn icd10code1-icd10code26  icd9code1-icd9code25; /*drop the old diagnosis variables*/
rename hospitalization_calendarday=datevalue; /*rename hospitalization_calendarday to datevalue*/ 
format InpatientSID 25.;
RUN;

/*NOTE: must run the SAS program named VAPD_Elixhauser_Comorbid and used the final dataset saved in the Procedure & Diagnosis folder*/

/*then, add/left join the 31 elixhauser comorbid groups to VAPD*/
PROC SQL;
	CREATE TABLE icu.vapd_daily&year._v3  (compress=yes)  AS 
	SELECT A.*, b.sum_Elixhauser_count, B.ELX_GRP_1, B.ELX_GRP_2, B.ELX_GRP_3, B.ELX_GRP_4, B.ELX_GRP_5, B.ELX_GRP_6, B.ELX_GRP_7, B.ELX_GRP_8, B.ELX_GRP_9, B.ELX_GRP_10, 
	B.ELX_GRP_11, B.ELX_GRP_12, B.ELX_GRP_13, B.ELX_GRP_14, B.ELX_GRP_15, B.ELX_GRP_16, B.ELX_GRP_17, B.ELX_GRP_18, B.ELX_GRP_19, B.ELX_GRP_20, B.ELX_GRP_21, B.ELX_GRP_22, B.ELX_GRP_23, 
	B.ELX_GRP_24, B.ELX_GRP_25, B.ELX_GRP_26, B.ELX_GRP_27, B.ELX_GRP_28, B.ELX_GRP_29, B.ELX_GRP_30, B.ELX_GRP_31
	FROM   vapd_daily&year._v3  A
	LEFT JOIN  icu.Elixhauser_2014_2017 B 
	ON A.patienticn=B.patienticn and a.sta6a=b.sta6a and a.icdtype=b.icdtype and a.specialtytransferdate=b.specialtytransferdate and a.specialtydischargedate=b.specialtydischargedate;
QUIT;


/**********************************************************************************************/
/*get Race data from CDW*/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table cohort_crosswalk (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT   t1.PatientSID, t1.Sta3n, t1.Race, t1.RaceSID 
FROM [&Studyname.].[Src].[patsub_patientrace] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

PROC FREQ DATA=cohort_crosswalk  order=freq;
TABLE race ;
RUN;

DATA  race_missing;
SET  cohort_crosswalk;
if race ='' then delete;
length new_race $45;
if race ='DECLINED TO ANSWER' or race='UNKNOWN BY PATIENT' then new_race='UNKNOWN';
else if race='WHITE NOT OF HISP ORIG' then new_race='WHITE';
else new_race=race;
RUN;

PROC FREQ DATA=race_missing  order=freq;
TABLE new_race ;
RUN;

PROC SORT DATA=race_missing  nodupkey  OUT=icu.patsub_patientrace;
BY  PatientSID Sta3n;
RUN;

/*get Gender and date of birth data from CDW*/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table SPatient (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.PatientSID, t1.Sta3n, t1.birthdatetime, t1.gender 
FROM [&Studyname.].[Src].[SPatient_SPatient] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

DATA  SPatient2;
SET  SPatient;
DOB=datepart(BirthDateTime);
format DOB  mmddyy10.;
RUN;

PROC SORT DATA= SPatient2  nodupkey  OUT=icu.SPatient_gender_dob;  
BY  PatientSID  Sta3n  dob gender;
RUN;

/*left join date of birth, race and gender back to VAPD*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v4 (compress=yes)  AS
	SELECT A.*, B.new_race as Race, C.gender, C.dob
	FROM  icu.vapd_daily&year._v3 A
	LEFT JOIN icu.patsub_patientrace B ON A.PatientSID =B.PatientSID and a.sta3n=b.sta3n  
	LEFT JOIN icu.SPatient_gender_dob C ON A.PatientSID =c.PatientSID and a.sta3n=c.sta3n;
QUIT;

/*calculate age at admission and hospital length of stay*/
DATA  vapd_daily&year._v5;
SET vapd_daily&year._v4;
age=(INT((newadmitdate-dob)/365.25)); 
hosp_LOS = (newdischargedate-newadmitdate)+1;
RUN;

/*** Table 2 of manuscript: Hospitalization Characteristic, don't need to include facility*/
/*unique hospitalizations*/
PROC SORT DATA=vapd_daily&year._v5  nodupkey  OUT=unique_hosp_2014_2017_table2; 
BY  patienticn  newadmitdate newdischargedate;
RUN;

PROC SORT DATA=unique_hosp_2014_2017_table2;
BY  admityear;
RUN;

PROC FREQ DATA=unique_hosp_2014_2017_table2  order=freq;
by  admityear;
tables inhospmort;
RUN; 

PROC MEANS DATA=unique_hosp_2014_2017_table2  MIN MAX MEAN MEDIAN Q1 Q3;
class admityear;
VAR hosp_los;
RUN;

/*should be out of live discharges only*/
DATA alive_discharge_hosps;
SET unique_hosp_2014_2017_table2;
if inhospmort=0;
RUN;

PROC SORT DATA=alive_discharge_hosps;
BY  admityear;
RUN;

PROC FREQ DATA=alive_discharge_hosps  order=freq;
by admityear;
TABLE  readmit30 mort30;
RUN;


/***************************************************************************************************/
/*Separate into DIFFERENT years*/
data vapd_daily2014 vapd_daily2015 vapd_daily2016 vapd_daily2017;
set vapd_daily&year._v5;
if admityear=2014 then output vapd_daily2014;
if admityear=2015 then output vapd_daily2015;
if admityear=2016 then output vapd_daily2016;
if admityear=2017 then output vapd_daily2017;
run;

/*2014*/
PROC SORT DATA=vapd_daily2014  nodupkey  OUT=unique_pat2014  ; /*N unique patients*/
BY  patienticn;
RUN;

PROC MEANS DATA=unique_pat2014   MEAN Std; /*mean age*/
VAR age ;
RUN;

PROC FREQ DATA=unique_pat2014  order=freq;
TABLE  gender;
RUN;

/*2015*/
PROC SORT DATA=vapd_daily2015  nodupkey  OUT=unique_pat2015  ; /*N unique patients*/
BY  patienticn;
RUN;

PROC MEANS DATA=unique_pat2015   MEAN Std; /*mean age*/
VAR age ;
RUN;

PROC FREQ DATA=unique_pat2015  order=freq;
TABLE  gender;
RUN;

/*2016*/
PROC SORT DATA=vapd_daily2016  nodupkey  OUT=unique_pat2016  ; /*N unique patients*/
BY  patienticn;
RUN;

PROC MEANS DATA=unique_pat2016   MEAN Std; /*mean age*/
VAR age ;
RUN;

PROC FREQ DATA=unique_pat2016  order=freq;
TABLE  gender;
RUN;

/*2017*/
PROC SORT DATA=vapd_daily2017  nodupkey  OUT=unique_pat2017 ; /*N unique patients*/
BY  patienticn;
RUN;

PROC MEANS DATA=unique_pat2017   MEAN Std; /*mean age*/
VAR age ;
RUN;

PROC FREQ DATA=unique_pat2017  order=freq;
TABLE  gender;
RUN;

/** End of table 2 of manuscript*/

/**********************************************************************************************/
/*get disposition type data from CDW*/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table icu.dispotype2014_2017 (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.AdmitDateTime, t1.PatientSID, t1.Sta3n,  t1.InpatientSID, t1.DispositionType, t1.PlaceOfDispositionSID, t1.DischargeDateTime
FROM [&Studyname.].[Src].[Inpat_Inpatient] as t1) as t1
where t1.AdmitDateTime > '2014-01-01' and t1.AdmitDateTime <'2018-01-01';
DISCONNECT FROM CDW1 ;
quit; 

PROC SORT DATA=icu.dispotype2014_2017  nodupkey;
BY PatientSID  sta3n InpatientSID;
RUN;

/*left join disposition type*/
PROC SQL;
	CREATE TABLE vapd_daily&year._v6  (compress=yes)  AS 
	SELECT A.*, B.DispositionType
	FROM  vapd_daily&year._v5    A
	LEFT JOIN icu.dispotype2014_2017  B
	ON A.PatientSID =b.PatientSID and a.sta3n=b.sta3n and a.InpatientSID=b.inpatientsid;
QUIT;

/*1 - Home,   2 - Transfer to another acute care hospital, 3 - Subacute facility, 4 - Death, 5 - Hospice*/
data vapd_daily&year._v6 (compress=yes); 
set vapd_daily&year._v6;
if DispositionType=1 then Discharge_dispo=1;
if DispositionType=2 then Discharge_dispo=2;
if DispositionType=4 then Discharge_dispo=1;
if DispositionType=5 then Discharge_dispo=2;
if DispositionType=6 then Discharge_dispo=4;
if DispositionType=7 then Discharge_dispo=4;
run;

/**** Add hospital characteristics ****/
/*hospital Excel files are saved in VAPD Github folder, must be imported into SAS datasets first*/
proc freq data=icu.hospital;
table Hospital_ID;
run;
PROC FREQ DATA=vapd_daily&year._v6 order=freq;
TABLE sta6a sta3n;
RUN;

/*sta6a=facility on site_level dataset & hosptal dataset,  sta3n=facility on level_beds */
proc sql;
	CREATE TABLE vapd_daily&year._v7 (compress=yes) AS /* 11203622 */
	SELECT a.*,  
           b.state_code,
		   b.site,
		   b.operatingbeds,
		   b.beds,
		   b.region,
		   b.teaching,
           c.level,
           d.Official__Station_Name
	FROM  vapd_daily&year._v6  a
	LEFT JOIN icu.hospital b ON a.sta6a=b.Hospital_ID
	Left JOIN icu.level_beds_region_teaching c on b.site=c.site
	Left Join icu.hospital_data d on a.sta6a=d.Station_Number;
quit;

PROC FREQ DATA=vapd_daily&year._v7 order=freq;
TABLE  state_code site operatingbeds beds region teaching level Official__Station_Name;
RUN;

/******************************************************************/
/*look at hosp characteristics*/
data hosp_copy; 
set icu.hospital;
sta3n_char = substr(hospital_id, 1,3);
sta3n=input(sta3n_char ,3.);
drop sta3n_char;
run;

data  hosp_copy;
set  hosp_copy;
length AHA_hosp_size  $6.; 
if beds<200 then AHA_hosp_size='Small'; else if  200=<beds<=499 then AHA_hosp_size='Medium'; else if beds>=500 then AHA_hosp_size='Large';
if sta3n=679 then  teaching=1 ;
if sta3n=675 or sta3n=629 or sta3n=529 then teaching=0;
if sta3n=675 or sta3n=629 or sta3n=529 then beds=0;
if sta3n=679 or sta3n=675 or sta3n=629 then region='SOUTH' ; 
if sta3n=679 then beds=87;
if sta3n=529 then region ='NORTHEAST';
if beds = '.' and region='' and teaching='.' then delete;
run;

PROC SORT DATA=hosp_copy  nodupkey  OUT=hosp_copy2; 
BY sta3n  beds  region teaching AHA_hosp_size;
RUN;

PROC SQL;
	CREATE TABLE vapd_daily&year._v8 (compress=yes) AS
	SELECT A.*, B.operatingbeds as operatingbeds2, b.beds as beds2, b.region as region2, b.teaching as teaching2, b.AHA_hosp_size as AHA_hosp_size2
	FROM  vapd_daily&year._v7  A
	LEFT JOIN hosp_copy2  B ON A.sta3n =B.sta3n;
QUIT;

DATA vapd_daily&year._v9 (compress=yes);
SET  vapd_daily&year._v8;
drop operatingbeds beds region teaching;
RUN;

DATA  vapd_daily&year._v10 (compress=yes);
set vapd_daily&year._v9;
rename operatingbeds2=operatingbeds beds2=beds region2=region teaching2=teaching AHA_hosp_size2=AHA_hosp_size;
run;

PROC FREQ DATA=vapd_daily&year._v10  order=freq;
TABLE admityear*operatingbeds  admityear*beds admityear*region admityear*teaching admityear*AHA_hosp_size;
RUN;

DATA  sta3n;
SET  vapd_daily&year._v10;
keep admityear sta3n operatingbeds beds  region teaching AHA_hosp_size;
RUN;

/*Table 2 of the manuscript: Facility Characteristic */
DATA vapd_2014 vapd_2015 vapd_2016 vapd_2017;
SET sta3n;
if admityear=2014 then output vapd_2014;
if admityear=2015 then output vapd_2015;
if admityear=2016 then output vapd_2016;
if admityear=2017 then output vapd_2017;
RUN;

PROC SORT DATA=vapd_2014 nodupkey;
BY  sta3n;
RUN;

PROC FREQ DATA=vapd_2014  order=freq;
TABLE region teaching AHA_hosp_size;
RUN;

PROC SORT DATA=vapd_2015 nodupkey;
BY  sta3n;
RUN;

PROC FREQ DATA=vapd_2015  order=freq;
TABLE region teaching AHA_hosp_size;
RUN;

PROC SORT DATA=vapd_2016 nodupkey;
BY  sta3n;
RUN;

PROC FREQ DATA=vapd_2016  order=freq;
TABLE  region teaching AHA_hosp_size;
RUN;

PROC SORT DATA=vapd_2017 nodupkey ;
BY  sta3n;
RUN;

PROC FREQ DATA=vapd_2017  order=freq;
TABLE  region teaching AHA_hosp_size;
RUN;

/*add microbiology labs and culture indicators*/

/*First, run the microbiology labs and culture indicators codes saved in the Micro Labs & Culture's folder in Github*/

PROC SQL;
	CREATE TABLE   vapd_daily&year._v11 (compress=yes)  AS 
	SELECT A.*, B.hasmicro as Blood_cult_ind, b.collectionsample as bloodcult_collection_sample, b.topography as bloodcult_topography,
	   c.hasmicro as Other_Micro_ind, c.collectionsample as othermicro_collection_sample, c.topography as othermicro_topography
	FROM  vapd_daily&year._v10 A
	LEFT JOIN  icu.has_bloodcult B ON A.patienticn =B.patienticn and a.datevalue=b.dv1 
    LEFT JOIN icu.has_oth_micro C on A.patienticn =c.patienticn and a.datevalue=c.dv1;
QUIT;


/*add medications data*/

/**** Vasopressors ****/
/*First, run the vasopressor med codes saved in the medications folder in Github*/
/*Add in the pressors for Cardiovascular SOFA*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v12 (compress=yes)   AS 
	SELECT A.*, B.drugname_1 as pressor_1, b.drugname_2 as pressor_2, b.drugname_3 as pressor_3, b.drugname_4 as pressor_4, b.drugname_5 as pressor_5
	FROM   vapd_daily&year._v11  A
	LEFT JOIN  icu.trans_pressors_2014_2017  B ON A.patienticn =B.patienticn and a.datevalue=b.actiondate;
QUIT;

/*** add sedatives, Paralytic and Analgesic ***/
/*First, run the Other_drugs_code saved in the Medications folder in Github*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v13  (compress=yes)  AS 
	SELECT A.*, B.drugname_1,B.drugname_2,B.drugname_3,B.drugname_4,B.drugname_5,B.drugname_6,B.drugname_7,B.drugname_8,B.drugname_9
	FROM  vapd_daily&year._v12  A
	LEFT JOIN icu.trans_all_sedatives_2014_2017  B ON A.patienticn =B.patienticn and a.datevalue=b.actiondate;
QUIT;

PROC FREQ DATA=vapd_daily&year._v13  order=freq;
TABLE  drugname_9;
RUN;


/*Antibiotics*/
/*First, run the Antibiotic_med_pull_codes saved in the Medications folder in Github*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v14 (compress=yes)  AS
	SELECT A.*, B.abx1, B.abx2,B.abx3,B.abx4,B.abx5,B.abx6,B.abx7,B.abx8,B.abx9,B.abx10,B.abx11,B.abx12,
				B.abx13,B.abx14,B.abx15,B.abx16,B.abx17,B.abx18,B.abx19,B.abx20
	FROM   vapd_daily&year._v13  A
	LEFT JOIN  icu.all_abx_num_06012018 B ON A.patienticn =B.patienticn and a.datevalue=b.actiondate;
QUIT;

/*** Add labs ***/
/*First, run all lab codes saved in the Labs folder in Github*/
/*lactate, platelets, bilirubin, and creatinine lab hi and lo values*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v15 (compress=yes) AS 
	SELECT A.*, B.hi_lactate_daily, b.lo_lactate_daily, b.Units as lactate_Units,
				c.hi_plate_daily, c.lo_plate_daily, c.Units as plate_Units,
				d.hi_creat_daily, d.lo_creat_daily, d.Units as creat_Units,
				e.hi_bili_daily, e.lo_bili_daily, e.Units as bili_Units
	FROM  vapd_daily&year._v14  A
	LEFT JOIN icu.all_lactate_hi_lo_2014_2017 B ON A.patienticn =B.patienticn and a.datevalue=b.LabSpecimenDate 
	LEFT JOIN icu.all_plate_hi_lo_2014_2017 C ON A.patienticn =C.patienticn and a.datevalue=c.LabSpecimenDate 
	LEFT JOIN icu.all_creat_hi_lo_2014_2017 D ON A.patienticn =D.patienticn and a.datevalue=d.LabSpecimenDate
	LEFT JOIN icu.all_bili_hi_lo_2014_2017  E ON A.patienticn =e.patienticn and a.datevalue=e.LabSpecimenDate;
QUIT;

/*add WBC, glucose, sodium, pottassium*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v16 (compress=yes) AS
	SELECT A.*, B.hi_WBC_daily, b.lo_WBC_daily, b.clean_unit as WBC_Units,
				c.hi_Glucose_daily, c.lo_Glucose_daily, c.clean_unit as Glucose_Units,
				d.hi_sodium_daily, d.lo_sodium_daily, d.clean_unit as sodium_Units,
				e.hi_potassium_daily, e.lo_potassium_daily, e.clean_unit as potassium_Units
	FROM  vapd_daily&year._v15  A
	LEFT JOIN icu.all_WBC_hi_lo_2014_2017 B ON A.patienticn =B.patienticn and a.datevalue=b.LabSpecimenDate 
	LEFT JOIN icu.all_Glucose_hi_lo_2014_2017 C ON A.patienticn =C.patienticn and a.datevalue=c.LabSpecimenDate 
	LEFT JOIN icu.all_sodium_hi_lo_2014_2017  D ON A.patienticn =D.patienticn and a.datevalue=d.LabSpecimenDate
	LEFT JOIN icu.all_potassium_hi_lo_2014_2017 E ON A.patienticn =e.patienticn and a.datevalue=e.LabSpecimenDate;
QUIT;

/*add Urea, Albumin, pH, PO2, pCO2*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v17 (compress=yes) AS 
	SELECT A.*, B.hi_urea_daily, b.lo_urea_daily, b.clean_unit as urea_Units,
				c.hi_albumin_daily, c.lo_albumin_daily, c.clean_unit as albumin_Units,
				d.hi_pH_daily, d.lo_pH_daily, d.clean_unit as ph_Units,
				e.hi_pO2_daily, e.lo_pO2_daily, e.clean_unit as pO2_Units,
			    f.hi_pCO2_daily, f.lo_pCO2_daily, f.clean_unit as pCO2_Units
	FROM  vapd_daily&year._v16  A
	LEFT JOIN icu.all_urea_hi_lo_2014_2017 B ON A.patienticn =B.patienticn and a.datevalue=b.LabSpecimenDate 
	LEFT JOIN icu.all_Albumin_hi_lo_2014_2017 C ON A.patienticn =C.patienticn and a.datevalue=c.LabSpecimenDate 
	LEFT JOIN icu.all_ph_hi_lo_2014_2017 D ON A.patienticn =D.patienticn and a.datevalue=d.LabSpecimenDate
	LEFT JOIN icu.all_pO2_hi_lo_2014_2017 E ON A.patienticn =e.patienticn and a.datevalue=e.LabSpecimenDate
	LEFT JOIN icu.all_pCO2_hi_lo_2014_2017 F ON A.patienticn =f.patienticn and a.datevalue=f.LabSpecimenDate;
QUIT;

/*add hemoglobin, Hematocrit*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v18 (compress=yes) AS 
	SELECT A.*, B.hi_Hemoglobin_daily, b.lo_Hemoglobin_daily, b.clean_unit as Hemoglobin_Units,
				c.hi_Hematocrit_daily, c.lo_Hematocrit_daily, c.clean_unit as Hematocrit_Units
	FROM  vapd_daily&year._v17  A
	LEFT JOIN icu.all_Hemoglobin_hi_lo_2014_2017 B ON A.patienticn =B.patienticn and a.datevalue=b.LabSpecimenDate 
	LEFT JOIN icu.all_Hematocrit_hi_lo_2014_2017 C ON A.patienticn =C.patienticn and a.datevalue=c.LabSpecimenDate;
QUIT;

/*add vitals*/
/*first run the vaitals SAS program saved in the Vitals folder in Github*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v19 (compress=yes)  AS
	SELECT A.*, B.hi_Systolic_daily, b.lo_Systolic_daily , b.hi_Diastolic_daily, b.lo_Diastolic_daily, b.hi_MAP_daily, b.lo_MAP_daily, 
	          c.hi_pulse_daily, c.lo_pulse_daily, d.hi_RESPIRATION_daily, d.lo_RESPIRATION_daily, e.hi_Temp_daily, e.lo_Temp_daily
	FROM  vapd_daily&year._v18   A
	LEFT JOIN icu.all_bp_hi_lo_2014_2017  B ON a.patienticn=b.patienticn and A.datevalue =B.Vital_date 
	LEFT JOIN icu.pulse_2014_2017  C ON a.patienticn=c.patienticn and A.datevalue =c.Vital_date 
	LEFT JOIN icu.rr_2014_2017  D ON a.patienticn=d.patienticn and A.datevalue =d.Vital_date 
	LEFT JOIN icu.temp_2014_2017  E ON a.patienticn=e.patienticn and A.datevalue =e.Vital_date ;
QUIT;

/*add bicarbonate*/
PROC SQL;
	CREATE TABLE vapd_daily&year._v20 (compress=yes)  AS
	SELECT A.*, B.hi_Bicarb_daily, b.lo_Bicarb_daily, b.clean_unit
	FROM  vapd_daily&year._v19  A
	LEFT JOIN icu.all_bicarb_hi_lo_2014_2017 B ON a.patienticn=b.patienticn and A.datevalue =B.LabSpecimenDate ;
QUIT;

/*add daily indicators: ABG_mech_vent, mechanical ventilation, dialysis*/
/*First, run the step 1 of pulling procedure codes and then step 2 identifying mechanical ventilation and dialysis indicators, saved in the Procedure & Diagnosis folder*/
PROC SQL;
	CREATE TABLE vapd_daily&year._v21  (compress=yes)  AS 
	SELECT A.*, B.abg_mechvent as abglab_mechvent_daily 
	FROM  vapd_daily&year._v20   A
	LEFT JOIN lab.abg_mechvent_2014_2017_V4  B ON A.patienticn =B.patienticn and a.datevalue=b.LabSpecimenDate;
QUIT;

PROC SQL;
	CREATE TABLE  vapd_daily&year._v22 (compress=yes)  AS
	SELECT A.*, c.mechvent as proccode_mechvent_daily
	FROM  vapd_daily&year._v21   A
	LEFT JOIN icu.mech_vent_2014_2017 C ON A.patienticn=c.patienticn and a.datevalue=c.procdate;
QUIT;

PROC SQL;
	CREATE TABLE  vapd_daily&year._v23 (compress=yes)  AS
	SELECT A.*, c.Dialysis as proccode_dialysis_daily
	FROM  vapd_daily&year._v22   A
	LEFT JOIN icu.dialysis_2014_2017 c ON A.patienticn =c.patienticn and a.datevalue=c.procdate;
QUIT;

/*save a permanent dataset for processing CDC EHR definition of sepsis*/
DATA  icu.vapd_daily&year._v23 (compress=yes);
SET  vapd_daily&year._v23 ;
RUN;

/*find mechvent =1 on hospitalization level*/
data mechvent_hosp; 
set vapd_daily&year._v23;
if proccode_mechvent_daily=1;
run;


/*save a mechanical ventilation on hospitalization level for running Angus definition of sepsis code later*/
PROC SORT DATA=mechvent_hosp  nodupkey  OUT=icu.VAPD_mechvent_hosp_06292018 (compress=yes keep=patienticn sta6a newadmitdate newdischargedate proccode_mechvent_daily rename=proccode_mechvent_daily=proccode_mechvent_hosp); /* 36854/1861336=1.97% of hosp on mech vent*/
BY  patienticn sta6a newadmitdate newdischargedate;
RUN;


/*First, run the CDC EHR Sepsis codes separately, code saved in Procedure & Diagnosis folder in Github*/

/*add CDC EHR sepsis indicators*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v24 (compress=yes) AS 
	SELECT A.*, B.hosp_sepsis, b.comm_sepsis, b.Infection,  b.hosp_infection, b.comm_infection, b.hospcomm_sepsis
	FROM  vapd_daily&year._v23 A
	LEFT JOIN icu.basic_sepsis_v2  B
	ON a.inpatientsid=b.Admission_ID and A.patienticn=B.patient_id and a.datevalue=b.datevalue and a.newadmitdate=b.newadmitdate;
QUIT;

/*First, run the Angus definition of sepsis code separately, SAS code saved in Procedure & Diagnosis folder in Github*/
/*Add Angus definition of sepsis indicator*/
PROC SQL;
	CREATE TABLE   vapd_daily&year._v25 (compress=yes)  AS 
	SELECT A.*, B.infection as angus_def_infection, b.organ_dysfunction as angus_organ_dysfunction, b.explicit_dx as angus_explicit_dx,
	     b.implicit_dx as angus_implicit_dx , b.Angus as Angus_def_sepsis
	FROM  vapd_daily&year._v24   A
	LEFT JOIN  icu.vapd_angus_sepsis B
	ON A.patienticn =B.patienticn and a.newadmitdate=b.newadmitdate  and a.newdischargedate =b.newdischargedate 
		and a.specialtytransferdate=b.specialtytransferdate and a.specialtydischargedate=b.specialtydischargedate and a.sta6a=b.sta6a;
QUIT;

/*calculate sum icu days per hosp*/
PROC SQL;
CREATE TABLE vapd_daily&year._v26 (compress=yes)  AS 
SELECT *, sum(icu) as sum_icu_hosp
FROM vapd_daily&year._v25
GROUP BY patienticn, newadmitdate, newdischargedate;
QUIT;

/*calculate sum icu days per specialty stay (bedsection) */
PROC SQL;
CREATE TABLE vapd_daily&year._v27 (compress=yes)  AS 
SELECT *, sum(icu) as sum_icu_bedsection /*at each speciality stay in the ICU*/
FROM vapd_daily&year._v26
GROUP BY patienticn, specialtytransferdate, specialtydischargedate ;
QUIT;

/*calculate SOFA scores*/
DATA vapd_daily&year._v27 (compress=yes);
SET vapd_daily&year._v27;
if hi_bili_daily='.' then Liver_SOFA=.;
    else if hi_bili_daily <1.2 then Liver_SOFA=0;
    else if 1.2<=hi_bili_daily <2.0 then Liver_SOFA=1;
    else if 2.0<=hi_bili_daily <6.0 then Liver_SOFA=2;
    else if 6.0<=hi_bili_daily <12.0 then Liver_SOFA=3;
    else if hi_bili_daily>12.0 then Liver_SOFA=4;
if  hi_creat_daily = '.' then Renal_SOFA=.;
 	else if  hi_creat_daily<1.2 then Renal_SOFA=0;
 	else if 1.2=< hi_creat_daily<2.0 then Renal_SOFA=1;
 	else if 2.0=< hi_creat_daily<3.5 then Renal_SOFA=2;
 	else if 3.5=< hi_creat_daily<5.0 then Renal_SOFA=3;
 	else if  hi_creat_daily>5.0 then Renal_SOFA=4;
if lo_plate_daily='.' then Coagulation_SOFA=.;
 	else if lo_plate_daily >150 then Coagulation_SOFA=0;
 	else if 101=<lo_plate_daily<151 then Coagulation_SOFA=1;
 	else if 51=<lo_plate_daily<101 then Coagulation_SOFA=2;
 	else if 21=<lo_plate_daily<51 then Coagulation_SOFA=3;
 	else if lo_plate_daily<=20 then Coagulation_SOFA=4;
if pressor_1 NE '' or pressor_2 NE '' or pressor_3 NE '' or pressor_4 NE '' or pressor_5 NE '' then any_pressor=1; 
else any_pressor=0; /*Cardio SOFA is based on vassopressors only due to limited data*/
if any_pressor=1 then Cardio_SOFA=3.5; else Cardio_SOFA=0;
RUN;

/*Calculate Elixhauser using weighted Van Walraven scale*/
DATA vapd_daily&year._v28 (compress=yes); 
SET  vapd_daily&year._v27;
/*replace missing values with 0*/
if ELX_GRP_1 NE 1 then ELX_GRP_1=0; if ELX_GRP_2 NE 1 then ELX_GRP_2=0; if ELX_GRP_3 NE 1 then ELX_GRP_3=0; if ELX_GRP_4 NE 1 then ELX_GRP_4=0; if ELX_GRP_5 NE 1 then ELX_GRP_5=0;
if ELX_GRP_6 NE 1 then ELX_GRP_6=0; if ELX_GRP_7 NE 1 then ELX_GRP_7=0; if ELX_GRP_8 NE 1 then ELX_GRP_8=0; if ELX_GRP_9 NE 1 then ELX_GRP_9=0; if ELX_GRP_10 NE 1 then ELX_GRP_10=0;
if ELX_GRP_11 NE 1 then ELX_GRP_11=0; if ELX_GRP_12 NE 1 then ELX_GRP_12=0; if ELX_GRP_13 NE 1 then ELX_GRP_13=0; if ELX_GRP_14 NE 1 then ELX_GRP_14=0; if ELX_GRP_15 NE 1 then ELX_GRP_15=0;
if ELX_GRP_16 NE 1 then ELX_GRP_16=0; if ELX_GRP_17 NE 1 then ELX_GRP_17=0; if ELX_GRP_18 NE 1 then ELX_GRP_18=0; if ELX_GRP_19 NE 1 then ELX_GRP_19=0; if ELX_GRP_20 NE 1 then ELX_GRP_20=0;
if ELX_GRP_21 NE 1 then ELX_GRP_21=0; if ELX_GRP_22 NE 1 then ELX_GRP_22=0; if ELX_GRP_23 NE 1 then ELX_GRP_23=0; if ELX_GRP_24 NE 1 then ELX_GRP_24=0; if ELX_GRP_25 NE 1 then ELX_GRP_25=0;
if ELX_GRP_26 NE 1 then ELX_GRP_26=0; if ELX_GRP_27 NE 1 then ELX_GRP_27=0; if ELX_GRP_28 NE 1 then ELX_GRP_28=0; if ELX_GRP_29 NE 1 then ELX_GRP_29=0; if ELX_GRP_30 NE 1 then ELX_GRP_30=0; if ELX_GRP_31 NE 1 then ELX_GRP_31=0;
 elixhauser_VanWalraven=sum(7*ELX_GRP_1, -1*ELX_GRP_3, 4*ELX_GRP_4, 2*ELX_GRP_5, 0*ELX_GRP_7, 7*ELX_GRP_8, 6*ELX_GRP_9, 3*ELX_GRP_10, 5*ELX_GRP_14, 11*ELX_GRP_15, 9*ELX_GRP_18,
      12*ELX_GRP_19, 4*ELX_GRP_20,3*ELX_GRP_21, -4*ELX_GRP_23, 6*ELX_GRP_24, 5*ELX_GRP_25, -2*ELX_GRP_26, -2*ELX_GRP_27, -7*ELX_GRP_29, -3*ELX_GRP_31);
RUN;

/*get 365-day mortality info, re-pull Date of Death (DOD), half of 2017 data will have missing 365-day mortality information due to date of pull was in July 2018*/
PROC SQL ;   
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table  icu.dod_07032018_pull (compress=yes) as select * FROM CONNECTION TO CDW1 
(SELECT t1.dod as dod_07032018_pull, b.patienticn
FROM [&Studyname.].[Src].[VitalStatus_Mini] as t1
left join [&Studyname.].[Src].[CohortCrosswalk]) as t1
where t1.dod is not null;
DISCONNECT FROM CDW1 ;
quit; 

/*change patienticn into numeric*/  
DATA icu.dod_07032018_pull (rename=patienticn2=patienticn);
SET icu.dod_07032018_pull ;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

PROC SORT DATA=temp.dod_07032018_pull  nodupkey; 
BY  patienticn;
RUN;

PROC SQL;
	CREATE TABLE vapd_daily&year._v29  (compress=yes)  AS
	SELECT A.*, B.dod_07032018_pull
	FROM   vapd_daily&year._v28  A
	LEFT JOIN icu.dod_07032018_pull  B ON A.patienticn =B.patienticn ;
QUIT;

data  vapd_daily&year._v29 (compress=yes);
set vapd_daily&year._v29;
if  dod_07032018_pull<newadmitdate then  dod_07032018_pull=.; 
run;

/*calculate 365-day mortality and censor variable (missing=1, not missing=0*/
data vapd_daily&year._v30 (compress=yes);
set vapd_daily&year._v29;
if  dod_07032018_pull NE '.' then do 
	doddaysafterdischarge=datdif(newdischargedate, dod_07032018_pull, 'act/act'); 
end;
else doddaysafterdischarge='.';
if not missing(doddaysafterdischarge) and (doddaysafterdischarge)<366 and inhospmort=0 then mort_365day=1; else mort_365day=0; /*If death occurred after discharge and within 365 days of discharge*/
if  dod_07032018_pull NE . then mort_censoring=0; else mort_censoring=1;
label mort_censoring = 'if date of death is missing then mort_censoring=1';
run;

/*save final dataset with date*/
DATA  icu.vapd_daily&year._&todaysDate. (compress=yes); 
SET  vapd_daily&year._v30;
RUN;




/****************************************************************************************************************************************/
