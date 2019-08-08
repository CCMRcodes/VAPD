/*Step 2: turning the acute hospitalizations into daily rows*/

/* Date Modified: 08/06/2019
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
/*Due to CDW EHR data error, look for and also delete some embedded hospitalization dates*/
data icu.allacute&year._date;
set icu.allacute&year._date;
format specialtytransferdate specialtydischargedate date9.;
/*must hand code in some embedded hospitalization dates and delete them from dataset*/
if (patienticn ='###' and new_dischargedate2='DDMONYEAR'd) or 
   (patienticn ='###' and new_dischargedate2='DDMONYEAR'd ) then delete;
run;

/*Create a row for each calendar day at a given STA6A*/
data icu.vapd_daily&year.;
set icu.allacute&year._date;
do datevalue=specialtytransferdate to specialtydischargedate;
	datevalue=datevalue; output;
end;
format datevalue mmddyy10.;
run;

/*creat hierarchy for specialty*/
data vapd_daily&year.; 
set icu.vapd_daily&year.;
/*specialty ranking order to prioritize which duplicate pat-fac-days to keep*/
if icu=1 then specialty_priority_order =1; /*all ICU*/
 else if specialty in ('MEDICAL OBSERVATION','SURGICAL OBSERVATION','ED OBSERVATION','NEUROLOGY OBSERVATION') 
    then specialty_priority_order =3; /*all Observations*/
  else specialty_priority_order =2; /*all other acute specialities*/
RUN;

PROC SORT DATA= vapd_daily&year. (compress=yes); 
BY patienticn sta6a  datevalue new_admitdate2 new_dischargedate2 specialty_priority_order;
RUN;

/*Create a an indicator for number of specialties each day*/
data vapd_daily&year.; 
set vapd_daily&year.;
by patienticn sta6a datevalue;
pat_day_spec_count+1;
if first.datevalue then pat_day_spec_count=1;
run;

/*Keep the first pat_day_spec_count*/
data vapd_daily&year._v2; 
set vapd_daily&year.;
where pat_day_spec_count=1;
run;

/*Count hospital day within a hospitalization*/
PROC SORT DATA=vapd_daily&year._v2  nodupkey OUT=vapd_daily&year._v3 (compress=yes); 
BY  patienticn sta6a new_admitdate2 new_dischargedate2 datevalue;
run;

data vapd_daily&year._v4 (compress=yes rename=patienticn2=patienticn);  
set vapd_daily&year._v3;
by patienticn sta6a new_admitdate2 new_dischargedate2;
if first.new_admitdate2 then hospital_day=0;
hospital_day+1; /*create hospital_day count for each hospitalization*/
admityear=year(new_admitdate2); 
if admityear=2018  then delete;
patienticn2 = input(patienticn, 10.); /*change patienticn into numeric*/  
drop patienticn;
run;

/*check hospital_day=1 and indicator to see if they match up*/
DATA hospital_day1 (compress=yes);
SET vapd_daily&year._v4;
if hospital_day=1;
RUN;

/*get # of unique hospitalizations only*/
PROC SORT DATA=vapd_daily&year._v4 nodupkey  OUT=unique_sshosp_2014_2017 (compress=yes); 
BY  patienticn sta6a new_admitdate2 new_dischargedate2;
RUN;

PROC SORT DATA=vapd_daily&year._v4 nodupkey dupout=dups OUT=unique_sshosp_2014_2017_v2 (compress=yes);   
BY  patienticn sta6a new_admitdate2 /*new_dischargedate2*/;
RUN;

/*separately calculate 30-day readmit rate, note this is the "is this hospitalization a 30-day readmit, instead of will
this hospitalization follow by another readmit in 30 days*/
/*get unique hospitalizations only, don't need to include facility*/
proc sort data=vapd_daily&year._v4  nodupkey 
out=unique_hosp_readmit30 (keep=patienticn new_admitdate2 new_dischargedate2);  
by patienticn new_admitdate2 new_dischargedate2;
run;

data part1; 
retain patienticn new_admitdate2 new_dischargedate2;
set unique_hosp_readmit30;
by PatientICN new_admitdate2 new_dischargedate2;
lag_discharge=lag(new_dischargedate2); /*retrieve the value of the previous discharge date*/
format lag_discharge mmddyy10.;
gap=new_admitdate2-lag_discharge; /*calculte gap between the current admission date and the previous discharge date*/

if first.patienticn then do; /*reset the values of the following fields in case of a new patienticn*/
lag_discharge=.;
gap=.;
readmit30=.;
end;
if 0 <= gap <=30 then readmit30=1; /*identify a readmit and assign value of 1 to readmit30*/
run;

/*left join lag_discharge, gap and readmit30 fields back to original unique hosp dataset*/
PROC SQL;
	CREATE TABLE icu.vapd_daily20142017_v6_01022019  (compress=yes)  AS /*12317166*/ 
	SELECT A.*,  b.readmit30
	FROM vapd_daily&year._v4 A
	LEFT JOIN part1  B
	ON A.PatientICN =B.PatientICN and a.new_admitdate2=b.new_admitdate2 and a.new_dischargedate2=b.new_dischargedate2;
QUIT;


/************ USE icu.vapd_daily20142017_v6_01022019 TO RUN THE ELIXHAUSER COMORBIDITIES CODE. Save the dataset as icu.Elixhauser_2014_2017_10012018***********/
/*NOTE: must run the SAS program named VAPD_Elixhauser_Comorbid and used the final dataset saved in the Procedure & Diagnosis folder*/

/* recode diagnosis variables*/
data check_diag; 
set icu.vapd_daily20142017_v6_01022019;
if icd10code1 ='' and icd9code1 ='';
run;

/*combine icd9 and icd10 diagnosis fields, shorten the number of diagnosis fields*/
/*want diagnosis on a daily level*/
data diag_daily (compress=yes); 
set icu.vapd_daily20142017_v6_01022019;
keep patienticn sta3n sta6a datevalue  specialtytransferdate  specialtydischargedate icdtype icd9code1-icd9code25  icd10code1-icd10code26;
run;

/*take out decimals in icd10 and icd9 diag codes*/
/*data  diag_daily2 (compress=yes); */
/*set  diag_daily;*/
/*icd10code1=compress(icd10code1,'.'); icd10code2=compress(icd10code2,'.'); icd10code3=compress(icd10code3,'.');icd10code4=compress(icd10code4,'.');icd10code5=compress(icd10code5,'.');*/
/*icd10code6=compress(icd10code6,'.');icd10code7=compress(icd10code7,'.');icd10code8=compress(icd10code8,'.');icd10code9=compress(icd10code9,'.');icd10code10=compress(icd10code10,'.');*/
/*icd10code11=compress(icd10code11,'.'); icd10code12=compress(icd10code12,'.'); icd10code13=compress(icd10code13,'.');icd10code14=compress(icd10code14,'.');icd10code15=compress(icd10code15,'.');*/
/*icd10code16=compress(icd10code16,'.');icd10code17=compress(icd10code17,'.');icd10code18=compress(icd10code18,'.');icd10code19=compress(icd10code19,'.');icd10code20=compress(icd10code20,'.');*/
/*icd10code21=compress(icd10code21,'.'); icd10code22=compress(icd10code22,'.'); icd10code23=compress(icd10code23,'.');icd10code24=compress(icd10code24,'.');icd10code25=compress(icd10code25,'.');*/
/*icd10code26=compress(icd10code26,'.');*/
/*icd9code1=compress(icd9code1,'.'); icd9code2=compress(icd9code2,'.'); icd9code3=compress(icd9code3,'.');icd9code4=compress(icd9code4,'.');icd9code5=compress(icd9code5,'.');*/
/*icd9code6=compress(icd9code6,'.');icd9code7=compress(icd9code7,'.');icd9code8=compress(icd9code8,'.');icd9code9=compress(icd9code9,'.');icd9code10=compress(icd9code10,'.');*/
/*icd9code11=compress(icd9code11,'.'); icd9code12=compress(icd9code12,'.'); icd9code13=compress(icd9code13,'.');icd9code14=compress(icd9code14,'.');icd9code15=compress(icd9code15,'.');*/
/*icd9code16=compress(icd9code16,'.');icd9code17=compress(icd9code17,'.');icd9code18=compress(icd9code18,'.');icd9code19=compress(icd9code19,'.');icd9code20=compress(icd9code20,'.');*/
/*icd9code21=compress(icd9code21,'.'); icd9code22=compress(icd9code22,'.'); icd9code23=compress(icd9code23,'.');icd9code24=compress(icd9code24,'.');icd9code25=compress(icd9code25,'.');*/
/*run;*/

/*decided not to take out the decimal for the VAPD dataset. BUT to run ELIXHAUSER COMORBIDITIES CODE, must take out the decimal*/

/*separate data into icd9 and icd10*/
DATA icd9 (keep=patienticn sta6a icdtype specialtytransferdate datevalue  icd9code1-icd9code25)  
     icd10 (keep=patienticn sta6a  icdtype specialtytransferdate datevalue  icd10code1-icd10code26) ;
SET  diag_daily;
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
SET icd9_V2 icd10_V2 ;
RUN;

PROC SORT DATA=all_diag nodupkey; 
BY patienticn sta6a icdtype specialtytransferdate datevalue  ;
RUN;

/*left join renamed fields back to dataset temp.vapd_daily&year._v2, compare/check the renamed fields with the original field to make sure they're
right before dropping the original/old diag fields*/
PROC SQL;
	CREATE TABLE vapd_daily&year._v6 (compress=yes)  AS 
	SELECT A.*, B.DiagnosisCode_1, b.DiagnosisCode_2, b.DiagnosisCode_3, B.DiagnosisCode_4, b.DiagnosisCode_5, b.DiagnosisCode_6, B.DiagnosisCode_7, b.DiagnosisCode_8, b.DiagnosisCode_9,
		B.DiagnosisCode_10, B.DiagnosisCode_11, b.DiagnosisCode_12, b.DiagnosisCode_13, B.DiagnosisCode_14, b.DiagnosisCode_15, b.DiagnosisCode_16, B.DiagnosisCode_17, b.DiagnosisCode_18, b.DiagnosisCode_19,
		B.DiagnosisCode_20,B.DiagnosisCode_21,B.DiagnosisCode_22,B.DiagnosisCode_23,B.DiagnosisCode_24,B.DiagnosisCode_25,B.DiagnosisCode_26
	FROM  icu.vapd_daily20142017_v6_01022019   A
	LEFT JOIN all_diag  B
	ON A.patienticn =B.patienticn and a.sta6a=b.sta6a and a.specialtytransferdate=b.specialtytransferdate 
	 and a.datevalue=b.datevalue and a.icdtype=b.icdtype ;
QUIT;

/*add the 31 elixhauser comorbids*/
/*NOTE: must run the SAS program named VAPD_Elixhauser_Comorbid and used the final dataset saved in the Procedure & Diagnosis folder. Must take out the decimals for the diagnosis codes.*/
PROC SQL;
	CREATE TABLE vapd_daily&year._v6b  (compress=yes)  AS 
	SELECT A.*, b.sum_Elixhauser_count, B.ELX_GRP_1, B.ELX_GRP_2, B.ELX_GRP_3, B.ELX_GRP_4, B.ELX_GRP_5, B.ELX_GRP_6, B.ELX_GRP_7, B.ELX_GRP_8, B.ELX_GRP_9, B.ELX_GRP_10, 
	B.ELX_GRP_11, B.ELX_GRP_12, B.ELX_GRP_13, B.ELX_GRP_14, B.ELX_GRP_15, B.ELX_GRP_16, B.ELX_GRP_17, B.ELX_GRP_18, B.ELX_GRP_19, B.ELX_GRP_20, B.ELX_GRP_21, B.ELX_GRP_22, B.ELX_GRP_23, 
	B.ELX_GRP_24, B.ELX_GRP_25, B.ELX_GRP_26, B.ELX_GRP_27, B.ELX_GRP_28, B.ELX_GRP_29, B.ELX_GRP_30, B.ELX_GRP_31
	FROM   vapd_daily&year._v6  A
	LEFT JOIN icu.Elixhauser_2014_2017_10012018  B  /*this dataset was ran using the ELIXHAUSER COMORBIDITIES CODE, commented above*/
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

PROC SQL;
	CREATE TABLE  vapd_daily&year._v6c (compress=yes)  AS
	SELECT A.*, B.new_race as Race, C.gender, C.dob
	FROM vapd_daily&year._v6b   A
	LEFT JOIN icu.patsub_patientrace  B ON A.PatientSID =B.PatientSID and a.sta3n=b.sta3n  
	LEFT JOIN icu.SPatient_gender_dob  c ON A.PatientSID =c.PatientSID and a.sta3n=c.sta3n ;
QUIT;

/*check for missing race, gender and dob*/
data race  gender   dob ;
set vapd_daily&year._v6c;
if race='' then output race;
if gender = '' then output gender;
if dob =. then output dob;
run;

DATA  vapd_daily&year._v6c (compress=yes);
SET vapd_daily&year._v6c;
age=(INT((new_admitdate2-dob)/365.25)); 
hosp_LOS =(new_dischargedate2-new_admitdate2)+1;
RUN;

/*look at sta3n frequencies, especially for sta3n 608*/
PROC FREQ DATA=vapd_daily&year._v6c  order=freq;
TABLE  sta3n; /*N=20 pat-days for 608, 17 pat-days for 529, and 4 pat-days for 679*/
RUN;

DATA sta3n_check_only; /*n=41, all MEDICAL OBSERVATION*/
SET  vapd_daily&year._v6c;
if sta3n in (608,529,679);
RUN;

PROC SORT DATA=sta3n_check_only ; /*sta3n 529 all 2015 of Transplantation, 608 has 2014, 2015, 2016 all med obs, 679 has pulmonary, 2014 & 2016*/ 
BY  sta3n admityear;
RUN;

/*On 10/17/18, PIs suggested dropping sta3ns: 679, 608, 539, due to less than 21 patient-facility-days in the VAPD each.  
New Orleans actually existed in the past but was destroyed in Hurricane Katrina and a brand-new facility was re-opened 2017. Decision: Keep 629 & 675.*/
DATA icu.vapd_daily&year._v5_20190102 (compress=yes); 
SET vapd_daily&year._v6c ;
if sta3n in (608, 529, 679) then delete;
RUN;

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
	CREATE TABLE vapd_daily&year._v6d  (compress=yes)  AS /*  12240459 */
	SELECT A.*, B.DispositionType
	FROM  icu.vapd_daily&year._v5_20190102  A
	LEFT JOIN  icu.dispotype2014_2017 B
	ON A.PatientSID =b.PatientSID and a.sta3n=b.sta3n and a.InpatientSID=b.inpatientsid ;
QUIT;

/*1 - Home,   2 - Transfer to another acute care hospital, 3 - Subacute facility, 4 - Death, 5 - Hospice*/
data vapd_daily&year._v6d (compress=yes); /*  12240459 */
set vapd_daily&year._v6d;
if DispositionType=1 then Discharge_dispo=1;
if DispositionType=2 then Discharge_dispo=2;
if DispositionType=4 then Discharge_dispo=1;
if DispositionType=5 then Discharge_dispo=2;
if DispositionType=6 then Discharge_dispo=4;
if DispositionType=7 then Discharge_dispo=4;
run;


/*drop some variables and those age < 18 and some sta6a facilities*/
DATA vapd_daily&year._v6d (compress=yes); 
SET  vapd_daily&year._v6d;
if age < 18 then delete;
/*list the sta6a facilities out for future record of what's deleted, so do step by step instead of optimizing code*/
/*1/7/2019: Jack said to exclude all those sta6as he indicated as NHC, DOM, RRTP*/
if sta6a in ('5089AB', '5289AB', '5289AL', '5379AA', '6269AB','6409AA','6649AA', '508BU','516BU','520BU','537BU','613BU','553BU',
'554BU', '557BU','546BU','568BU','573BU', '586BU','589BU','642BU', '689BU', '691BU','590BU', '539BV', '539BU','508PA','598PA', '646PA',
'658BU', '691PB','6009AA','589BV','6409AA','626BV','539BU') then delete; /*1030 pat-fac-days deleted*/
/*During February 2019 VAPD meeting, the group decided to drop sta6a with less than 1000 patient-facility-days in the VAPD 2014-2017.*/
if sta6a in ('509A0','619A4','561A4','610','620','636A4','674A4','671A4','640A0','648A4','596','663A4','523') then delete; /*1570 deleted*/
/*missings and unknown sta6a total < 1000 days, decided to delete*/
if  sta6a in ('*Missing*','','*Unknown at this time*') then delete; /*914 pat-fac-days deleted*/
/*drop unwanted fields*/
drop  icd10code1 icd10code2 icd10code3 icd10code4 icd10code5 icd10code6 icd10code7 icd10code8 icd10code9
icd10code10 icd10code11 icd10code12 icd10code13 icd10code14 icd10code15 icd10code16 icd10code17 icd10code18
icd10code19 icd10code20 icd10code21 icd10code22 icd10code23 icd10code24 icd10code25 icd10code26 icd9code1
icd9code2 icd9code3 icd9code4 icd9code5 icd9code6 icd9code7 icd9code8 icd9code9 icd9code10 icd9code11 icd9code12
icd9code13 icd9code14 icd9code15 icd9code16 icd9code17 icd9code18 icd9code19 icd9code20 icd9code21 icd9code22
icd9code23 icd9code24 icd9code25 cdw_admitdatetime cdw_dischargedatetime newdischargedatetime newadmitdatetime
newadmitdate newdischargedate specialtyvisit firstvisit lastvisit issue error previous_admit next_admit flag2
losinservice admitdatetime dischargedatetime deathdaysafterdischarge deathdaysafteradmit specialtydischargedatetime
mean_acute nextadmdate previousdisdate previousspecdis nextspecadm unique_hosp lag_specialtydischargedate diff_days
lag_sta6a first_pat new_hosp_ind Unique_hosp_ind pat_day_spec_count specialtytransferdatetime acute  flag specialtytransfer;
RUN;

/*check facility freq, all greater than 1000 pat-fac-days*/
/*PROC FREQ DATA=vapd_daily&year._v6d  order=freq;*/
/*TABLE sta6a;*/
/*RUN;*/

/*************************************** Add Clinical Data to VAPD ********************************************************************/
/*add  microbiology labs and culture indicators*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v7 (compress=yes)  AS 
	SELECT A.*, B.hasmicro as Blood_cult_ind, b.collectionsample as bloodcult_collection_sample, b.topography as bloodcult_topography,
	   c.hasmicro as Other_Micro_ind, c.collectionsample as othermicro_collection_sample, c.topography as othermicro_topography
	FROM  vapd_daily&year._v6d A
	LEFT JOIN  sepsis.has_bloodcult B ON A.patienticn =B.patienticn and a.datevalue=b.dv1 
    LEFT JOIN sepsis.has_oth_micro C on A.patienticn =c.patienticn and a.datevalue=c.dv1 ;
QUIT;

/*add medications*/
/**** Vasopressors ****/
/*Add in the pressors for Cardiovascular SOFA*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v8 (compress=yes)   AS 
	SELECT A.*, B.drugname_1 as pressor_1, b.drugname_2 as pressor_2, b.drugname_3 as pressor_3, b.drugname_4 as pressor_4, b.drugname_5 as pressor_5
	FROM   vapd_daily&year._v7  A
	LEFT JOIN  pharm.trans_pressors2014201_5292018 B
	ON A.patienticn =B.patienticn and a.datevalue=b.actiondate ;
QUIT;

/*** add sedatives, Paralytic and Analgesic ***/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v9  (compress=yes)  AS 
	SELECT A.*, B.drugname_1,B.drugname_2,B.drugname_3,B.drugname_4,B.drugname_5,B.drugname_6,B.drugname_7,B.drugname_8,B.drugname_9
	FROM   vapd_daily&year._v8   A
	LEFT JOIN pharm.transallsedative20142017_5292018  B
	ON A.patienticn =B.patienticn and a.datevalue=b.actiondate;
QUIT;

/*Antibiotics*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v10 (compress=yes)  AS  
	SELECT A.*, B.abx1, B.abx2,B.abx3,B.abx4,B.abx5,B.abx6,B.abx7,B.abx8,B.abx9,B.abx10,B.abx11,B.abx12,B.abx13,B.abx14,B.abx15,B.abx16,B.abx17,B.abx18,B.abx19,B.abx20
	FROM   vapd_daily&year._v9  A
	LEFT JOIN  pharm.all_abx_num_07132018 B
	ON A.patienticn =B.patienticn and a.datevalue=b.actiondate;
QUIT;


/*12/6/18: After checking patients on mechanical ventilation but have no pH labs, Hallie decided to use the new method of pulling labs by excluding the "LOINCSID=-1" from SQL code.
Also for pH and PaCO2 labs, it is only to keep topography of mixed arterial and venuous blood.*/

/*** Add new labs: 8/28/18 pull ***/
/*lactate, platelets, bilirubin, and creatinine lab hi and lo values*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v11 (compress=yes) AS 
	SELECT A.*, B.hi_lactate_daily, b.lo_lactate_daily, b.clean_unit as lactate_Units,
				c.hi_plate_daily, c.lo_plate_daily, c.clean_unit as plate_Units,
				d.hi_creat_daily, d.lo_creat_daily, d.clean_unit as creat_Units,
				e.hi_bili_daily, e.lo_bili_daily, e.clean_unit as bili_Units
	FROM vapd_daily&year._v10  A
	LEFT JOIN lab.lactate_hi_lo_20142015_120718 B ON A.patienticn =B.patienticn and a.datevalue=b.LabSpecimenDate 
	LEFT JOIN lab.plat_hi_lo_20142017_111218 C ON A.patienticn =C.patienticn and a.datevalue=c.LabSpecimenDate 
	LEFT JOIN lab.creat_hi_lo_20142017_082818 D ON A.patienticn =D.patienticn and a.datevalue=d.LabSpecimenDate
	LEFT JOIN lab.bili_hi_lo_20142017_08282018  E ON A.patienticn =e.patienticn and a.datevalue=e.LabSpecimenDate;
QUIT;

/*add WBC, glucose, sodium, potassium*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v12 (compress=yes) AS 
	SELECT A.*, B.hi_WBC_daily, b.lo_WBC_daily, b.clean_unit as WBC_Units,
				c.hi_Glucose_daily, c.lo_Glucose_daily, c.clean_unit as Glucose_Units,
				d.hi_sodium_daily, d.lo_sodium_daily, d.clean_unit as sodium_Units,
				e.hi_potassium_daily, e.lo_potassium_daily, e.clean_unit as potassium_Units
	FROM  vapd_daily&year._v11  A
	LEFT JOIN lab.wbc_hi_lo_20142017_082818 B ON A.patienticn =B.patienticn and a.datevalue=b.LabSpecimenDate 
	LEFT JOIN lab.Glucose_hi_lo_20142017_101118 C ON A.patienticn =C.patienticn and a.datevalue=c.LabSpecimenDate 
	LEFT JOIN lab.sodium_hi_lo_20142017_082818 D ON A.patienticn =D.patienticn and a.datevalue=d.LabSpecimenDate
	LEFT JOIN lab.potass_hi_lo_20142017_082818 E ON A.patienticn =e.patienticn and a.datevalue=e.LabSpecimenDate;
QUIT;

/*add Urea, Albumin, pH, PO2, pCO2*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v13 (compress=yes) AS 
	SELECT A.*, B.hi_urea_daily, b.lo_urea_daily, b.clean_unit as urea_Units,
				c.hi_albumin_daily, c.lo_albumin_daily, c.clean_unit as albumin_Units,
				d.hi_pH_daily, d.lo_pH_daily, d.clean_unit as ph_Units,
				e.hi_pO2_daily, e.lo_pO2_daily, e.clean_unit as pO2_Units,
			    f.hi_pCO2_daily, f.lo_pCO2_daily, f.clean_unit as pCO2_Units
	FROM  vapd_daily&year._v12  A
	LEFT JOIN lab.urea_hi_lo_20142017_111218 B ON A.patienticn =B.patienticn and a.datevalue=b.LabSpecimenDate 
	LEFT JOIN lab.albumin_hi_lo_20142017_082818 C ON A.patienticn =C.patienticn and a.datevalue=c.LabSpecimenDate 
	LEFT JOIN lab.ph_hi_lo_20142017_12062018 D ON A.patienticn =D.patienticn and a.datevalue=d.LabSpecimenDate
	LEFT JOIN lab.Po2_hi_lo_20142017_10112018 E ON A.patienticn =e.patienticn and a.datevalue=e.LabSpecimenDate
	LEFT JOIN lab.pco2_hi_lo_20142017_12062018 F ON A.patienticn =f.patienticn and a.datevalue=f.LabSpecimenDate;
QUIT;

/*also add in Procalcitonin, BNP, Troponin labs*/
libname Procal 'FOLDER PATH';
libname bnp 'FOLDER PATH';
libname Troponin 'FOLDER PATH';

/*add hemoglobin, Hematocrit & bicarbonate, Procalcitonin, BNP, and Troponin labs */
PROC SQL;
	CREATE TABLE  vapd_daily&year._v14 (compress=yes) AS 
	SELECT A.*, B.hi_Hemoglobin_daily, b.lo_Hemoglobin_daily, b.clean_unit as Hemoglobin_Units,
				c.hi_Hematocrit_daily, c.lo_Hematocrit_daily, c.clean_unit as Hematocrit_Units,
				d.hi_Bicarb_daily, d.lo_Bicarb_daily, d.clean_unit as bicarb_units,
				e.hi_procalcitonin_daily, e.lo_procalcitonin_daily, d.clean_unit as Procalcitonin_units,
				f.hi_bnp_uln_daily as hi_bnp_daily, f.lo_bnp_uln_daily as lo_bnp_daily, f.clean_unit as bnp_units,
				g.hi_trop_uln_daily as hi_trop_daily, g.lo_trop_uln_daily as lo_trop_daily, g.clean_unit as Troponin_units
	FROM  vapd_daily&year._v13  A
	LEFT JOIN lab.hemoglobin_hi_lo_20142017_101118 B ON A.patienticn =B.patienticn and a.datevalue=b.LabSpecimenDate 
	LEFT JOIN lab.hematocrit_hi_lo_20142017_101118 C ON A.patienticn =C.patienticn and a.datevalue=c.LabSpecimenDate
    LEFT JOIN lab.bicarb_hi_lo_20142017_082818 d ON a.patienticn=d.patienticn and A.datevalue =d.LabSpecimenDate
    left join Procal.ALL_PROCAL_HI_LO_2014_2017 e on a.patienticn=e.patienticn and A.datevalue =e.LabSpecimenDate
    left join bnp.ALL_BNP_HI_LO_2014_2017 f on a.patienticn=f.patienticn and A.datevalue =f.LabSpecimenDate
    left join troponin.ALL_TROP_HI_LO_2014_2017 g on  a.patienticn=g.patienticn and A.datevalue =g.LabSpecimenDate;
QUIT;

/*add vitals*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v15 (compress=yes)  AS 
	SELECT A.*, B.hi_Systolic_daily, b.lo_Systolic_daily , b.hi_Diastolic_daily, b.lo_Diastolic_daily, b.hi_MAP_daily, b.lo_MAP_daily, 
	          c.hi_pulse_daily, c.lo_pulse_daily, d.hi_RESPIRATION_daily, d.lo_RESPIRATION_daily, e.hi_Temp_daily, e.lo_Temp_daily
	FROM vapd_daily&year._v14  A
	LEFT JOIN vital.all_bp_hi_lo_2014_2017  B ON a.patienticn=b.patienticn and A.datevalue =B.Vital_date 
	LEFT JOIN vital.pulse_2014_2017  C ON a.patienticn=c.patienticn and A.datevalue =c.Vital_date 
	LEFT JOIN vital.rr_2014_2017  D ON a.patienticn=d.patienticn and A.datevalue =d.Vital_date 
	LEFT JOIN vital.temp_2014_2017  E ON a.patienticn=e.patienticn and A.datevalue =e.Vital_date ;
QUIT;

/*add indicators:  mechanical ventilation, dialysis*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._v16 (compress=yes)  AS  
	SELECT A.*, c.mechvent as proccode_mechvent_daily,
				d.Dialysis as proccode_dialysis_daily
	FROM  vapd_daily&year._v15   A
	LEFT JOIN diag.mech_vent_2014_2017_06182018 C ON A.patienticn =c.patienticn and a.datevalue=c.procdate
	LEFT JOIN diag.dialysis_2014_2017_06182018 D ON A.patienticn =D.patienticn and a.datevalue=D.procdate;;
QUIT;

/*save a perm dataset for processing CDC EHR definition of sepsis*/
/*Calculate Elixhauser using Van Walraven scale*/
DATA  vapd_daily&year._v16b  (compress=yes); 
SET   vapd_daily&year._v16;
if elx_grp_1 NE 1 then elx_grp_1 =0; if elx_grp_2 NE 1 then elx_grp_2 =0;
if elx_grp_3 NE 1 then elx_grp_3 =0;if elx_grp_4 NE 1 then elx_grp_4 =0;
if elx_grp_5 NE 1 then elx_grp_5 =0;if elx_grp_6 NE 1 then elx_grp_6 =0;
if elx_grp_7 NE 1 then elx_grp_7 =0;if elx_grp_8 NE 1 then elx_grp_8 =0;
if elx_grp_9 NE 1 then elx_grp_9 =0;if elx_grp_10 NE 1 then elx_grp_10 =0;
if elx_grp_11 NE 1 then elx_grp_11 =0; if elx_grp_12 NE 1 then elx_grp_12 =0;
if elx_grp_13 NE 1 then elx_grp_13 =0;if elx_grp_14 NE 1 then elx_grp_14 =0;
if elx_grp_15 NE 1 then elx_grp_15 =0;if elx_grp_16 NE 1 then elx_grp_16 =0;
if elx_grp_17 NE 1 then elx_grp_17 =0;if elx_grp_18 NE 1 then elx_grp_18 =0;
if elx_grp_19 NE 1 then elx_grp_19 =0;if elx_grp_20 NE 1 then elx_grp_20 =0;
if elx_grp_21 NE 1 then elx_grp_21 =0; if elx_grp_22 NE 1 then elx_grp_22 =0;
if elx_grp_23 NE 1 then elx_grp_23 =0;if elx_grp_24 NE 1 then elx_grp_24 =0;
if elx_grp_25 NE 1 then elx_grp_25 =0;if elx_grp_26 NE 1 then elx_grp_26 =0;
if elx_grp_27 NE 1 then elx_grp_27 =0;if elx_grp_28 NE 1 then elx_grp_28 =0;
if elx_grp_29 NE 1 then elx_grp_29 =0;if elx_grp_30 NE 1 then elx_grp_30 =0;
if elx_grp_31 NE 1 then elx_grp_31 =0;
elixhauser_VanWalraven=sum(7*ELX_GRP_1, -1*ELX_GRP_3, 4*ELX_GRP_4, 2*ELX_GRP_5, 0*ELX_GRP_7, 7*ELX_GRP_8, 6*ELX_GRP_9, 3*ELX_GRP_10, 5*ELX_GRP_14, 11*ELX_GRP_15, 9*ELX_GRP_18,
      12*ELX_GRP_19, 4*ELX_GRP_20, 0*ELX_GRP_21, -4*ELX_GRP_23, 6*ELX_GRP_24, 5*ELX_GRP_25, -2*ELX_GRP_26, -2*ELX_GRP_27, -7*ELX_GRP_29, -3*ELX_GRP_31, 0*ELX_GRP_16,
		0*ELX_GRP_11, 0*ELX_GRP_12, 0*ELX_GRP_17, 5*ELX_GRP_2, 0*ELX_GRP_13, 3*ELX_GRP_22, 0*ELX_GRP_28, 0*ELX_GRP_30);
RUN;

/*check: find mechvent =1 on hospitalization level*/
data mechvent_hosp; 
set vapd_daily&year._v16b;
if proccode_mechvent_daily=1;
run;

/*get sofa*/
DATA vapd_daily&year._v17 (compress=yes); 
SET vapd_daily&year._v16b;
if hi_bili_daily='.' then Liver_SOFA=.; /*unit in mg/dL*/
    else if hi_bili_daily <1.2 then Liver_SOFA=0;
    else if 1.2<=hi_bili_daily <2.0 then Liver_SOFA=1;
    else if 2.0<=hi_bili_daily <6.0 then Liver_SOFA=2;
    else if 6.0<=hi_bili_daily <12.0 then Liver_SOFA=3;
    else if hi_bili_daily>12.0 then Liver_SOFA=4;
if  hi_creat_daily = '.' then Renal_SOFA=.; /*unit in mg/dL*/
 	else if  hi_creat_daily<1.2 then Renal_SOFA=0;
 	else if 1.2=< hi_creat_daily<2.0 then Renal_SOFA=1;
 	else if 2.0=< hi_creat_daily<3.5 then Renal_SOFA=2;
 	else if 3.5=< hi_creat_daily<5.0 then Renal_SOFA=3;
 	else if  hi_creat_daily>5.0 then Renal_SOFA=4;
if lo_plate_daily='.' then Coagulation_SOFA=.; /*unit in 1000s*/
 	else if lo_plate_daily >150 then Coagulation_SOFA=0;
 	else if 101=<lo_plate_daily<151 then Coagulation_SOFA=1;
 	else if 51=<lo_plate_daily<101 then Coagulation_SOFA=2;
 	else if 21=<lo_plate_daily<51 then Coagulation_SOFA=3;
 	else if lo_plate_daily<=20 then Coagulation_SOFA=4;
if pressor_1 NE '' or pressor_2 NE '' or pressor_3 NE '' or pressor_4 NE '' or pressor_5 NE '' then any_pressor=1;
else any_pressor=0;
if any_pressor=1 then Cardio_SOFA=3.5; else Cardio_SOFA=0;
RUN;

PROC FREQ DATA=vapd_daily&year._v17 order=freq;
TABLE  Liver_SOFA Renal_SOFA Coagulation_SOFA Cardio_SOFA;
RUN;

/********************************************/
/*** create unique single-site hosp ID ***/
/*assign uniquesingle-site hospitalization count ID*/
PROC SORT DATA=vapd_daily&year._v17  nodupkey 
out=hosp (keep=patienticn sta6a new_admitdate2 new_dischargedate2); /* 2232068*/
BY  patienticn sta6a new_admitdate2 new_dischargedate2;
RUN;

DATA hosp; 
SET hosp;
unique_hosp_count_id= _N_;
RUN;

PROC SQL;
	CREATE TABLE  unique_hosp_count_id (compress=yes)  AS 
	SELECT A.*, B.unique_hosp_count_id
	FROM  vapd_daily&year._v17 A
	LEFT JOIN hosp  B
	ON A.patienticn =B.patienticn and a.sta6a=b.sta6a and a.new_admitdate2=b.new_admitdate2 and a.new_dischargedate2=b.new_dischargedate2;
QUIT;

DATA icu.vapd_daily&year._v24_20190102 (compress=yes);
SET  unique_hosp_count_id;
label  
unique_hosp_count_id='assigned an unique hospitalization ID to each hospitalization (by patient, admission and discharge date)';
RUN;

/*add CCS categories*/
/*11/26/18: add CCS multi-level and single-level categories*/
/*Use primary diagnosis to create Single-level CCS categories*/
/****************** Turn diagnosis code into single-level CCS ********************/
/*7/8/19: decided to keep the decimals in diag codes, so need to create one without decimals for this*/
data diagnosis (compress=yes);
set icu.vapd_daily&year._v24_20190102;
icd10code1=compress(DiagnosisCode_1,'.');
keep patienticn sta6a datevalue new_admitdate2 new_dischargedate2 icdtype specialtytransferdate specialtydischargedate DiagnosisCode_1 icd10code1;
run;

data icd9 (compress=yes) icd10 (compress=yes);
set diagnosis;
if icdtype ='ICD10' then output ICD10; 
if icdtype ='ICD9' then output ICD9; 
run;

data missing_diag (compress=yes); 
set diagnosis;
if icdtype = '';
run;

PROC SORT DATA= missing_diag  nodupkey  OUT= missing_diag_hosps (compress=yes); 
BY patienticn sta6a  new_admitdate2 new_dischargedate2;
RUN;

PROC SORT DATA=icd9  nodupkey  OUT=icd9_undup (compress=yes); 
BY  patienticn specialtytransferdate specialtydischargedate DiagnosisCode_1;
RUN;

DATA icd9_undup (compress=yes); 
SET  icd9_undup;
if DiagnosisCode_1 ='' then delete;
RUN;

PROC SORT DATA=icd10  nodupkey  OUT=icd10_undup (compress=yes); 
BY  patienticn specialtytransferdate specialtydischargedate DiagnosisCode_1;
RUN;

DATA icd10_undup (compress=yes); 
SET  icd10_undup;
if DiagnosisCode_1 ='' then delete;
RUN;

PROC FREQ DATA=icd10_undup order=freq;
TABLE  DiagnosisCode_1;
RUN;

/**** ICD-10 Single Level****/
/******************************************************************/
* Path & name for the ICD-10-CM/PCS CCS tool ;
/*download file: FOLDER PATH\ccs_dx_icd10cm_2018_1.csv */       
FILENAME INRAW1  'FOLDER PATH\ccs_dx_icd10cm_2018_1.csv' 			LRECL=300; 
/*LIBNAME  IN1     'Location of input discharge data';*/
/*LIBNAME  OUT1    'Directory to store output file';         */

/*TITLE1 'CREATE ICD-10-CM/PCS CCS SINGLE-LEVEL TOOL CATEGORIES';*/
/*TITLE2 'USE WITH DISCHARGE ADMINISTRATIVE DATA THAT HAS ICD-10-CM/PCS CODES';*/
                                            
/******************************************************************/
/*  Macro Variables that must be set to define the characteristics*/
/*  of your SAS discharge data. Change these values to match the  */
/*  number of diagnoses and procedures in your dataset. Change    */
/*  CORE to match the name of your dataset.                       */
/******************************************************************/

* Maximum number of DXs on any record;        %LET NUMDX=1;
/** Maximum number of PRs on any record;        %LET NUMPR=15;*/
* Input SAS file member name;                 %LET CORE=work.icd10_undup;


/******************* SECTION 1: CREATE INFORMATS ******************/
/*  SAS Load the ICD-10-CM/PCS CCS single-level tool & convert    */
/*  into temporary SAS informats that will be used to assign the  */
/*  single-level CCS variable in the next step.                   */
/******************************************************************/

/* Diagnoses CCS */
DATA DXCCS;
    INFILE INRAW1 DSD DLM=',' END = EOF FIRSTOBS=2;
    INPUT
       START            : $CHAR7.
       LABEL            : 4.
       ICD10CM_label    : $CHAR100.
       CCS_Label        : $CHAR100.
       Multi_lvl1       : $CHAR2.
       Multi_lvl1_label : $CHAR100.
       Multi_lvl2       : $CHAR5.
       Multi_lvl2_label : $CHAR100.
    ;
   RETAIN HLO " ";
   FMTNAME = "I10DXCCS" ;
   TYPE    = "I" ;
   OUTPUT;

   IF EOF THEN DO ;
      START = " " ;
      LABEL = 0 ;
      HLO   = "O";
      OUTPUT ;
   END ;
RUN;

PROC FORMAT LIB=WORK CNTLIN = DXCCS;
RUN;

/*left join ccs groups to icd10 dataset*/
PROC SQL;
	CREATE TABLE  icd10_ccs (compress=yes)  AS  /* 2070718*/
	SELECT A.*, B.LABEL as singlelevel_ccs
	FROM   icd10_undup  A
	LEFT JOIN  DXCCS B  ON A.icd10code1 =B.START;
QUIT;

/**** ICD-9 Single Level****/
/******************************************************************/
* Path & name for the ICD-10-CM/PCS CCS tool ;
/*download file: FOLDER PATH\$dxref 2015.csv */ 
FILENAME INRAW1  'FOLDER PATH\$dxref 2015.csv' LRECL=300; 
/*LIBNAME  IN1     'Location of input discharge data';*/
/*LIBNAME  OUT1    'Directory to store output file';         */

/*TITLE1 'CREATE ICD-10-CM/PCS CCS SINGLE-LEVEL TOOL CATEGORIES';*/
/*TITLE2 'USE WITH DISCHARGE ADMINISTRATIVE DATA THAT HAS ICD-10-CM/PCS CODES';*/
                                            
/******************************************************************/
/*  Macro Variables that must be set to define the characteristics*/
/*  of your SAS discharge data. Change these values to match the  */
/*  number of diagnoses and procedures in your dataset. Change    */
/*  CORE to match the name of your dataset.                       */
/******************************************************************/

* Maximum number of DXs on any record;        %LET NUMDX=1;
/** Maximum number of PRs on any record;        %LET NUMPR=15;*/
* Input SAS file member name;                 %LET CORE=work.icd9_undup;




/******************* SECTION 1: CREATE INFORMATS ******************/
/*  SAS Load the ICD-10-CM/PCS CCS single-level tool & convert    */
/*  into temporary SAS informats that will be used to assign the  */
/*  single-level CCS variable in the next step.                   */
/******************************************************************/

/* Diagnoses CCS */
data ccs2010;
   infile inraw1  dsd dlm=',' end = eof firstobs=3 missover;
	input start       :  $char5.
	      label       :  4.
			codelabel   :  $char70.
			ccslabel    :  $char70.
			optionalcat :  $char5.
			optionallab :  $char40.
         ;
	retain hlo " ";
   fmtname = "$ccs" ;
   type    = " " ;
   output;

   if eof then do ;
      start = " " ;
		label = " " ;
      hlo   = "o";
      output ;
   end ;
run;

proc format cntlin = ccs2010;
run;

data ccs2010;
set ccs2010;
if start = '' then delete;
run;

/*left join ccs groups to icd9 dataset*/
PROC SQL;
	CREATE TABLE  icd9_ccs (compress=yes)  AS   
	SELECT A.*, B.LABEL as singlelevel_ccs
	FROM   icd9_undup  A
	LEFT JOIN ccs2010 B  ON A.icd10code1 =B.START;
QUIT;

/*combine icd9 and icd10 single level ccs datasets*/
data combine (compress=yes); 
set icd9_ccs (keep= patienticn specialtytransferdate specialtydischargedate icdtype DiagnosisCode_1 singlelevel_ccs)
icd10_ccs (keep= patienticn specialtytransferdate specialtydischargedate icdtype DiagnosisCode_1 singlelevel_ccs);
run;

/*left join singlelevel_ccs back to VAPD*/
PROC SQL;
	CREATE TABLE vapd_daily&year._07102019_v3  (compress=yes)  AS 
	SELECT A.*, B.singlelevel_ccs
	FROM icu.vapd_daily&year._v24_20190102  A
	LEFT JOIN  combine B ON A.patienticn =B.patienticn and  a.DiagnosisCode_1=b.DiagnosisCode_1 
	and a.icdtype=b.icdtype and a.specialtytransferdate=b.specialtytransferdate and a.specialtydischargedate=b.specialtydischargedate;
QUIT;

DATA  check_missing (compress=yes); /*n= 64523 , matches those with missing diag*/
SET vapd_daily&year._07102019_v3;
if singlelevel_ccs=.;
RUN;

/*get multi-level level 1 CCS*/
/**** ICD-9 Multi-Level, level 1****/
/*import ccs_multi_dx_tool_2015.csv*/

/*download and import CCS_MULTI_DX_TOOL_2015.CSV file*/
PROC IMPORT OUT = my_icd9_csv
          FILE = "W:\PERSONAL SHARE\Shirley Wang\Expanded VAPD\CCS\Multi-level\CCS_MULTI_DX_TOOL_2015.CSV"
          DBMS = CSV
          REPLACE;
RUN;

PROC SQL;
	CREATE TABLE  ccs_multi_icd9 (compress=yes)  AS 
	SELECT A.*, B.CCS_LVL_1 as multilevel1_ccs
	FROM   icd9_undup  A
	LEFT JOIN  my_icd9_csv B ON A.icd10code1=B.ICD_9_CM_CODE ;
QUIT;

/**** ICD-10 Multi-Level, level 1****/
/*download and import ccs_dx_icd10cm_2018_1.CSV file*/
/*import csv file*/
PROC IMPORT OUT = my_icd10_csv
          FILE = "W:\PERSONAL SHARE\Shirley Wang\Expanded VAPD\CCS\Multi-level\ccs_dx_icd10cm_2018_1.CSV"
          DBMS = CSV
          REPLACE;
RUN;

PROC SQL;
	CREATE TABLE  ccs_multi_icd10 (compress=yes)  AS 
	SELECT A.*, B.MULTI_CCS_LVL_1 as multilevel1_ccs
	FROM   icd10_undup  A
	LEFT JOIN  my_icd10_csv B ON A.icd10code1=B.ICD_10_CM_CODE;
QUIT;

data combine_multi (compress=yes); 
set ccs_multi_icd9 (keep= patienticn specialtytransferdate specialtydischargedate icdtype DiagnosisCode_1 multilevel1_ccs )
ccs_multi_icd10 (keep= patienticn specialtytransferdate specialtydischargedate icdtype DiagnosisCode_1 multilevel1_ccs );
run;

PROC SQL;
	CREATE TABLE vapd_daily&year._07102019_v4 (compress=yes)  AS 
	SELECT A.*,c.multilevel1_ccs
	FROM  vapd_daily&year._07102019_v3 A  
    LEFT JOIN  combine_multi c ON A.patienticn=c.patienticn and a.DiagnosisCode_1=c.DiagnosisCode_1 
	and a.icdtype=c.icdtype and a.specialtytransferdate=c.specialtytransferdate and a.specialtydischargedate=c.specialtydischargedate;
QUIT;


/** ADD ANGUS SEPSIS and CDC EHR sepsis INDICATORS **/
PROC SQL;
	CREATE TABLE icu.vapd_ccs_sepsis_20190710  (compress=yes)  AS  
	SELECT A.*, B.infection as angus_def_infection, b.organ_dysfunction as angus_organ_dysfunction, 
		b.explicit_dx as angus_explicit_dx, b.implicit_dx as angus_implicit_dx, b.angus as angus_def_sepsis,
		c.hosp_sepsis as cdc_hosp_sepsis, c.comm_sepsis as cdc_comm_sepsis, c.Infection as cdc_Infection,  c.hosp_infection as cdc_hosp_infection, 
    c.comm_infection as cdc_comm_infection, c.hospcomm_sepsis as cdc_hospcomm_sepsis
	FROM  vapd_daily&year._07102019_v4  A
	LEFT JOIN temp.vapd_angus_07102019  B
    ON A.patienticn =B.patienticn and a.sta6a=b.sta6a and a.specialtytransferdate=b.specialtytransferdate
 and a.specialtydischargedate=b.specialtydischargedate
   Left JOIN  raw.basic_sepsis_V3  C
    ON a.inpatientsid=c.Admission_ID and A.patienticn=c.patient_id and a.datevalue=c.datevalue and a.new_admitdate2=c.new_admitdate2;
QUIT;

/**** Add hospital characteristics ****/
/*Sarah updated the Council for Teaching Hospital criteria for each sta6a facility, 2017-2018, use that for teaching indicator.
This file is saved in */
/*import csv file*/
/*hospital Excel files (teaching indicator and region) are saved in VAPD Github folder, must be imported into SAS datasets first*/

/*import csv file*/
PROC IMPORT OUT = new_teaching /*named: hospital_teaching_indicators_20190109*/
          FILE = ""
          DBMS = 
          REPLACE;
RUN;

/*add labels to variable names*/
DATA new_teaching;
SET  new_teaching;
label
new_teaching ='new teaching indicator (COTH Member 2007 + 2018)'
old_teaching='COTH 2007 criteria for teaching indicator'
current_teaching='as of 1/9/19, current teaching indicator based on COTH';
run;

/*sta6a=facility on site_level dataset & hosptal dataset,  sta3n=facility on level_beds, */
DATA hospital_data ; /*1213*/
SET icu.hospital;
RUN;

DATA check_site ; /*missing regions*/
SET hospital_data ;
if Hospital_ID in ('679','675','629','529','608');
RUN;

/*manually code in region for those above facilities*/
DATA hospital_data2; /*119*/
SET hospital_data;
if Hospital_ID='679' or Hospital_ID='675' or Hospital_ID='629' then region='SOUTH'; 
if Hospital_ID='529' or Hospital_ID='608' then region ='NORTHEAST';
length sta6a_v2 $3;
sta6a_v2=substr(Hospital_ID, 1,3);
if region = '' then delete;
RUN;

PROC SORT DATA=hospital_data2  nodupkey ;
BY  sta6a_v2 region;
RUN;

/*replace missing sta6as with missing region to match sta3n*/
DATA vapd_daily&year._hosp (compress=yes); 
SET icu.vapd_ccs_sepsis_20190710;
sta3n_char=put(sta3n, 3.);
/*if sta6a in ('*Missing*','','*Unknown at this time*') then sta6a=sta3n_char; */
length sta6a_v2 $3;
sta6a_v2=substr(sta6a, 1,3);
drop sta3n_char;
RUN;

/*left join teaching and region to VAPD*/
proc sql;
	CREATE TABLE vapd_ccs_sepsis_hosp_20190710 (compress=yes) AS 
	SELECT a.*, b.new_teaching, c.region
	FROM  vapd_daily&year._hosp  a
	LEFT JOIN new_teaching b on a.sta6a=b.sta6a
    left join hospital_data2 c on a.sta6a_v2=c.sta6a_v2;
quit;

/*region missing?*/
data missing_region; /*0*/
set vapd_ccs_sepsis_hosp_20190710;
if region='';
run;

PROC SORT DATA=vapd_ccs_sepsis_hosp_20190710  nodupkey 
  OUT= vapd_daily&year._sta6a (keep=admityear sta6a sta3n region new_teaching unique_hosp_count_id); /*n=134*/
BY  sta6a ;
RUN;

PROC FREQ DATA=vapd_ccs_sepsis_hosp_20190710  order=freq;
TABLE  sta6a;
RUN;

/*get ethnicity data*/
/*get ethnicity info for VAPD from CDW*/
/*undup ethnicity dataset first*/
PROC SORT DATA=icu.ethnicity_pull_10252018  nodupkey  OUT=ethnicity (compress=yes);
BY  patienticn ;
RUN;

/*change patienticn into numeric*/   
DATA ethnicity (rename=patienticn2=patienticn compress=yes);
SET ethnicity;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

PROC SQL;
	CREATE TABLE vapd_ccs_sepsis_hosp_20190710_v2  (compress=yes)  AS 
	SELECT A.*, B.ethnicity, c.hi_ALT_daily, c.lo_ALT_daily, c.clean_unit2 as ALT_units
	FROM  vapd_ccs_sepsis_hosp_20190710  A
	LEFT JOIN  ethnicity  B ON A.patienticn=B.patienticn
	left join lab.Alt_hi_lo_20142017_20190417 C on a.patienticn=c.patienticn and a.datevalue=c.LabSpecimenDate;
QUIT;

/*4/22/19: revise VAPD based on discussion in the 4/18/19 VAPD meeting.
-drop  gender missing (N=9),,create hispanic variable from ethnicity, 
combine the hypertension comorbid to make N=30 Elixhauser comorbids, drop problematic hospitalzations before recode readmit30 & hospital_day*/
DATA vapd_ccs_sepsis_hosp_20190710_v2 (compress=yes); 
SET vapd_ccs_sepsis_hosp_20190710_v2;
if ethnicity ='' then ethnicity='UNKNOWN BY PATIENT';
if race ='' then race = 'UNKNOWN';
if  gender = '' then delete; /*drop if gender = missing*/
if ethnicity  ='HISPANIC OR LATINO' then hispanic=1; else hispanic=0; /*create hispanic variable from ethnicity*/
if  ELX_GRP_6=1 or ELX_GRP_7=1 then htn=1; else htn=0;/*combine the hypertension comorbid to make N=30 Elixhauser comorbids*/
hosp_los=(new_dischargedate2-new_admitdate2)+1;/*get hosp_los*/
drop ELX_GRP_7  ELX_GRP_6;
RUN;

PROC FREQ DATA=vapd_ccs_sepsis_hosp_20190710_v2  order=freq;
TABLE ethnicity race;
RUN;

/*some hosps are the same (same datevalues), just different facilities, drop the duplicate ones*/
PROC SORT DATA=vapd_ccs_sepsis_hosp_20190710_v2 nodupkey out=revision_vapd1b (compress=yes) dupout=dups  (compress=yes) /*0, no need for additional steps*/;
BY  unique_hosp_count_id datevalue;
RUN;

PROC SORT DATA=vapd_ccs_sepsis_hosp_20190710_v2  nodupkey   
OUT=revision_vapd_hosps (compress=yes keep=patienticn  unique_hosp_count_id sta6a sta3n new_admitdate2 new_dischargedate2  admityear);
BY patienticn new_admitdate2 new_dischargedate2;
RUN;

/************************ recode readmission 30-day **************************************/
/*before recoding readmission 30-day and hospital_day, find hosps that are within the dates of previous admission or are admitted before
previous admit's discharge date*/
data revision_vapd_hosps (compress=yes);  
retain patienticn new_admitdate2 new_dischargedate2 unique_hosp_count_id sta6a ;
set revision_vapd_hosps;
by PatientICN new_admitdate2 new_dischargedate2;
lag_admit=lag(new_admitdate2); /*retrieve the value of the previous admit date*/
lag_discharge=lag(new_dischargedate2); /*retrieve the value of the previous discharge date*/
format lag_admit mmddyy10.  lag_discharge mmddyy10.;
Ndays_since_lagdis=new_admitdate2-lag_discharge; /*count # days difference between current admission date and last discharge date*/
if first.patienticn then do; /*reset the values of the following fields in case of a new patienticn*/
lag_admit=.;
lag_discharge=.;
Ndays_since_lagdis=.;
end;
run;

/*check hosps*/
DATA check_revision_hosps (compress=yes); /*209 hosps*/
SET  revision_vapd_hosps ;
if Ndays_since_lagdis <0 and Ndays_since_lagdis NE .;
RUN;

PROC SQL;
CREATE TABLE   check_revision_hosps2  (COMPRESS=YES) AS /*some hosps still embedded within another hosps, drop those 209  */
SELECT A.* FROM revision_vapd_hosps AS A
WHERE A.patienticn IN (SELECT patienticn   FROM  check_revision_hosps );
QUIT;

/***************************************************************/
/*drop those 209 problematic hosps from daily-level dataset and rerun the readmit30 codes*/
PROC SQL;
CREATE TABLE vapd_ccs_sepsis_hosp_20190710_v3   (COMPRESS=YES) AS 
SELECT A.* FROM vapd_ccs_sepsis_hosp_20190710_v2 AS A
WHERE A.unique_hosp_count_id NOT IN (SELECT  unique_hosp_count_id  FROM  check_revision_hosps );
QUIT;

/*sort */
PROC SORT DATA=vapd_ccs_sepsis_hosp_20190710_v3  nodupkey
out=revision_vapd_hosps_readm (compress=yes keep=patienticn  unique_hosp_count_id sta6a sta3n new_admitdate2 new_dischargedate2); /*  2231732 hosps*/
BY  patienticn  new_admitdate2 new_dischargedate2;
RUN;

data part1 (compress=yes); 
retain patienticn new_admitdate2 new_dischargedate2 unique_hosp_count_id sta6a ;
set revision_vapd_hosps_readm;
by PatientICN new_admitdate2 new_dischargedate2;
lag_admit=lag(new_admitdate2); /*retrieve the value of the previous admit date*/
lag_discharge=lag(new_dischargedate2); /*retrieve the value of the previous discharge date*/
format lag_admit mmddyy10.  lag_discharge mmddyy10.;
Ndays_since_lagdis=new_admitdate2-lag_discharge; /*count # days difference between current admission date and last discharge date*/
if first.patienticn then do; /*reset the values of the following fields in case of a new patienticn*/
lag_admit=.;
lag_discharge=.;
Ndays_since_lagdis=.;
Isa_readm30=.; end;
if 0 <= Ndays_since_lagdis <=30 then Isa_readm30=1; else Isa_readm30=0; /*this hospitalization is a readmit?*/
run;

/*get time2readm, first resort the data to get lead_adm (future adm_date)*/
/*sort the data by patient and descending admit date*/
PROC SORT DATA=part1  nodupkey  OUT=part1C (compress=yes);
BY  patienticn descending new_admitdate2 descending  new_dischargedate2;
RUN;

data part1D (compress=yes); 
set part1C;
by PatientICN descending new_admitdate2 descending new_dischargedate2;
lead_admit=lag(new_admitdate2); /*retrieve the value of the future admit date*/
format lead_admit mmddyy10.;
time2readm=abs(new_dischargedate2-lead_admit);
if first.patienticn then do; /*reset the values of the following fields in case of a new patienticn*/
lead_admit=.;
time2readm=.;
end;
if 0 <= time2readm <=30 then follby_readm30=1; else  follby_readm30=0; /*followed by another hospitalization within 30 days?*/
run;

/*sort the dataset back to ascending order*/
PROC SORT DATA=part1D  nodupkey  OUT= part2 (compress=yes); 
BY  PatientICN sta6a new_admitdate2  new_dischargedate2;
RUN;

PROC FREQ DATA=part2   order=freq;
TABLE Isa_readm30  follby_readm30; /*both N=354,337, matches*/
RUN;

/* add and rename operatingbeds as beds_in_use, add Isa_readm30  follby_readm30 back to VAPD*/
/*Get hospital/facility #operating bed info, use FY16 since data is from 2014-2017*/
proc freq data=hospital.hospital;
table Hospital_ID;
run;

/*sta6a=facility on site_level dataset & hosptal dataset,  sta3n=facility on level_beds, */
/*join Ndays_since_lagdis,  Isa_readm30,  time2readm, follby_readm30 back to VAPD*/
proc sql;
	CREATE TABLE vapd_ccs_sepsis_hosp_20190710_v4 (compress=yes) AS 
	SELECT a.*, c.operatingbeds as beds_in_use, B.Ndays_since_lagdis, b.Isa_readm30 , b.time2readm, b.follby_readm30
	FROM  vapd_ccs_sepsis_hosp_20190710_v3 a
	LEFT JOIN hospital.hospital C ON a.sta6a=c.Hospital_ID
	LEFT JOIN  part2 B ON A.PatientICN=B.PatientICN and a.sta6a=b.sta6a and a.new_admitdate2=b.new_admitdate2 and a.new_dischargedate2=b.new_dischargedate2;
quit;

DATA  check_days1  (compress=yes); /* 2231858 vs. 2231732, hosp-day=1 don't match, debug below*/
SET vapd_ccs_sepsis_hosp_20190710_v4 ;
if hospital_day=1;
RUN;

DATA  part2check  (compress=yes); 
SET  part2;
n=1;
RUN;

PROC SQL;
	CREATE TABLE check_days2  (compress=yes)  AS 
	SELECT A.*, B.n
	FROM  check_days1    A
	LEFT JOIN  part2check  B
	ON A.patienticn =B.patienticn and a.sta6a=b.sta6a and a.new_admitdate2=b.new_admitdate2 and a.new_dischargedate2=b.new_dischargedate2 ;
QUIT;

DATA check_days3   (compress=yes); 
SET  check_days2 ;
where n=.;
RUN;

PROC SQL;
CREATE TABLE  check_days2b   (COMPRESS=YES) AS 
SELECT A.* FROM check_days2 AS A
WHERE A.patienticn IN (SELECT  patienticn  FROM  check_days3  );
QUIT;

DATA  check_days2b  (compress=yes);
SET  check_days2b ;
keep patienticn sta6a datevalue new_admitdate2 new_dischargedate2 hospital_day unique_hosp_count_id Isa_readm30 follby_readm30;
RUN;

PROC SORT DATA=  check_days2b ;
BY  patienticn sta6a datevalue new_admitdate2 new_dischargedate2 hospital_day;
RUN;

DATA  check_days2b  (compress=yes);  /*these hosp have same admit and discharge dates but different sta3n and sta6a. Drop these 126 days=126 hosps*/
SET  check_days2b ;
if datevalue NE  new_admitdate2 then flag=1;
RUN;

PROC SQL;
CREATE TABLE   vapd_ccs_sepsis_hosp_20190710_v5  (COMPRESS=YES) AS 
SELECT A.* FROM vapd_ccs_sepsis_hosp_20190710_v4 AS A
WHERE A.unique_hosp_count_id NOT IN (SELECT  unique_hosp_count_id  FROM check_days3);
QUIT;

proc sql;
SELECT count(distinct unique_hosp_count_id) 
FROM vapd_ccs_sepsis_hosp_20190710_v5;
quit;

/*check again hospital_day=1 = 2231732 hosps*/
DATA  check_days1b  (compress=yes); /* 2231732 hosps, match*/
SET vapd_ccs_sepsis_hosp_20190710_v5;
if hospital_day=1;
RUN;


/*rename some variables*/
DATA vapd_ccs_sepsis_hosp_20190710_v6 (compress=yes); /*  12310970 pat-fac-days*/
SET  vapd_ccs_sepsis_hosp_20190710_v5;
rename 	any_pressor=any_pressor_daily
		inhospmort=inhosp_mort
		mort30=mort30_admit
		DiagnosisCode_1=unit_dx1
		DiagnosisCode_2=unit_dx2
		DiagnosisCode_3=unit_dx3
		DiagnosisCode_4=unit_dx4
		DiagnosisCode_5=unit_dx5
		DiagnosisCode_6=unit_dx6
		DiagnosisCode_7=unit_dx7
		DiagnosisCode_8=unit_dx8
		DiagnosisCode_9=unit_dx9
		DiagnosisCode_10=unit_dx10
		DiagnosisCode_11=unit_dx11
		DiagnosisCode_12=unit_dx12
		DiagnosisCode_13=unit_dx13
		DiagnosisCode_14=unit_dx14
		DiagnosisCode_15=unit_dx15
		DiagnosisCode_16=unit_dx16
		DiagnosisCode_17=unit_dx17
		DiagnosisCode_18=unit_dx18
		DiagnosisCode_19=unit_dx19
		DiagnosisCode_20=unit_dx20
		DiagnosisCode_21=unit_dx21
		DiagnosisCode_22=unit_dx22
		DiagnosisCode_23=unit_dx23
		DiagnosisCode_24=unit_dx24
		DiagnosisCode_25=unit_dx25
		DiagnosisCode_26=unit_dx26
		ELX_GRP_1=chf
		ELX_GRP_2=cardic_arrhym
		ELX_GRP_3=valvular_d2
		ELX_GRP_4=pulm_circ
		ELX_GRP_5=pvd
/*		ELX_GRP_6=htn_uncomp*/
/*		ELX_GRP_7=htn_comp*/
		ELX_GRP_8=paralysis
		ELX_GRP_9=neuro
		ELX_GRP_10=pulm
		ELX_GRP_11=dm_uncomp
		ELX_GRP_12=dm_comp
		ELX_GRP_13=hypothyroid
		ELX_GRP_14=renal
		ELX_GRP_15=liver
		ELX_GRP_16=pud
		ELX_GRP_17=ah
		ELX_GRP_18=lymphoma
		ELX_GRP_19=cancer_met
		ELX_GRP_20=cancer_nonmet
		ELX_GRP_21=ra
		ELX_GRP_22=coag
		ELX_GRP_23=obesity
		ELX_GRP_24=wtloss
		ELX_GRP_25=fen
		ELX_GRP_26=anemia_cbl
		ELX_GRP_27=anemia_def
		ELX_GRP_28=etoh
		ELX_GRP_29=drug
		ELX_GRP_30=psychoses
		ELX_GRP_31=depression
		pressor_1=pressor_med1
		pressor_2=pressor_med2
		pressor_3=pressor_med3
		pressor_4=pressor_med4
		pressor_5=pressor_med5
		drugname_1=drugname_med1
		drugname_2=drugname_med2
		drugname_3=drugname_med3
		drugname_4=drugname_med4
		drugname_5=drugname_med5
		drugname_6=drugname_med6
		drugname_7=drugname_med7
		drugname_8=drugname_med8
		drugname_9=drugname_med9
		abx1=abx_penicillin
		abx2=abx_pseudomonal_pcn
		abx3=abx_1st_gen_cephalosporin
		abx4=abx_2nd_gen_cephalosporin
		abx5=abx_3rd_gen_cephalosporin
		abx6=abx_4th_gen_cephalosporin
		abx7=abx_fluoroquinolone
		abx8=abx_Vancomycin_IV
		abx9=abx_Vancomycin_PO
		abx10=abx_antiviral
		abx11=abx_macrolide
		abx12=abx_flagyl
		abx13=abx_sulfa
		abx14=abx_antifungal
		abx15=abx_Aztreonam_IV
		abx16=abx_clinda
		abx17=abx_big_abx
		abx18=abx_aminoglycoside
		abx19=abx_tetracycline
		abx20=abx_other;
RUN;

/*macro to change all variables from uppercase to lowercases because unlike SAS, STATA is case sensitive*/ 
%macro lowcase(dsn); 
     %let dsid=%sysfunc(open(&dsn)); 
     %let num=%sysfunc(attrn(&dsid,nvars)); 
     %put &num;
     data &dsn; 
           set &dsn(rename=( 
        %do i = 1 %to &num; 
        %let var&i=%sysfunc(varname(&dsid,&i));      /*function of varname returns the name of a SAS data set variable*/
        &&var&i=%sysfunc(lowcase(&&var&i))         /*rename all variables*/ 
        %end;)); 
        %let close=%sysfunc(close(&dsid)); 
  run; 
%mend lowcase; 

%lowcase(vapd_ccs_sepsis_hosp_20190710_v6 ) /*change name of dataset here*/

PROC CONTENTS DATA=vapd_ccs_sepsis_hosp_20190710_v6  VARNUM;
RUN;

/*label variables*/
DATA vapd_ccs_sepsis_hosp_20190710_v7 (compress=yes);
SET  vapd_ccs_sepsis_hosp_20190710_v6;
if gender ='F' then female=1; else if gender ='M' then female =0; else if gender ='' then female=.;
drop sta6a_v2 readmit30;
label  
female ='1=female, 0=male'
patienticn=	'Main unique patient ID'
scrssn=	'scrambled SSN'
inpatientsid='inpatient hosp id'
patientsid=	'secondary patient id'
race='Race of patient'
gender='gender of patient'
ethnicity=	'patient ethnicity'
hispanic ='Hispanic indicator for patient'
specialty_priority_order='prioritize specialty order,icu=1, other acute=3, Observations=3'
admitdate_icu='ICU admit date'
admitdatetime_icu='ICU discharge date'
dob	=	'Date of Birth'
age	=	'Age'
sta3n	=	'sta3n, parent facility'
specialty	=	'specialty'
sta6a	=	'sta6a, child facility, this is the main facility variable used in VAPD'
icu	=	'ICU indicator'
admityear	=	'year of admission'
/*cdw_admitdatetime	=	'CDW admissions date and time'*/
/*cdw_dischargedatetime	=	'CDW discharge date and time'*/
specialtytransferdate	=	'Speciality transfer admission date'
specialtydischargedate	=	'Speciality transfer discharge date'
new_admitdate2 	=	'Admission Date, this is the new admission date after Obs & acute roll up'
new_dischargedate2	=	'Discharge Date, this is the new discharge date after Obs & acute roll up'
dod_09212018_pull	=	'date of death, Sept 21, 2018 data pull'
inhosp_mort	=	'in-hospital mortality'
mort30_admit	=	'30-day mortality from admission date'
datevalue	=	'Date of inpatient'
hospital_day	=	'nth hospital day during entire hospitalization'
icdtype	=	'ICD diagnosis code type ("icd9" or "icd10")'
unit_dx1	=	'primary unit diagnosis code'
unit_dx2	=	'2nd unit diagnosis code'
unit_dx3	=	'2nd unit diagnosis code'
unit_dx4	=	'2nd unit diagnosis code'
unit_dx5	=	'2nd unit diagnosis code'
unit_dx6	=	'2nd unit diagnosis code'
unit_dx7	=	'2nd unit diagnosis code'
unit_dx8	=	'2nd unit diagnosis code'
unit_dx9	=	'2nd unit diagnosis code'
unit_dx10	=	'2nd unit diagnosis code'
unit_dx11	=	'2nd unit diagnosis code'
unit_dx12	=	'2nd unit diagnosis code'
unit_dx13	=	'2nd unit diagnosis code'
unit_dx14	=	'2nd unit diagnosis code'
unit_dx15	=	'2nd unit diagnosis code'
unit_dx16	=	'2nd unit diagnosis code'
unit_dx17	=	'2nd unit diagnosis code'
unit_dx18	=	'2nd unit diagnosis code'
unit_dx19	=	'2nd unit diagnosis code'
unit_dx20	=	'2nd unit diagnosis code'
unit_dx21	=	'2nd unit diagnosis code'
unit_dx22	=	'2nd unit diagnosis code'
unit_dx23	=	'2nd unit diagnosis code'
unit_dx24	=	'2nd unit diagnosis code'
unit_dx25	=	'2nd unit diagnosis code'
unit_dx26	=	'2nd unit diagnosis code'
sum_elixhauser_count	=	'Total Elixhauser comorbidity count'
chf	=	'Congestive Heart Failure'
cardic_arrhym	=	'Cardiac Arrhythmia'
valvular_d2	=	'Valvular Disease'
pulm_circ	=	'Pulmonary Circulation Disorders'
pvd	=	'Peripheral Vascular Disorders'
htn	=	'Hypertension Uncomplicated & Complicated'
paralysis	=	'Paralysis'
neuro	=	'Other Neurological Disorders'
pulm	=	'Chronic Pulmonary Disease'
dm_uncomp	=	'Diabetes Uncomplicated'
dm_comp	=	'Diabetes Complicated'
hypothyroid	=	'Hypothyroidism'
renal	=	'Renal Failure'
liver	=	'Liver Disease'
pud	=	'Peptic Ulcer Disease excluding bleeding'
ah	=	'AIDS/HIV'
lymphoma	=	'Lymphoma'
cancer_met	=	'Metastatic Cancer'
cancer_nonmet	=	'Solid Tumor without Metastasis'
ra	=	'Rheumatoid Arthritis/collagen'
coag	=	'Coagulopathy'
obesity	=	'Obesity'
wtloss	=	'Weight Loss'
fen	=	'Fluid and Electrolyte Disorders'
anemia_cbl	=	'Blood Loss Anemia'
anemia_def	=	'Deficiency Anemia'
etoh	=	'Alcohol Abuse'
drug	=	'Drug Abuse'
psychoses	=	'Psychoses'
depression	=	'Depression'
hosp_los	=	'Total hospitalization Length of Stay in days (admission day = day 1)'
dispositiontype	=	'DispositionType'
discharge_dispo	=	'Discharge disposition'
region	= 'facility region'
blood_cult_ind	=	'Blood culture lab indicator'
bloodcult_collection_sample	=	"Blood culture's collection sample"
bloodcult_topography	=	"blood culture sample's topography"
other_micro_ind	=	'Other micro-biology labs indicator'
othermicro_collection_sample	=	"Other micro-biology lab's collection sample"
othermicro_topography	=	"Other micro-biology lab's topography"
pressor_med1	=	'1st Vaso-pressor medications (not an indicator of 1 or 0), first pressor given during the patient-day'
pressor_med2	=	'2nd Vaso-pressor medications (not an indicator of 1 or 0), first pressor given during the patient-day'
pressor_med3	=	'3rd Vaso-pressor medications (not an indicator of 1 or 0), first pressor given during the patient-day'
pressor_med4	=	'4th Vaso-pressor medications (not an indicator of 1 or 0), first pressor given during the patient-day'
pressor_med5	=	'5th Vaso-pressor medications (not an indicator of 1 or 0), first pressor given during the patient-day'
drugname_med1	=	'1st Other drugs/medications taken (sedatives, paralytics and analgesic drugs, not an indicator of 1 or 0)'
drugname_med2	=	'2nd Other drugs/medications taken (sedatives, paralytics and analgesic drugs, not an indicator of 1 or 0)'
drugname_med3	=	'3rd Other drugs/medications taken (sedatives, paralytics and analgesic drugs, not an indicator of 1 or 0)'
drugname_med4	=	'4th Other drugs/medications taken (sedatives, paralytics and analgesic drugs, not an indicator of 1 or 0)'
drugname_med5	=	'5th Other drugs/medications taken (sedatives, paralytics and analgesic drugs, not an indicator of 1 or 0)'
drugname_med6	=	'6th Other drugs/medications taken (sedatives, paralytics and analgesic drugs, not an indicator of 1 or 0)'
drugname_med7	=	'7th Other drugs/medications taken (sedatives, paralytics and analgesic drugs, not an indicator of 1 or 0)'
drugname_med8	=	'8th Other drugs/medications taken (sedatives, paralytics and analgesic drugs, not an indicator of 1 or 0)'
drugname_med9	=	'9th Other drugs/medications taken (sedatives, paralytics and analgesic drugs, not an indicator of 1 or 0)'
abx_penicillin	=	'indicator for taken Antibiotics in class 1: penicillin'
abx_pseudomonal_pcn	=	'indicator for taken Antibiotics in class 2: pseudomonal_pcn'
abx_1st_gen_cephalosporin	=	'indicator for taken Antibiotics in class 3: 1st_gen_cephalosporin'
abx_2nd_gen_cephalosporin	=	'indicator for taken Antibiotics in class 4: 2nd_gen_cephalosporin'
abx_3rd_gen_cephalosporin	=	'indicator for taken Antibiotics in class 5: 3rd_gen_cephalosporin'
abx_4th_gen_cephalosporin	=	'indicator for taken Antibiotics in class 6: 4th_gen_cephalosporin'
abx_fluoroquinolone	=	'indicator for taken Antibiotics in class 7: fluoroquinolone'
abx_Vancomycin_IV	=	'indicator for taken Antibiotics in class 8: Vancomycin_IV'
abx_Vancomycin_PO	=	'indicator for taken Antibiotics in class 9: Vancomycin_PO'
abx_antiviral	=	'indicator for taken Antibiotics in class 10: antiviral'
abx_macrolide	=	'indicator for taken Antibiotics in class 11: macrolide'
abx_flagyl	=	'indicator for taken Antibiotics in class 12: flagyl'
abx_sulfa	=	'indicator for taken Antibiotics in class 13: sulfa'
abx_antifungal	=	'indicator for taken Antibiotics in class 14: antifungal'
abx_Aztreonam_IV	=	'indicator for taken Antibiotics in class 15: Aztreonam_IV'
abx_clinda	=	'indicator for taken Antibiotics in class 16: clinda'
abx_big_abx	=	'indicator for taken Antibiotics in class 17: big_abx'
abx_aminoglycoside	=	'indicator for taken Antibiotics in class 18: aminoglycoside'
abx_tetracycline	=	'indicator for taken Antibiotics in class 19: tetracycline'
abx_other	=	'indicator for taken Antibiotics in class 20: other'
hi_lactate_daily	=	'highest lactate lab value during this day'
lo_lactate_daily	=	'lowest lactate lab value during this day'
lactate_units	=	'lactate lab unit'
hi_plate_daily	=	'highest platelet lab value during this day'
lo_plate_daily	=	'lowest platelet lab value during this day'
plate_units	=	'platelet lab unit'
hi_creat_daily	=	'highest Creatinine lab value during this day'
lo_creat_daily	=	'lowest Creatinine lab value during this day'
creat_units	=	'Creatinine lab unit'
hi_bili_daily	=	'highest Bilirubin lab value during this day'
lo_bili_daily	=	'lowest Bilirubin lab value during this day'
bili_units	=	'Bilirubin lab unit'
hi_wbc_daily	=	'highest white blood cell lab value during this day'
lo_wbc_daily	=	'lowest white blood cel lab value during this day'
wbc_units	=	'white blood cel lab unit'
hi_glucose_daily	=	'highest glucose lab value during this day'
lo_glucose_daily	=	'lowest glucose lab value during this day'
glucose_units	=	'glucose lab unit'
hi_sodium_daily	=	'highest sodium lab value during this day'
lo_sodium_daily	=	'lowest sodium lab value during this day'
sodium_units	=	'sodium lab unit'
hi_potassium_daily	=	'highest potassium lab value during this day'
lo_potassium_daily	=	'lowest potassium lab value during this day'
potassium_units	=	'potassium lab unit'
hi_urea_daily	=	'highest urea lab value during this day'
lo_urea_daily	=	'lowest urea lab value during this day'
urea_units	=	'urea lab unit'
hi_albumin_daily	=	'highest albumin lab value during this day'
lo_albumin_daily	=	'lowest albumin lab value during this day'
albumin_units	=	'albumin lab unit'
hi_ph_daily	=	'highest pH lab value during this day'
lo_ph_daily	=	'lowest pH lab value during this day'
ph_units	=	'pH lab unit'
hi_po2_daily	=	'highest po2 lab value during this day'
lo_po2_daily	=	'lowest po2 lab value during this day'
po2_units	=	'po2 lab unit'
hi_pco2_daily='highest pco2 lab value during this day'
lo_pco2_daily='lowest pco2 lab value during this day'
pco2_units='pco2 lab unit'
hi_hemoglobin_daily	=	'highest hemoglobin lab value during this day'
lo_hemoglobin_daily	=	'lowest hemoglobin lab value during this day'
hemoglobin_units	=	'hemoglobin lab unit'
hi_hematocrit_daily	=	'highest hematocrit lab value during this day'
lo_hematocrit_daily	=	'lowest hematocrit lab value during this day'
hematocrit_units	=	'hematocrit lab unit'
hi_systolic_daily	=	'highest systolic vitals value during this day'
lo_systolic_daily	=	'lowest systolic vitals value during this day'
hi_diastolic_daily	=	'highest diastolic vitals value during this day'
lo_diastolic_daily	=	'lowest diastolic vitals value during this day'
hi_map_daily	=	'highest mean arterial pressure vitals value during this day'
lo_map_daily	=	'lowest mean arterial pressure vitals value during this day'
hi_pulse_daily	=	'highest heart rate vitals value during this day'
lo_pulse_daily	=	'lowest heart rate vitals value during this day'
hi_respiration_daily	=	'highest respiration vitals value during this day'
lo_respiration_daily	=	'lowest respiration vitals value during this day'
hi_temp_daily	=	'highest temperature vitals value during this day'
lo_temp_daily	=	'lowest temperature vitals value during this day'
hi_bicarb_daily	=	'highest bicarbonate lab value during this day'
lo_bicarb_daily	=	'lowest bicarbonate lab value during this day'
bicarb_units	=	'bicarbonate lab unit'
proccode_mechvent_daily	=	'Mechanical Ventilation daily indicator, based on procedure codes'
proccode_dialysis_daily	=	'Dialysis daily indicator, based on procedure codes'
cdc_hosp_sepsis	=	'CDC definition of hospital sepsis indicator'
cdc_comm_sepsis	=	'CDC definition of comminity sepsis indicator'
cdc_infection	=	'CDC definition of  infection indicator'
cdc_hosp_infection	=	'CDC definition of hospital infection indicator'
cdc_comm_infection	=	'CDC definition of comminity infection indicator'
cdc_hospcomm_sepsis	=	'CDC definition of sepsis indicator  '
angus_def_infection	=	'Angus definition of infection indicator'
angus_organ_dysfunction	=	'Angus definition of organ dysfunction indicator'
angus_explicit_dx	=	'Angus definition of explicit diagnosis indicator'
angus_implicit_dx	=	'Angus definition of implicit diagnosis indicator'
Angus_def_sepsis	=	'Angus definition of Sepsis indicator'
liver_sofa	=	'Liver SOFA score'
renal_sofa	=	'Renal SOFA score'
coagulation_sofa	=	'Coagulation SOFA score'
any_pressor_daily	=	'taken any vaso-pressor indicator, daily level'
cardio_sofa	=	'Cardio SOFA score (0 if no vaso-pressor, 3.5 if on vaso-pressor)'
elixhauser_vanwalraven	=	'Weighted Elixhauser score based on Van Walraven'
new_teaching = 'teaching indicator, COTH criteria'
singlelevel_ccs='single-level CCS based on primary diagnosis'
multilevel1_ccs='Multi-level 1 CCS based on primary diagnosis'
hi_procalcitonin_daily	=	'highest procalcitonin lab value during this day'  /*put labels in for the new labs*/
lo_procalcitonin_daily	=	'lowest procalcitonin lab value during this day'
procalcitonin_units	=	'procalcitonin lab unit'
hi_ALT_daily ='daily high ALT lab value'
lo_ALT_daily ='low high ALT lab value'
ALT_units ='ALT lab unit'
beds_in_use ='number of beds in use in each facility'
Ndays_since_lagdis ='number of days since last hospitalization discharge date for this patient'
Isa_readm30 ='This hospitalization is a readmission within 30 days of previous hospitalization'
time2readm ='time to future hospitalization admission date'
follby_readm30='Will this hospitalization followed by another hospitalization? Readmission within 30days indicator'
major_surg_daily ='daily major surgery indicator based on procedure codes, CCS category'
diuretics_daily='daily indicator of inpatient diuretics meds'
steroids_daily='daily indicator of inpatient steriods meds IV or PO'
hi_bnp_daily='highest BNP lab value during this day'
lo_bnp_daily='lowest BNP lab value during this day'
bnp_units='BNP lab unit'
hi_trop_daily='highest troponin lab value during this day'
lo_trop_daily='lowest troponinlab value during this day'
troponin_units='troponin lab unit'
;
RUN;

PROC CONTENTS DATA=vapd_ccs_sepsis_hosp_20190710_v7  VARNUM;
RUN;

DATA icu.vapd_daily&year._20190111 (compress=yes); 
SET vapd_ccs_sepsis_hosp_20190710_v7;
RUN;







