/* Date Modified: 08/06/2019
   Author: Brenda Vincent */

libname icu ""; /*Insert folder path*/

/*http://vaww.virec.research.va.gov/Notebook/RNB/RNB3-CDW-BedSection-CY15.pdf*/

options compress=yes;
%let studyname= ; /*insert your own study name path in datawarehouse (CDW)*/
%let data_source= " " ;/*insert your own data source*/
%let year=20142017;


/*Download the VAPD for 2014-2017 from step 1A*/
proc sql noprint;
CONNECT TO OLEDB  AS tunnel ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=studyname. 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ); 

create table icu.vapd_inpat20142017 as 
  select *
from connection to tunnel (
SELECT *
FROM &Studyname.dflt.vapd_inpat);  /*this is the table saved in the end of step 1A*/
;
disconnect from tunnel ;
quit; 

data icu.vapd_inpat&year. (compress=yes);
set icu.vapd_inpat&year.;
if specialty in ('CARDIAC INTENSIVE CARE UNIT', 'MEDICAL ICU', 'SURGICAL ICU') then icu=1;
else icu=0;
admityear=year(datepart(admitdatetime));
drop n;
run;

/*Identify hospitalizations with erroneous Date Of Death (DOD)*/
data icu.vapd_inpat&year. (compress=yes);
set icu.vapd_inpat&year.;
if not missing(dod) then do;
disdate1=datepart(intnx ('dtday', dischargedatetime, -1));
end;
format disdate1 date9.;
run;

data error_dod;
set icu.vapd_inpat&year. (keep=inpatientsid dischargedatetime disdate1 dod);
if not missing(dod_09212018_pull) then do;
if dod_09212018_pull<disdate1 then output;
end;
run;

/*Remove hospitalizations with erroneous DOD*/
proc sql;
create table vapd_inpat&year. (compress=yes) AS
select a.* 
from icu.vapd_inpat&year. a
left join error_dod b
on a.inpatientsid=b.inpatientsid
where b.inpatientsid is null;
quit;

/*Indicate whether the diagnosis codes are in ICD-9 or ICD-10*/

data icu.vapd_inpat&year. (compress=yes);
set vapd_inpat&year.;
if (icd10code ne "*Unknown at this time*" or icd10code ne '')  and icd9code="*Unknown at this time*" then icdtype="ICD10";
else if (icd9code ne "*Unknown at this time*" or icd9code ne '') and icd10code="*Unknown at this time*" then icdtype="ICD9";
run;

/*Sort by patient, admission, ordinal number.*/
proc sort data=icu.vapd_inpat&year.; 
by patienticn inpatientsid specialtytransferdatetime OrdinalNumber; 
run;

/*Output the ICD-9 and ICD-10 diagnoses into seperate datasets*/
data icd9 icd10;
set icu.vapd_inpat&year.;
if icdtype="ICD9" then output icd9;
if icdtype="ICD10" then output icd10;
run;

proc sort data=icd9 nodupkey; 
by InpatientSID specialtytransferdatetime ordinalNumber; 
run;

proc sort data=icd10 nodupkey; 
by inpatientsid specialtytransferdatetime ordinalnumber; 
run;

/*transpose diagnoses long to wide*/
proc transpose data=icd9 out=icd9_wide prefix=icd9code;
by inpatientsid specialtytransferdatetime;
var icd9code;
run;

proc sort data=icd9_wide nodupkey; 
by inpatientsid specialtytransferdatetime; 
run;

data icd9_wide ;
set icd9_wide ;
icdtype='ICD9';
run;

proc transpose data=icd10 out=icd10_wide prefix=icd10code;
by inpatientsid specialtytransferdatetime;
var icd10code;
run;

proc sort data=icd10_wide nodupkey; 
by inpatientsid specialtytransferdatetime; 
run;

data icd10_wide;
set icd10_wide;
icdtype='ICD10';
run;

data both;
set icd10_wide icd9_wide;
drop _name_  _label_;
run;                                   

data vapd_inpat&year.;
set icu.vapd_inpat&year.;
drop ordinalnumber icd10code icd9code disdate1;
run;

/*select distinct observations (one row for each dx becomes one row for each specialty stay)*/
proc sort data=vapd_inpat&year. out=vapd_inpat nodup; 
	by patienticn inpatientsid specialtytransferdatetime; 
run;

/*join the diagnosis codes*/
proc sql;
create table icu.vapd_inpat&year. as
select a.*, b.*
from vapd_inpat a
left join both b
on a.inpatientsid=b.inpatientsid and a.specialtytransferdatetime=b.specialtytransferdatetime;
quit;

/*Distinct hospitalizations by year*/
proc sql;
select admityear, count(distinct inpatientsid)
from icu.vapd_inpat&year.
where acute=1
group by admityear;
quit;

/**********************************************************************************************************************/

/*This program adjusts the admission and discharge dates to reflect time spent in an acute setting
at a particular hospital. 
Additionally, this program calculates 30-day readmission, in-hospital mortality
Finally, this program creates a row for each patient-facility-day*/

DATA  check_missing_sta6a (compress=yes); /**/
SET  icu.vapd_inpat&year.;
if sta6a in ('*Missing*','','*Unknown at this time*');
RUN;

data obs2_check (compress=yes); /*all acute=1 indicated, correct*/
set icu.vapd_inpat&year.;
if specialty in ('MEDICAL OBSERVATION','SURGICAL OBSERVATION','ED OBSERVATION','NEUROLOGY OBSERVATION');
run;

data inp&year. (compress=yes);
set icu.vapd_inpat&year.;
date_of_pull=today();
format date_of_pull date9.;
run;

/*We will create new admission and new discharge dates, which will reflect the 
acute portions of stays. So we identify the cdw admission and discharge dates
for future reference if needed*/
data inp&year.;
set inp&year.;
	cdw_admitdatetime=admitdatetime;
	cdw_dischargedatetime=dischargedatetime;
	newdischargedatetime=dischargedatetime;
	newadmitdatetime=admitdatetime;
	specialtytransferdate=datepart(specialtytransferdatetime);
	newadmitdate=datepart(newadmitdatetime);
	newdischargedate=datepart(newdischargedatetime);
format cdw_admitdatetime cdw_dischargedatetime newadmitdatetime 
newdischargedatetime specialtytransferdatetime datetime22.
specialtytransferdate newadmitdate newdischargedate date9.;
run;

/*Identify ICU admit date for ICU stays*/
data inp&year.;
set inp&year.;
if icu=1 then do;
admitdatetime_ICU=specialtytransferdatetime;
admitdate_ICU=datepart(admitdatetime_ICU);
end;
format admitdatetime_ICU DATETIME22. admitdate_ICU date9.;
run;

/*remove errors in the specialty tranfer date relative to the discharge date*/
data inp&year.;
set inp&year.;
	if newdischargedate<specialtytransferdate then delete;
run;

/*Create an indicator for the first specialty visit (and last specialty visit) during an episode of care.
	If admitted to the hospital on the same day as specialty transfer, then first specialty visit*/
proc sort data=inp&year.; 
by patienticn sta6a newadmitdatetime specialtytransferdatetime; 
run;

/*enumerate the specialty visit for each hospitalization*/
data inp&year.;
set inp&year.;
	by patienticn sta6a newadmitdatetime;
		specialtyvisit+1;
		if first.newadmitdatetime then specialtyvisit=1;
		if first.newadmitdatetime then firstvisit=1;
		if last.newadmitdatetime then lastvisit=1;	
run;

/*create an indicator for problems with admissiondate ne specialtytransferdate for the first visit*/
data inp&year.;
set inp&year.;
	issue=0;
	if firstvisit=1 then do;
		if newadmitdate~=specialtytransferdate then issue=1;
	end;
run; 

/*by how much?*/
proc sort data=inp&year.;
by patienticn newadmitdate specialtytransferdate; 
run;

data inp&year.;
set inp&year.;
by patienticn newadmitdate;
retain error 0;
	if firstvisit=1 and issue=0 then error=0;
	if firstvisit=1 and issue=1 then error=specialtytransferdate-newadmitdate;
run;

data error;
set inp&year.;
where error ne 0;
run;

data error;
set error;
where acute=1;
run;

/*For visits with an error ne 0, remove the entire hospitalization*/
data inp&year.;
set inp&year.;
	where error = 0;
run; 

proc sort data=inp&year. nodupkey; 
by patienticn specialtytransferdatetime sta6a specialty descending icu; 
run;

proc sort data=inp&year. nodupkey dupout=dups; 
by patienticn specialtytransferdatetime sta6a specialty; 
run;

/*Address issue where a bed is held in non-acute setting
--same discharge dates, but different admission dates*/
proc sort data=inp&year.; 
by patienticn newdischargedatetime newadmitdatetime; 
run;

data inp&year.;
set inp&year.;
by patienticn newdischargedatetime;
		previous_admit=ifn(first.newdischargedatetime=0, lag(newadmitdatetime),.);
	format  newadmitdatetime newdischargedatetime specialtytransferdatetime previous_admit datetime22.;
	informat newadmitdatetime newdischargedatetime specialtytransferdatetime previous_admit datetime22.;
run;

proc sort data=inp&year.; 
by patienticn newdischargedatetime descending newadmitdatetime ; 
run;

data inp&year.;
set inp&year.;
by patienticn newdischargedatetime;
		next_admit=ifn(first.newdischargedatetime=0, lag(newadmitdatetime),.);
	format  next_admit datetime22.;
	informat next_admit datetime22.;
run;

proc sort data=inp&year.; 
by patienticn newdischargedatetime newadmitdatetime; 
run;

data inp&year.;
set inp&year.;
by patienticn newdischargedatetime;
retain flag2;
if first.newdischargedatetime then flag2=.;
	if not missing(previous_admit) and newadmitdatetime ne previous_admit then flag2=1;
	if not missing(next_admit) and newadmitdatetime ne next_admit then flag2=1;
run;

data bedheld inp&year.;
set inp&year.;
	if flag2=1 then output bedheld;
	if flag2=. then output inp&year.;
run;

proc sort data=bedheld; 
by patienticn newadmitdatetime; 
run;

data bedheld;
set bedheld;
	by patienticn newdischargedatetime;
		specialtyvisit+1;
		if first.newdischargedatetime then specialtyvisit=1;
		if first.newdischargedatetime then firstvisit=1;
		if last.newdischargedatetime then lastvisit=1;	
run;

data bedheld;
set bedheld;
if firstvisit=1 and lastvisit=1 then specialtytransfer=0;
	else specialtytransfer=1;
run;

proc sort data=bedheld; 
by patienticn newdischargedatetime newadmitdatetime; 
run;

data bedheld;
set bedheld; 
	by patienticn newdischargedatetime newadmitdatetime;
	retain specialtydischargedatetime;
		if newadmitdatetime=next_admit or newadmitdatetime=previous_admit then specialtydischargedatetime=newdischargedatetime;
		else specialtydischargedatetime=.;
		format specialtydischargedatetime datetime22.;
		informat specialtydischargedatetime datetime22.;	
run;

data bedheld;
set bedheld; 
	if specialtydischargedatetime=. then do;
		specialtydischargedatetime=next_admit;
	end;
run;

data bedheld;
set bedheld; 
	if specialtydischargedatetime=. then do;
		specialtydischargedatetime=newdischargedatetime;
	end;
run;

/*Adjust the discharge date times accordingly*/
data bedheld;
set bedheld;
newdischargedatetime=specialtydischargedatetime;
run;

/*Add the adjusted bedheld dataset back*/
data inp&year.;
set inp&year. bedheld;
run;

/*Add specialtydischargedatetime*/
proc sort data=inp&year. nodupkey; 
by patienticn newadmitdatetime descending specialtytransferdatetime; 
run;

data inp&year.;
set inp&year.;
	by patienticn newadmitdatetime;
		specialtydischargedatetime=ifn(first.newadmitdatetime=0, lag(specialtytransferdatetime), newdischargedatetime); 
			/*next specialty transfer date*/
	run;

/*Is the veteran ever transfered between specialties?*/
data inp&year.;
set inp&year.;
	if firstvisit=1 and lastvisit=1 then specialtytransfer=0;
		else specialtytransfer=1;
run;

data inp&year.;
set inp&year.;
	specialtydischargedate=datepart(specialtydischargedatetime);
run;

	/*no*/
		data notransfers;
		set inp&year.;
			where specialtytransfer=0 ; 
		run;

		/*keep only acute inpatient*/
		data notransfers;
		set notransfers;
			where acute=1;
		run;

	/*yes*/
		data transfers;
		set inp&year.;
			where specialtytransfer=1; 
		run;

proc sort data=transfers nodupkey; 
by patienticn newadmitdatetime specialtytransferdatetime; 
run;

/*reenumerate*/
data transfers;
set transfers;
	drop specialtyvisit firstvisit lastvisit;
run;

proc sort data=transfers ; 
by patienticn newadmitdatetime specialtytransferdatetime newdischargedatetime;
run;

data transfers;
set transfers;
	by patienticn newadmitdatetime;
		specialtyvisit+1;
			if first.newadmitdatetime then specialtyvisit=1;
			if first.newadmitdatetime then firstvisit=1;
			if last.newadmitdatetime then lastvisit=1;	
run;

/*Does the inpatient stay contain both acute and non-acute specialties?*/
proc sort data=transfers;
	by patienticn newadmitdatetime specialtytransferdatetime; run;

/*Calculate the mean value for the acute value.
		if mean=1 then all specialties were acute. if mean=0 then all specialties were non-acute*/
proc sql;
	create table mean_inpat as
	select patienticn, newadmitdatetime, mean(acute) as mean_acute
	from transfers
	group by patienticn, newadmitdatetime;
quit;

/*join this mean to our transfer dataset*/
proc sql;
	create table transfers1 as
	select a.*, b.mean_acute
	from transfers a
	left join mean_inpat b
	on a.patienticn=b.patienticn and a.newadmitdatetime=b.newadmitdatetime;
quit;

/*Does the inpatient stay contain both acute and non-acute specialties?*/

/*Yes*/
data transfers;
set transfers1;
where mean_acute not in (0,1);
run;

/*No--Keep only the acute visits*/
data allacute;
set transfers1;
	where mean_acute=1;
run;



/**************************************Collapsing hospitalization with acute and non-acute transfers***************************/

/*Drop "non-acute" portions of the inpatient stay. 
If there is >1 day between "acute" portions, then this is considered a new stay*/

proc sort data=transfers nodupkey; 
by patienticn newadmitdatetime descending specialtytransferdatetime icu; 
run;

/*Create the next specialty admission date*/
data transfers;
set transfers;
	nextadmdate=specialtydischargedate;
	format nextadmdate date9.;
run;

/*create the previous discharge date*/
proc sort data=transfers; 
by patienticn newadmitdatetime specialtytransferdatetime; 
run;

data transfers;
set transfers;
	by patienticn newadmitdatetime;
		previousdisdate=ifn(first.patienticn=0 and first.newadmitdatetime=0, lag(specialtydischargedate),.); 
			/*previous specialty discharge date*/
	format previousdisdate date9.;
run;

/*Step 2--Remove all non-acute inpatient*/
data transfers;
set transfers;
	where acute=1;
run;

proc sort data=transfers; 
by patienticn newadmitdatetime; 
run;

data transfers; 
set transfers; 
drop specialtyvisit firstvisit lastvisit;
run;

data transfers;
set transfers;
		by patienticn newadmitdatetime;
			specialtyvisit+1;
				if first.newadmitdatetime then specialtyvisit=1;
				if first.newadmitdatetime then firstvisit=1;
				if last.newadmitdatetime then lastvisit=1;	
	run;

/*Step 3--create nextspecadm and previousspecdis variables*/
proc sort data=transfers;
by patienticn specialtytransferdatetime; 
run;

data transfers;
set transfers;
	by patienticn specialtytransferdate;
		previousspecdis=ifn(first.patienticn=0, lag(specialtydischargedate),.); /*previous specialty discharge date*/
format previousspecdis specialtydischargedate date9.;
run;

proc sort data=transfers;
by patienticn descending specialtytransferdate;
run;

data transfers;
set transfers;
	by patienticn;
		nextspecadm=ifn(first.patienticn=0, lag(specialtytransferdate),.); 
			/*next specialty transfer date*/
	format nextspecadm date9.;
	run;

/*If the next hospital admission is within one day of next inpat admission that counts as same hospitalization
	Otherwise, we have a new hospitalization
	Create a new indicator for firstvist and renumerate the specialtyvisit*/

proc sort data=transfers; 
by patienticn specialtytransferdate; 
run; 

data transfers;
set transfers;
drop firstvisit lastvisit specialtyvisit; 
run;

data transfers;
set transfers;
	by patienticn;
	if first.patienticn then firstvisit=1;
		else if specialtytransferdate>previousspecdis+1 then firstvisit=1;/*If the current specialty transfer is 
		more than one day later than the previous specialty discharge, then consider new hospitalization*/
run;

data transfers;
set transfers;
		specialtyvisit+1;
		by patienticn;
			if first.patienticn then specialtyvisit=1;
			else if specialtytransferdate>previousspecdis+1 then specialtyvisit=1;
run;

/*Change the admission dates*/
data transfers;
set transfers;
	by patienticn;
	retain newadmitdatetime2;
		if firstvisit=1 then newadmitdatetime2=specialtytransferdatetime;
	format newadmitdatetime2 datetime22.3;
	run;

data transfers;
set transfers;
if not missing(newadmitdatetime2) then newadmitdatetime=newadmitdatetime2;
drop newadmitdatetime2;
format newadmitdatetime datetime22.3;
run;

data transfers;
set transfers;
newadmitdate=datepart(newadmitdatetime);
format newadmitdate date9.;
run;

proc sort data=transfers; 
by patienticn newadmitdate specialtyvisit; 
run;

/*create an indicator for last visit*/
data transfers;
set transfers;
	by patienticn newadmitdate;
		if last.newadmitdate then lastvisit=1;
	run;

/*Change the discharge dates*/
proc sort data=transfers;
	by patienticn newadmitdate descending specialtyvisit;
run; 

data transfers;
set transfers;
	by patienticn newadmitdate;
	retain newdischargedatetime2;
		if lastvisit=1 then newdischargedatetime2=specialtydischargedatetime;
		format newdischargedatetime2 datetime22.3;
	run;

data transfers;
set transfers;
if not missing(newdischargedatetime2) then newdischargedatetime=newdischargedatetime2;
newdischargedate=datepart(newdischargedatetime);
drop newdischargedatetime2;
run;

data transfers (compress=yes);
set transfers;
	newadmitdate=datepart(newadmitdatetime);
	newdischargedate=datepart(newdischargedatetime);
	format newadmitdate newdischargedate date9.;
run;

/*Now merge back together to capture all acute hospitalizations for year &year.*/
data icu.allacute&year. (compress=yes);
set notransfers allacute transfers;
run;

data icu.allacute&year. (compress=yes);
set icu.allacute&year.;
format newadmitdate mmddyy10.  newdischargedate mmddyy10. specialtydischargedate mmddyy10. specialtydischargedate mmddyy10.;
run;

/**** Noticed that Med, Surg, ED, & Neuro Observations were mostly coded as separate hospitalizations under cdw_admitdatetime & cdw_dischargedatetime.
Therefore, whenever the patient have an Observation stay, the hospitalizations date were not rolled up like it should.
To further code in order to roll up the hospitalization dates ****/
/*Goal to create single-site hospitalizations. site=sta6a*/

/*1.assign each patienticn, newadmitdate & newdischargedate a unique hosp id*/;
/*create unique patient hosp count*/
PROC SORT DATA=icu.allacute&year. nodupkey  OUT=final_copy_undup2 (compress=yes); 
BY  patientsid sta6a newadmitdate newdischargedate;
RUN;

DATA final_copy_undup2 (compress=yes);
SET final_copy_undup2 ;
unique_hosp=_N_; 
RUN;

/*match unique_hosp back to original dataset icu.allacute&year.*/
PROC SQL;
	CREATE TABLE  final_copy2  (compress=yes)  AS 
	SELECT A.*, B.unique_hosp
	FROM  icu.allacute&year.  A
	LEFT JOIN final_copy_undup2  B ON A.patientsid =B.patientsid and a.sta6a=b.sta6a 
             and a.newadmitdate=b.newadmitdate and a.newdischargedate=b.newdischargedate;
QUIT;

/*use specialtytransferdatetime & specialtydischargedatetime*/
PROC SORT DATA=final_copy2; 
BY patientsid unique_hosp sta6a specialtytransferdatetime  specialtydischargedatetime;
RUN;

DATA final_copy3 (compress=yes); 
SET  final_copy2;
by patientsid /*patienticn*/;
if first.patientsid /*patienticn*/ then do;
	lag_specialtydischargedate=specialtydischargedate;  end;  /*create a lag_specialtydischargedate for first unique patient, because they shouldn't have a lag_specialtydischargedate, so it is = specialtydischargedate */
lag_specialtydischargedate2=lag(specialtydischargedate); /*create a lag_specialtydischargedate2*/
format lag_specialtydischargedate mmddyy10.  lag_specialtydischargedate2 mmddyy10.;
RUN;

/*if lag_specialtydischargedate is missing, then replace it with lag_specialtydischargedate2*/
DATA final_copy4 (compress=yes);
SET final_copy3;
if lag_specialtydischargedate NE . then lag_specialtydischargedate2= .;
if lag_specialtydischargedate = . then lag_specialtydischargedate=lag_specialtydischargedate2;
drop lag_specialtydischargedate2;
diff_days=specialtytransferdate-lag_specialtydischargedate; /*calculate date difference from last specialty discharge*/
RUN;

/*sta6a should be within each unique hosp not patienticn*/
/*by unique_hosp, get lag_sta6a=sta6a  first for the first admit date, should be the same*/
PROC SORT DATA=final_copy4   OUT=final_copy5 (compress=yes) ;
BY  unique_hosp;
RUN;

data final_copy6 (compress=yes);
set final_copy5;
by unique_hosp;
if first.unique_hosp then do;
	lag_sta6a=sta6a;  end;
lag_sta6a2=lag(sta6a);
run;

/*if lag_newadmitdatetime and lag_sta6a is missing, then replace it with lag_newadmitdatetime_v2*/
DATA final_copy8 (compress=yes); 
SET  final_copy6 ;
if lag_sta6a NE '' then lag_sta6a2= '';
if lag_sta6a = '' then lag_sta6a=lag_sta6a2;
drop lag_sta6a2 ;
run;

/*create first patienticn indicator, if first.patientinc is true, then it's a new hosp*/
DATA  final_copy8b (compress=yes);
SET final_copy8 ;
by patientsid /*patienticn*/;
if first.patientsid /*patienticn*/ then first_pat=0;
 first_pat+1;
RUN;

/*if diff_days >1 or  diff_days<0 then it's a new hosp, also check if it's the same facility*/
DATA final_copy9 (compress=yes); 
SET final_copy8b;
if (first_pat=1 and diff_days=0)  /*on a sorted dataset, if it's a first unique patient, then it is a new hosp and diff_days should be 0*/
OR
((diff_days >1 or diff_days<0) and (lag_sta6a=sta6a))  /*in same facility, if diff_days >1 then it is a new hosp, also if admit-previous discharge<0 then it is new hosp because admission < discharge date if fist hosp*/
	then new_hosp_ind=1; else new_hosp_ind=0;
RUN;

/*check to see previous step works before only selecting new_hosp_ind=1*/
DATA  final_copy10 (compress=yes); 
SET final_copy9;
if new_hosp_ind=1;
RUN;

/*assign each unique_hosp and new_hosp_ind a unique ID*/
PROC SORT DATA=final_copy10  nodupkey  OUT=Unique_hosp_ind (compress=yes);
BY  patientsid sta6a unique_hosp new_hosp_ind;
RUN;

DATA Unique_hosp_ind (compress=yes);  
SET  Unique_hosp_ind;
Unique_hosp_ind=_n_;
RUN;

/*left join Unique_hosp_ind back to original dataset final_copy9*/
PROC SQL;
	CREATE TABLE  final_copy11 (compress=yes)  AS 
	SELECT A.*, B.Unique_hosp_ind
	FROM  final_copy9 A
	LEFT JOIN Unique_hosp_ind  B ON A.patientsid =B.patientsid and a.sta6a=b.sta6a and a.unique_hosp=b.unique_hosp;
QUIT;

/*fill down in a table for Unique_hosp_ind*/
data  final_copy12 (drop=filledx compress=yes);  
set final_copy11;
retain filledx; /*keeps the last non-missing value in memory*/
if not missing(Unique_hosp_ind) then filledx=Unique_hosp_ind; /*fills the new variable with non-missing value*/
Unique_hosp_ind=filledx;
run;

PROC SORT DATA=final_copy12;
BY  patienticn patientsid sta6a specialtytransferdatetime specialtydischargedatetime cdw_admitdatetime;
RUN;

/*use max and min group by Unique_ICU_specialty to get new speicaltytransferdate and specialtydischargedates*/
PROC SQL;
CREATE TABLE  final_copy13 (compress=yes) AS   
SELECT *, min(specialtytransferdate) as new_admitdate2, max(specialtydischargedate) as new_dischargedate2
FROM final_copy12
GROUP BY Unique_hosp_ind;
QUIT;

DATA final_copy13 (compress=yes); 
SET  final_copy13;
format new_admitdate2 mmddyy10. new_dischargedate2 mmddyy10.; /* the new single-site hospitalizations admission and discharge dates are new_admitdate2 and new_dischargedate2*/
RUN;

DATA  icu.allacute&year._date (compress=yes); 
SET  final_copy13;
RUN;

proc sort data=icu.allacute&year._date  nodupkey; 
	by patienticn specialtytransferdatetime specialtydischargedatetime; run;

/*Condition on surviving the hospitalization for calculating the readmission rate, inhospmort, and mort30*/
data icu.allacute&year._date (compress=yes); 
set icu.allacute&year._date;
if not missing(dod_09212018_pull) then do;
	deathdaysafterdischarge=datdif(new_dischargedate2, dod_09212018_pull, 'act/act');  
	deathdaysafteradmit=datdif(new_admitdate2, dod_09212018_pull, 'act/act');
end;
if not missing(dod_09212018_pull) and abs(deathdaysafterdischarge)<=1 then inhospmort=1;
	else inhospmort=0;
if not missing(dod_09212018_pull) and abs(deathdaysafteradmit) <=30 then mort30=1;
       else mort30=0;
label mort30='30-day mortality indicator, after admission date, those that died within 30 days after being admitted to hospital' 
      inhospmort='in-hospital mortality indicator, 1=died while inpatient, 0=dicharged alive';
run;



/**** END OF STEP 1B ****/
