/* Date Modified: June 2018
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

data icu.vapd_inpat&year.;
set icu.vapd_inpat&year.;
if specialty in ('CARDIAC INTENSIVE CARE UNIT', 'MEDICAL ICU', 'SURGICAL ICU') then icu=1;
else icu=0;
run;

data icu.vapd_inpat&year.;
set icu.vapd_inpat&year.;
admityear=year(datepart(admitdatetime));
run;

/*Identify hospitalizations with erroneous Date Of Death (DOD)*/
data icu.vapd_inpat&year.;
set icu.vapd_inpat&year.;
if not missing(dod) then do;
disdate1=datepart(intnx ('dtday', dischargedatetime, -1));
end;
format disdate1 date9.;
run;

data error_dod;
set icu.vapd_inpat&year. (keep=inpatientsid dischargedatetime disdate1 dod);
if not missing(dod) then do;
if dod<disdate1 then output;
end;
run;

/*Remove hospitalizations with erroneous DOD*/
proc sql;
create table vapd_inpat&year. AS
select a.* 
from icu.vapd_inpat&year. a
left join error_dod b
on a.inpatientsid=b.inpatientsid
where b.inpatientsid is null;
quit;

/*Indicate whether the diagnosis codes are in ICD-9 or ICD-10*/

data icu.vapd_inpat&year.;
set vapd_inpat&year.;
if icd10code ne "*Unknown at this time*" and icd9code="*Unknown at this time*" then icdtype="ICD10";
else if icd9code ne "*Unknown at this time*" and icd10code="*Unknown at this time*" then icdtype="ICD9";
run;

/*Sort by patient, admission, ordinal number.*/
proc sort data=icu.vapd_inpat&year.; by patienticn inpatientsid specialtytransferdatetime OrdinalNumber; run;

/*Output the ICD-9 and ICD-10 diagnoses into seperate datasets*/
data icd9 icd10;
set icu.vapd_inpat&year.;
if icdtype="ICD9" then output icd9;
if icdtype="ICD10" then output icd10;
run;

proc sort data=icd9 nodupkey; by InpatientSID specialtytransferdatetime ordinalNumber; run;
proc sort data=icd10 nodupkey; by inpatientsid specialtytransferdatetime ordinalnumber; run;

/*transpose diagnoses long to wide*/
proc transpose data=icd9 out=icd9_wide prefix=icd9code;
by inpatientsid specialtytransferdatetime;
var icd9code;
run;

proc sort data=icd9_wide nodupkey; by inpatientsid specialtytransferdatetime; run;

data icd9_wide ;
set icd9_wide ;
icdtype='ICD9';
run;

proc transpose data=icd10 out=icd10_wide prefix=icd10code;
by inpatientsid specialtytransferdatetime;
var icd10code;
run;

proc sort data=icd10_wide nodupkey; by inpatientsid specialtytransferdatetime; run;

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
	by patienticn inpatientsid specialtytransferdatetime; run;

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

data inp&year.;
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
proc sort data=inp&year.; by patienticn sta6a newadmitdatetime specialtytransferdatetime; run;

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
proc sort data=inp&year.; by patienticn newadmitdate specialtytransferdate; run;

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

proc sort data=inp&year. nodupkey; by patienticn specialtytransferdatetime sta6a specialty descending icu; run;
proc sort data=inp&year. nodupkey dupout=dups; by patienticn specialtytransferdatetime sta6a specialty; run;

/*Address issue where a bed is held in non-acute setting
--same discharge dates, but different admission dates*/
proc sort data=inp&year.; by patienticn newdischargedatetime newadmitdatetime; run;

data inp&year.;
set inp&year.;
by patienticn newdischargedatetime;
		previous_admit=ifn(first.newdischargedatetime=0, lag(newadmitdatetime),.);
	format  newadmitdatetime newdischargedatetime specialtytransferdatetime previous_admit datetime22.;
	informat newadmitdatetime newdischargedatetime specialtytransferdatetime previous_admit datetime22.;
run;

proc sort data=inp&year.; by patienticn newdischargedatetime descending newadmitdatetime ; run;

data inp&year.;
set inp&year.;
by patienticn newdischargedatetime;
		next_admit=ifn(first.newdischargedatetime=0, lag(newadmitdatetime),.);
	format  next_admit datetime22.;
	informat next_admit datetime22.;
run;

proc sort data=inp&year.; by patienticn newdischargedatetime newadmitdatetime; run;

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

proc sort data=bedheld; by patienticn newadmitdatetime; run;

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

proc sort data=bedheld; by patienticn newdischargedatetime newadmitdatetime; run;

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
proc sort data=inp&year. nodupkey; by patienticn newadmitdatetime descending specialtytransferdatetime; run;

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

proc sort data=transfers nodupkey; by patienticn newadmitdatetime specialtytransferdatetime; run;

/*reenumerate*/
data transfers;
set transfers;
	drop specialtyvisit firstvisit lastvisit;
run;

proc sort data=transfers ; by patienticn newadmitdatetime specialtytransferdatetime newdischargedatetime;
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

proc sort data=transfers nodupkey; by patienticn newadmitdatetime descending specialtytransferdatetime icu; run;

/*Create the next specialty admission date*/
data transfers;
set transfers;
	nextadmdate=specialtydischargedate;
	format nextadmdate date9.;
run;

/*create the previous discharge date*/
proc sort data=transfers; by patienticn newadmitdatetime specialtytransferdatetime; run;

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

proc sort data=transfers; by patienticn newadmitdatetime; run;

data transfers; set transfers; drop specialtyvisit firstvisit lastvisit; run;

data transfers;
set transfers;
		by patienticn newadmitdatetime;
			specialtyvisit+1;
				if first.newadmitdatetime then specialtyvisit=1;
				if first.newadmitdatetime then firstvisit=1;
				if last.newadmitdatetime then lastvisit=1;	
	run;

/*Step 3--create nextspecadm and previousspecdis variables*/
proc sort data=transfers; by patienticn specialtytransferdatetime; run;

data transfers;
set transfers;
	by patienticn specialtytransferdate;
		previousspecdis=ifn(first.patienticn=0, lag(specialtydischargedate),.); /*previous specialty discharge date*/
format previousspecdis specialtydischargedate date9.;
run;

proc sort data=transfers; by patienticn descending specialtytransferdate; run;

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

proc sort data=transfers; by patienticn specialtytransferdate; run; 

data transfers;
set transfers;
	drop firstvisit lastvisit specialtyvisit; run;

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

proc sort data=transfers; by patienticn newadmitdate specialtyvisit; run;

/*create and indicator for last visit*/
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

data transfers;
set transfers;
	newadmitdate=datepart(newadmitdatetime);
	newdischargedate=datepart(newdischargedatetime);
	format newadmitdate newdischargedate date9.;
run;

/*Now merge back together to capture all acute hospitalizations for year &year.*/
data icu.allacute&year.;
set notransfers allacute transfers;
run;

data icu.allacute&year.;
set icu.allacute&year.;
format newadmitdate newdischargedate specialtydischargedate date9.;
run;

proc sort data=icu.allacute&year. nodupkey; 
	by patienticn specialtytransferdatetime specialtydischargedatetime; run;

/*Counts of distinct hospitalization by year*/
proc sql;
select admityear, count(distinct cat(patienticn,newadmitdatetime)) 
from icu.allacute&year. 
group by admityear;
quit;

/*Condition on surviving the hospitalization for calculating the readmission rate*/
data icu.allacute&year.;
set icu.allacute&year.;
if not missing(dod) then do;
	deathdaysafterdischarge=datdif(newdischargedate, dod, 'act/act');
end;
run;

/*If death occurred before discharge*/
data icu.allacute&year.;
set icu.allacute&year.;
	if not missing(deathdaysafterdischarge) and abs(deathdaysafterdischarge)<=1 then inhospmort=1;
	else inhospmort=0;
	if not missing(deathdaysafterdischarge) and abs(deathdaysafterdischarge)<=30 then mort30=1;
	else mort30=0; 
run;

/**** END OF STEP 1B ****/
