/*This code will identify which hospitalization has the criteria for CDC EHR definition of Sepsis. Criteria include: basic, blood culture, 
lab, meds, vasopressors, antibiotic, diagnosis, mechanical ventilation indicators*/

/* Date Modified: 6/29/2018
   Author: Shirley Wang */

libname raw  'INSERT FOLDER PATH WHERE VAPD DATASET IS SAVED IN STEP 3';
%let year=20142017;


/********* Dataset Preparations *********/
/*create basic dataset*/
DATA raw.basic (rename=(inpatientsid=admission_id patienticn=patient_id sta6a=hospital_id admityear=year 
		hosp_LOS=discharge_date  hospital_day=day) compress=yes); 
retain patienticn sta6a  inpatientsid age gender race admityear newadmitdate  newdischargedate datevalue  hospital_day hosp_LOS
icu_day readmit30 inhospmort mort30 discharge_dispo;
SET  raw.vapd_daily&year._v23 ; /*dataset was saved from step 2 of VAPD*/
keep patienticn sta6a  inpatientsid age gender race admityear newadmitdate  newdischargedate datevalue specialtytransferdate specialtydischargedate
hospital_day hosp_LOS icu_day   readmit30 inhospmort mort30 discharge_dispo ;
RUN; 

/***************************************************************************/
/*laboratory dataset*/
data raw.laboratory (compress=yes); 
set raw.vapd_daily&year._v23;
keep patienticn sta6a  inpatientsid  datevalue  hospital_day hi_creat_daily lo_creat_daily hi_bili_daily lo_bili_daily
hi_plate_daily lo_plate_daily hi_lactate_daily lo_lactate_daily;
rename 
inpatientsid=admission_id 
patienticn=patient_id 
sta6a=hospital_id
hospital_day=day
hi_creat_daily=cr_max
lo_creat_daily=cr_min
hi_bili_daily=tbili_max
lo_bili_daily=tbili_min
hi_plate_daily= plt_max
lo_plate_daily=plt_min
hi_lactate_daily=lactate_max
lo_lactate_daily=lactate_min;
run;

/***************************************************************************/
/*Bloodculture*/
DATA  raw.bloodculture (compress=yes);
SET  raw.vapd_daily&year._v23;
if Blood_cult_ind =1;
keep patienticn inpatientsid hospital_day ;
rename 	inpatientsid=admission_id 
		hospital_day=bcx_drawn_day 
		patienticn=patient_id ;
RUN;

/***************************************************************************/
/*medications*/
/*med_type=1 is vasopressors and all IV, med_type=2 is antibiotics (ABX)*/
data pressors (compress=yes);
retain patienticn inpatientsid datevalue hospital_day pressor_1-pressor_5;
set raw.vapd_daily&year._v23;
keep patienticn inpatientsid hospital_day datevalue pressor_1-pressor_5;
run;

PROC SORT DATA=pressors nodupkey; 
BY   patienticn inpatientsid datevalue;
RUN;

/*transpose pressors into long form*/
proc transpose data=pressors  out=pressors_long1 (rename=COL1=med drop=_NAME_); 
var pressor_1-pressor_5;
by patienticn  inpatientsid datevalue;
run;

/*get back hosp_day*/
PROC SQL;
	CREATE TABLE  pressors_long2 (compress=yes)  AS 
	SELECT A.*, B.hospital_day as day
	FROM  pressors_long1   A
	LEFT JOIN pressors  B ON A.patienticn =B.patienticn and a.inpatientsid=b.inpatientsid and a.datevalue=b.datevalue ;
QUIT;

DATA pressors_long3; 
SET  pressors_long2;
if med='' then delete;
length route $2 med2 $200;
route='IV';
med_type=1;
med2=med;
RUN;

DATA  pressors_long3b (rename=med2=med); 
retain  patienticn  inpatientsid day med2  med_type route;
SET  pressors_long3;
drop med datevalue;
RUN;


/*Antibiotics*/
data abx (compress=yes);
retain patienticn  inpatientsid datevalue hospital_day abx_1-abx_11;
set raw.vapd_daily&year._v23;
keep patienticn  inpatientsid hospital_day datevalue abx_1-abx_11;
run;

PROC SORT DATA=abx  nodupkey; 
BY patienticn inpatientsid datevalue;
RUN;

/*transpose pressors into long form*/
proc transpose data=abx  out=abx_long1 (rename=COL1=med drop=_NAME_); 
var abx_1-abx_11;
by patienticn inpatientsid datevalue;
run;

/*get back hosp_day*/
PROC SQL;
	CREATE TABLE  abx_long2 (compress=yes)  AS
	SELECT A.*, B.hospital_day as day
	FROM abx_long1   A
	LEFT JOIN abx B ON A.patienticn =B.patienticn and a.inpatientsid=b.inpatientsid and a.datevalue=b.datevalue;
QUIT;

DATA abx_long3; 
retain patienticn  inpatientsid day med  med_type;
SET abx_long2;
if med='' then delete;
med_type=2;
keep patienticn  inpatientsid day med  med_type;
RUN;

PROC SQL;
CREATE TABLE abx_long4 AS 
SELECT *, case when med like '%_PO' then 'PO'  else 'IV'
end as route
from abx_long3;
QUIT;

DATA  raw.medication (compress=yes);
SET pressors_long3b abx_long4;
RUN;

DATA  raw.medication (compress=yes rename=(inpatientsid=admission_id patienticn=patient_id)); /*rename variables*/
SET  raw.medication;
RUN;

/***************************************************************************/
/*Diagnosis*/
/*need patient, admitID, hospital_id, icd10 primary dx, and icd9 primary dx*/
data diag (compress=yes);
set final.vapd_daily&year._v2;
keep patienticn  inpatientsid datevalue hospital_day icdtype icd9code1-icd9code25  icd10code1-icd10code26;
run;

/*take out decimals in icd10 and icd9 diag codes*/
data  diag2 (compress=yes); 
set  diag;
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
DATA icd9   icd10;
SET  diag2;
if icdtype='ICD10' then output icd10; 
if icdtype='ICD9' then output icd9;
RUN;

/*transpose icd9 into long form*/
PROC SORT DATA=icd9 nodupkey;
BY patienticn inpatientsid hospital_day;
RUN;
proc transpose data=icd9  out=icd9_long1  (rename=COL1=Diagnosiscode9 drop=_NAME_); 
var icd9code1-icd9code25;
by patienticn inpatientsid hospital_day;
run;
DATA icd9_long1; 
SET icd9_long1;
if Diagnosiscode9 ='' then delete; 
RUN;

/*transpose icd10 into long form*/
PROC SORT DATA=icd10 nodupkey;
BY patienticn inpatientsid hospital_day ;
RUN;

proc transpose data=icd10  out=icd10_long1 (rename=COL1=Diagnosiscode10 drop=_NAME_); 
var icd10code1-icd10code26;
by patienticn inpatientsid hospital_day;
run;

DATA icd10_long1; 
SET icd10_long1;
if Diagnosiscode10 ='' then delete; 
RUN;

/*combine icd9long and icd10long*/
data all_dia;
set icd10_long1 icd9_long1;
run;

DATA raw.diagnosis (compress=yes rename=(hospital_day=day inpatientsid=admission_id patienticn=patient_id )); 
retain patienticn  inpatientsid hospital_day Diagnosiscode10  Diagnosiscode9;
SET  all_dia;
keep  patienticn  inpatientsid hospital_day Diagnosiscode10  Diagnosiscode9;
RUN;

/***************************************************************************/
/*Mechanical Ventilation*/
/*need patient_id,Admission_ID, and day*/ 
DATA raw.mechvent (compress=yes); 
SET  raw.vapd_daily&year._v23;
if mech_vent_daily=1;
keep  patienticn inpatientsid hospital_day mech_vent_daily;
rename 	inpatientsid=admission_id 
		hospital_day=day 
		patienticn=patient_id;
RUN;

/* end of dataset processing */

/*********************************************************************************************************************************/

**************************************************************************************
* PROGRAM: 1.1-ehr_infection_v2.sas                                                  *
* DATE: 08/02/2016                                                                   *
* Robert Jin                                                                         *
*                                                                                    *
* Calcuates infection cases in Sepsis dataset using blood culture, laboratory, and   *
* basic dataset                                                                      *
* Modification 04/07/16: Use basic data from 04/05/16                                *
* Modification 04/07/16: Use cases with at least 1 QAD with IV route in QAD 1-4      *
* Modification 04/14/16: Use Day instead of PHADay and bcx_drawn_day instead of      *
*                        Coll_Day                                                    *
* Modification 04/20/16: Use EHR variables to run on EHR datasets                    *
* Modification 04/22/16: Fix CALC_QAD macro, for cases when Antibiotic other than    *
*                        VANCOMYCIN have 2 routes on same day, QAD is not            *
*                        incremented for 2nd route of same Antibiotic                *
* Modification 04/26/16: Add discharge_dispo code 2 for satisfying less than 4 QADs  *
*                        when last QAD >= discharge_date - 1                         *
* Modification 04/26/16: For antibiotics with gap of more than 2 days from previous  *
*                        antibiotic, if any antibiotic has matching antibiotic from  *
*                        2 days earliers, other antibiotics on same day will have    *
*                        same QAD assigned to them.                                  *
* Modification 05/02/16: Use new data v1_3                                           *
* Modification 05/03/16: Revise CALC_QAD macro                                       *
*                        Replace if [first.Day then prev_ab2 = 1] with               *
*                          [if first.Day and find_ab2 = 1 then prev_ab2 = 1]         *                                                                                    
*                          [else if first.Day then prev_ab2 = 0]                     * 
* Modification 05/03/16: Instead of selecting cases with at least 1 IV route in      *
*                        first 4 QAD days, select cases with at least 1 IV route in  *
*                        2 day window +/- from blood culture day                     *
* Modification 05/03/16:  Remove data with Day > Discharge_date from                 *
*                        antibiotic_qad_hosp1 dataset                                *
* Modification 05/05/16: Revise CALC_QAD macro                                       *
*                        Change [if first.Patient_ID then QAD_max = 1] with          *
*                               [if first.QAD_start then QAD_max = 1]                *
* Modification 05/16/16: Use new data received on 5/16/16                            *
* Modification 05/18/16: Remove blood cultures occurring after discharge date        *
* Modification 06/15/16: Rename variables to use those in specification              *
* Modification 06/17/16: qadenc_ge4 macro replace from antibiotic_qad_ip1 with       *
*                        from antibiotic_qad_&type.1                                 *
* Modification 07/26/16: Create section for user inputs                              *
* Modification 07/26/16: Revised code for antibiotic_qad2 table for renaming         *
*                        antibiotic variables                                        *
* Modification 07/22/16: Renamed final output infection dataset  					 *
* Modified 08/02/16: Include User Input File										 *
**************************************************************************************;

/*options nocenter pageno = 1 mprint macrogen;*/
/**/
/*%macro delete_ds(dslist);*/
/**/
/*    proc datasets library = work nolist;*/
/*        delete &dslist.;*/
/*    quit;*/
/**/
/*%mend delete_ds;*/


* Create basic dataset;
data basic_fnl (compress=yes); 
    set raw.basic;
	encounter_type = 'I';
run; 

/* Create records for each day blood culture was drwan */
proc sql;
    create table bloodculture_draw as 
        select distinct Admission_ID,
                        Patient_ID,
                        bcx_drawn_day
        from raw.bloodculture;
quit;

proc sql;
    create table bloodculture_draw2 as 
        select a.*
        from bloodculture_draw a,
             basic_fnl b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              a.bcx_drawn_day <= b.Discharge_Date;
quit;

/*%delete_ds(dslist = bloodculture_draw);*/

proc sql;
    create table antibiotic_raw1 as 
        select distinct Admission_ID,
                        Patient_ID,
                        upcase(Med) as Antibiotic length = 32,
                        Route,
                        Day
        from raw.medication;
quit;

/* Modified 06/17/2016: remove antibiotics not used in sepsis definition */
data antibiotic_raw2; 
    set antibiotic_raw1;

    if route = 'PO' and 
       Antibiotic IN("ACYCLOVIR", "AMANTADINE", "CLARITHROMYCIN", "ERYTHROMYCIN", "FAMCICLOVIR",
                     "GANCICLOVIR", "RIBAVIRIN", "RIFAMPIN", "RIMANTADINE", "VALACYCLOVIR",
                     "VALGANCICLOVIR", 
					 "ACYCLOVIR_PO", "AMANTADINE_PO", "CLARITHROMYCIN_PO", "ERYTHROMYCIN_PO", "FAMCICLOVIR_PO",
                     "GANCICLOVIR_PO", "RIBAVIRIN_PO", "RIFAMPIN_PO", "RIMANTADINE_PO", "VALACYCLOVIR_PO",
                     "VALGANCICLOVIR_PO"

) then delete;
    else if route = 'IV' and Antibiotic in('RIBAVIRIN','RIBAVIRIN_IV') then delete;
    
run;

/*%delete_ds(dslist = antibiotic_raw1);*/

proc sql;
    create table antibiotic_qad1 as  
        select distinct a.Admission_ID,
                        a.Patient_ID,
                        a.bcx_drawn_day,
                        b.Antibiotic,
                        b.Route,
                        b.Day
         from bloodculture_draw2 a,
              antibiotic_raw2 b
         where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID;
quit;

/*%delete_ds(dslist = antibiotic_raw2);*/

** Separate antibiotic data for cases with antibiotic = VANCOMYCIN;
data antibiotic_qad2 antibiotic_qad_van1;
    set antibiotic_qad1;

    /* Rename antibiotics with non alphabetic characters, replace non-alphabetic characters with [_] */

    ab_len = length(antibiotic);
    antibiotic = prxchange("s/\W/_/o", -1, antibiotic);
    do i = ab_len+1 to 32;
        if i > ab_len then substr(antibiotic,i,1) = ' ';
    end; 
    if antibiotic =: "AMPHOTERICIN_B" then antibiotic = "AMPHOTERICIN_B";

    if antibiotic = "VANCOMYCIN_PO" then antibiotic = "VANCOMYCIN";
    if antibiotic = "VANCOMYCIN_IV" then antibiotic = "VANCOMYCIN";

    if antibiotic ^= 'VANCOMYCIN' then output antibiotic_qad2; 
    else output antibiotic_qad_van1;
run;

/*%delete_ds(dslist = antibiotic_qad1);*/

/* Create a subset of antibiotic data */
proc sql;
    create table antibiotic_qad3 as 
        select distinct Admission_ID,
                        Patient_ID,
                        Antibiotic,
                        Day
        from antibiotic_qad2
        order by Admission_ID, Patient_ID, Antibiotic, Day;
quit;

/* Determine if antibiotic has been administered in previous 2 days */
/* and if a gap of 1 day exists between antibiotics                 */
data antibiotic_qad4; 
    set antibiotic_qad3;
    by Admission_ID Patient_ID antibiotic;
    diff = ifn(first.antibiotic = 0, Day - lag(Day), .);
    if first.antibiotic then do;
         antibiotic_new = 1;
    end;
    else do;
        if diff > 2 then antibiotic_new = 1;
        else antibiotic_new = 0;

    end;
run;

/*%delete_ds(dslist = antibiotic_qad3);*/

/* Combine variables for new antibiotic with original data */
proc sql;
    create table antibiotic_qad5 as
            select a.*,
                   b.antibiotic_new
        from antibiotic_qad2 a
        LEFT JOIN antibiotic_qad4 b
            ON a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
               a.Antibiotic = b.Antibiotic and a.Day = b.Day;
quit;

/*%delete_ds(dslist = antibiotic_qad2 antibiotic_qad4);*/

/* Determine if vancomycin is new and if a gap of 1 day exists between vancomycin for */
/* a route                                                                            */

/* Create a subset of antibiotic data */
proc sql;
    create table antibiotic_qad_van2 as
        select distinct Admission_ID,
                        Patient_ID,
                        Antibiotic,
                        Route,
                        Day
        from antibiotic_qad_van1
        order by Admission_ID, Patient_ID, Antibiotic, Route, Day;
quit;

data antibiotic_qad_van3(drop = diff);
    set Antibiotic_qad_van2;
    by Admission_ID Patient_ID Antibiotic Route;
    diff = ifn(first.route = 0, Day - lag(Day), .);
    if first.route then do;
        antibiotic_new = 1;
    end;
    else do;
        if diff > 2 then antibiotic_new = 1;
        else antibiotic_new = 0;
    end;
run;

/*%delete_ds(dslist = antibiotic_qad_van2);*/

/* Combine variables for new antibiotic with original data for VANCOMYCIN */
proc sql;
    create table antibiotic_qad_van4 as 
            select a.*,
                   b.antibiotic_new
        from antibiotic_qad_van1 a
        LEFT JOIN antibiotic_qad_van3 b
            ON a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
               a.Antibiotic = b.Antibiotic and a.Route = b.Route and a.Day = b.Day;
quit;

/*%delete_ds(dslist = antibiotic_qad_van1 antibiotic_qad_van3);*/

/* Combine non-VANCOMYCIN and VANCOMYCIN antibiotic results */
data antibiotic_qad_all1; 
    set antibiotic_qad5 antibiotic_qad_van4;
run;

/*%delete_ds(dslist = antibiotic_qad5 antibiotic_qad_van4);*/

/* Find new antibiotics within +/- 2 day surveillance window of blood culture */
proc sql;
    create table antibiotic_qad_all2 as  
        select distinct a.Admission_ID,
                        a.Patient_ID,
                        a.bcx_drawn_day,
                        b.Antibiotic,
                        b.Antibiotic_new,
                        b.Route,
                        b.Day
         from bloodculture_draw2 a,
              antibiotic_qad_all1 b
         where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
               a.bcx_drawn_day = b.bcx_drawn_day and
               b.antibiotic_new = 1 and -2 <= a.bcx_drawn_day - b.Day <= 2
         order by Admission_ID, Patient_ID, bcx_drawn_day, Day;
quit;

/*%delete_ds(dslist = bloodculture_draw2);*/

/* Find 1st QAD for antibiotic that is new by Admission_ID, Patient_ID, bcx_drawn_day */
data antibiotic_qad_first;  
    set antibiotic_qad_all2;
    by Admission_ID Patient_ID bcx_drawn_day;
    length antibiotic_first $40.;
    if first.bcx_drawn_day;
    Day_min = Day;
    antibiotic_first = antibiotic;
run;

/*%delete_ds(dslist = bloodculture_draw antibiotic_qad_all2);*/

/* Find antibiotics other than VANCOMYCIN occuring after 1st QAD in window       */
/* or on the same day but are different antibiotics from first antibiotic in QAD */
proc sql;
    create table antibiotic_qad_sub1 as  
        select distinct a.Admission_ID,
                        a.Patient_ID,
                        a.bcx_drawn_day,
                        a.Day,
                        a.antibiotic,
                        a.antibiotic_new,
                        b.Day_min,
                        b.antibiotic_first
        from antibiotic_qad_all1 a,
             antibiotic_qad_first b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              a.bcx_drawn_day = b.bcx_drawn_day and a.antibiotic ^= 'VANCOMYCIN' and
              (a.Day > b.Day_min or
               (a.Day = b.Day_min and a.antibiotic ^= b.Antibiotic)
              );
quit;

proc sort data = antibiotic_qad_sub1;
by Admission_ID Patient_ID bcx_drawn_day antibiotic Day;
run;

/* Select antibiotics other than VANCOMYCIN when 1st antibiotic is new */
/* or antibiotic is same as antibiotic for 1st QAD in window           */
data antibiotic_qad_sub2;
    set antibiotic_qad_sub1;
        by Admission_ID Patient_ID bcx_drawn_day antibiotic;
        if first.antibiotic and (antibiotic_new = 1 or antibiotic = antibiotic_first);
run;

/*%delete_ds(dslist = antibiotic_qad_sub1);*/

/* Select all records for antibiotics other than VANCOMYCIN when 1st antibiotic is new */
/* or antibiotic is same as antibiotic for 1st QAD in window                           */
proc sql;
    create table antibiotic_qad_sub3 as
        select a.*,
               b.Day_min,
               b.Antibiotic_first
        from antibiotic_qad_all1 a,
             antibiotic_qad_sub2 b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              a.bcx_drawn_day = b.bcx_drawn_day and a.Antibiotic = b.Antibiotic and
              a.Day >= b.Day;
quit;

/*%delete_ds(dslist = antibiotic_qad_sub2);*/

/* Find VANCOMYCIN antibiotics occuring on or after 1st QAD in window when  */
/* 1st antibiotic is not VANCOMYCIN                                         */
proc sql;
    create table antibiotic_qad_sub1a_van as
        select distinct a.Admission_ID,
                        a.Patient_ID,
                        a.bcx_drawn_day,
                        a.Day,
                        a.antibiotic,
                        a.Route,
                        a.antibiotic_new,
                        b.Day_min,
                        b.antibiotic_first
        from antibiotic_qad_all1 a,
             antibiotic_qad_first b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              a.bcx_drawn_day = b.bcx_drawn_day and a.antibiotic = 'VANCOMYCIN' and
              b.antibiotic ^= 'VANCOMYCIN' and
              (a.Day > b.Day_min or
               (a.Day = b.Day_min and a.antibiotic ^= b.Antibiotic)
              );
quit;

proc sort data = antibiotic_qad_sub1a_van;
by Admission_ID Patient_ID bcx_drawn_day antibiotic route Day;
run;

/* Select VANCOMYCIN when 1st antibiotic is new */
data antibiotic_qad_sub2a_van; 
    set antibiotic_qad_sub1a_van;
    by Admission_ID Patient_ID bcx_drawn_day antibiotic route;
    if first.route and antibiotic_new = 1;
run;

/*%delete_ds(dslist = antibiotic_qaD_sub1a_van);*/

/* Select all records for VANCOMYCIN when 1st antibiotic is new */
proc sql;
    create table antibiotic_qad_sub3a_van as
        select a.*,
               b.Day_min,
               b.Antibiotic_first
        from antibiotic_qad_all1 a,
             antibiotic_qad_sub2a_van b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              a.bcx_drawn_day = b.bcx_drawn_day and a.Antibiotic = b.Antibiotic and
              a.Route = b.Route and a.Day >= b.Day;
quit;

/*%delete_ds(dslist = antibiotic_qaD_sub2a_van);*/

/* Find VANCOMYCIN antibiotics occuring on or after 1st QAD in window when  */
/* 1st antibiotic is VANCOMYCIN                                             */
proc sql;
    create table antibiotic_qad_sub1b_van as
        select distinct a.Admission_ID,
                        a.Patient_ID,
                        a.bcx_drawn_day,
                        a.Day,
                        a.antibiotic,
                        a.Route,
                        a.antibiotic_new,
                        b.Day_min,
                        b.antibiotic_first,
                        b.route as route_first
        from antibiotic_qad_all1 a,
             antibiotic_qad_first b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              a.bcx_drawn_day = b.bcx_drawn_day and a.antibiotic = 'VANCOMYCIN' and
              b.antibiotic = 'VANCOMYCIN' and
              (a.Day > b.Day_min or
               (a.Day = b.Day_min and a.route ^= b.route)
              );
quit;

proc sort data = antibiotic_qad_sub1b_van;
by Admission_ID Patient_ID bcx_drawn_day antibiotic route Day;
run;

/* Select VANCOMYCIN when 1st antibiotic is new or route is different than 1st route */
data antibiotic_qad_sub2b_van;
    set antibiotic_qad_sub1b_van;
    by Admission_ID Patient_ID bcx_drawn_day antibiotic route;
    if first.route and (antibiotic_new = 1 or route = route_first);
run;

/*%delete_ds(dslist = antibiotic_qaD_sub1b_van);*/

/* Select all records for VANCOMYCIN when 1st antibiotic is new */
proc sql;
    create table antibiotic_qad_sub3b_van as
        select a.*,
               b.Day_min,
               b.Antibiotic_first
        from antibiotic_qad_all1 a,
             antibiotic_qad_sub2b_van b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              a.bcx_drawn_day = b.bcx_drawn_day and a.Antibiotic = b.Antibiotic and
              a.Route = b.Route and a.Day >= b.Day;
quit;

/*%delete_ds(dslist = antibiotic_qad_all1 antibiotic_qad_sub2b_van);*/

/* Combine antibiotics for non-Vancomycin and Vancomycin */
data antibiotic_qad_all3; 
    set antibiotic_qad_first antibiotic_qad_sub3 antibiotic_qad_sub3a_van antibiotic_qad_sub3b_van;
run;

/*%delete_ds(dslist = antibiotic_qad_first antibiotic_qad_sub3 antibiotic_qad_sub3a_van*/
/*                    antibiotic_qad_sub3b_van);*/

proc sort data = antibiotic_qad_all3; 
by Admission_ID Patient_ID bcx_drawn_day Day;
run;

/* Create list of antibiotics for each Admission_ID, Patient_ID, bcx_drawn_day Day */
proc transpose data = antibiotic_qad_all3
     out = antibiotic_trans(drop = _name_) prefix = lag_ab;
     var antibiotic;
     by Admission_ID Patient_Id bcx_drawn_day Day;
run;

/* Create list of routes for each Admission_ID, Patient_ID, bcx_drawn_day, Day    */
/* for VANCOMYCIN                                                                 */
proc transpose data = antibiotic_qad_all3
     out = antibiotic_trans_route(drop = _name_) prefix = van_route;
     var route;
     by Admission_ID Patient_Id bcx_drawn_day Day;
     where Antibiotic = "VANCOMYCIN";
run;

/* Find maximum number of antibiotics for an Admission_ID, Patient_ID, bcx_drawn_day, Day */
proc sql noprint;
    select max(N) into :nab from
         (select Admission_ID,
                 Patient_ID,
                 bcx_drawn_day,
                 Day,
                 count(*) as N
          from antibiotic_qad_all3
          group by Admission_ID, Patient_ID, bcx_drawn_day, Day);
quit;

%let nab = %trim(&nab.);

/* Find maximum number of routes for an Admission_ID, Patient_ID, bcx_drawn_day, Day */
proc sql noprint;
    select max(N) into :nrt from
         (select Admission_ID,
                 Patient_ID,
                 bcx_drawn_day,
                 Day,
                 count(*) as N
          from antibiotic_qad_all3
          where Antibiotic = 'VANCOMYCIN'
          group by Admission_ID, Patient_ID, bcx_drawn_day, Day);
quit;

%let nrt = %trim(&nrt.);

/* Retrieve antibiotics from previous day and previous 2 days */
/* and vancomycin routes from previous 2 days                 */

%macro makeab;

    proc sql;
        create table antibiotic_qad_all4 as
            select a.*,
                 %do i = 1 %to &nab.;
                     b.lag_ab&i. as lag1_ab&i.,
                 %end;
                 %do i = 1 %to &nab.;
                     c.lag_ab&i. as lag2_ab&i.,
                 %end;

                 %if &nrt > 0 %then %do;
                     %do i = 1 %to %eval(&nrt- 1);
                         d.van_route&i. as lag2_van_route&i.,
                     %end;
                     d.van_route&nrt. as lag2_van_route&nrt.

                 %end;
                 %else %do;
                     " " as lag2_van_route1
                 %end;

            from antibiotic_qad_all3 a
            left join antibiotic_trans b
                on a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
                   a.bcx_drawn_day = b.bcx_drawn_day and a.Day = b.Day + 1
            left join antibiotic_trans c
                on a.Admission_ID = c.Admission_ID and a.Patient_ID = c.Patient_ID and
                   a.bcx_drawn_day = c.bcx_drawn_day and a.Day = c.Day + 2
            left join antibiotic_trans_route d
                on a.Admission_ID = d.Admission_ID and a.Patient_ID = d.Patient_ID and
                   a.bcx_drawn_day = d.bcx_drawn_day and a.Day = d.Day + 2;
    quit;

/*    %delete_ds(dslist = antibiotic_qad_all3);*/

    /* Deterimine if antibiotic exsists in record 2 days prior to current record */
    data antibiotic_qad_all5;
        set antibiotic_qad_all4;
        array lag2var [&nab] lag2_ab1-lag2_ab&nab.;
        %if &nrt > 0 %then %do;
            array lag2rt [&nrt] lag2_van_route1-lag2_van_route&nrt.;
        %end;
        find_ab2 = 0;
        do i = 1 to &nab.;
            /* For antibiotics other than VANCOMYCIN check if record exists 2 days ago */
            if antibiotic = lag2var[i] then do;
                if antibiotic ^= "VANCOMYCIN" then do;
                    find_ab2 = 1;
                    i = &nab.;
                end;
                /* For Vancomycin, check if route matches from 2 days ago */
                else do;
                    %if &nrt. > 0 %then %do;
                        do j = 1 to &nrt.;
                            if route = lag2rt[j] and lag2rt[j] ^= " " then do;
                                find_ab2 = 1;
                                j = &nrt.;
                                i = &nab.;
                            end;
                        end;
                    %end;
                end;
            end;
        end;
        no_find_ab2 = 1 - find_ab2;
    run;

/*    %delete_ds(dslist = antibiotic_qad_all4);*/

%mend makeab;

%makeab;


/* Find all antibiotics in dataset and store in macro variable */
proc sql noprint;
    select distinct antibiotic into :all_antibiotic separated by ' '
        from antibiotic_qad_all5;
quit;

/*************************************************************************************/
/* Create rolling sequences of QADs after blood culture date for each day within     */
/* 2 days of blood culture where new antibiotic is received                          */
/*************************************************************************************/

proc sql;
    create table bc_qad_start as 
       select distinct Admission_ID,
                       Patient_ID,
                       bcx_drawn_day,
                       day as QAD_start
       from antibiotic_qad_all5
       where -2 <= bcx_drawn_day - day <= 2 and antibiotic_new = 1;
quit;

proc sql;
    create table antibiotic_qad_all5_roll as  
        select a.*,
               b.QAD_start
        from antibiotic_qad_all5 a,
             bc_qad_start b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              a.bcx_drawn_day = b.bcx_drawn_day and a.Day >= b.QAD_start;
quit;


/**************************************************************************************/

proc sort data = antibiotic_qad_all5_roll; 
by Admission_ID Patient_ID bcx_drawn_day QAD_start Day no_find_ab2 Antibiotic;
run;

%global &all_antibiotic VANCOMYCIN_IV VANCOMYCIN_PO;

%macro calc_qad;

/* Determine QADs */
/* QAD max is the maximum QAD value in a Admission_ID, Patient_ID, bcx_drawn_day record */

data antibiotic_qad_all5a(drop = i lag: find_ab2 prev_ab2 no_find_ab2 Day_min QAD_max);
    set antibiotic_qad_all5_roll;
    by Admission_ID Patient_ID bcx_drawn_day QAD_start Day no_find_ab2 Antibiotic;
    retain QAD QAD_max prev_ab2;

    if first.Day and find_ab2 = 1 then prev_ab2 = 1;
    else if first.Day then prev_ab2 = 0;

    if first.QAD_start then QAD_max = 1;
    if first.Day and find_ab2 = 0 and
       cmiss(of lag1_ab1-lag1_ab&nab.) = &nab. then QAD_max = 1;

    /* 1. If record is first for a bcx_drawn_day or the Day is the same as the        */
    /*    smallest Day, set QAD to 1                                                  */
    /* 2. If no antibiotics in previous day or from 2 days ago, reset QAD to 1        */
    /* 3. If antibiotic exists in previous day and record is first for a Day          */
    /*    increment QAD + 1 from maximum QAD on previous day                          */
    /* 4. If antibiotic exists in previous day and record is not first for a Day      */
    /*    keep QAD as same value as previous record                                   */
    /* 5. If no antibiotics in previous day and antibiotic exists from 2 days ago,    */
    /*    check if antibiotic in current record exists from 2 days ago                */
    /*    If it does exist, increment QAD by 2, if it does not exist reset QAD to 1   */
    /*    For Vancomycin, routes must be same between Day and Day - 2                 */
    /*    Macro variable with name of Antibiotic is used to store and process QAD     */
    /*    value as dataset is processed. Macro variables VANCOMYCIN_IV, VANCOMYCIN_PO */
    /*    are used to store QAD values for IV, PO routes                              */

    /* QAD max is the maximum QAD value in a Admission_ID, Patient_ID, bcx_drawn_day, QAD_Start  record  */
    /* QAD_max is updated as each record is processed                                                    */

    if first.QAD_start or Day = QAD_start then do;
         QAD = 1;
         if Antibiotic ^= "VANCOMYCIN" then call symput(Antibiotic, QAD);
         else if Antibiotic = "VANCOMYCIN" then
            call symput("VANCOMYCIN_" || trim(Route), QAD);
    end;
    else if cmiss(of lag1_ab1-lag1_ab&nab.) = &nab and
            cmiss(of lag2_ab1-lag2_ab&nab.) = &nab then do;
        QAD = 1;
        if Antibiotic ^= "VANCOMYCIN" then call symput(Antibiotic, QAD);
        else if Antibiotic = "VANCOMYCIN" then
            call symput("VANCOMYCIN_" || trim(Route), QAD);
    end;
    else if cmiss(of lag1_ab1-lag1_ab&nab.) ^= &nab. and first.Day
         then do;
              QAD = QAD_max + 1;
              if antibiotic ^= "VANCOMYCIN" then call symput(Antibiotic, QAD);
              else if antibiotic = "VANCOMYCIN" then
                 call symput("VANCOMYCIN_" || trim(route), QAD);
         end;
    else if cmiss(of lag1_ab1-lag1_ab&nab.) ^= &nab. and first.Day = 0
          then do;
               QAD + 0;
               if Antibiotic ^= "VANCOMYCIN" then call symput(Antibiotic, QAD);
               else if antibiotic = "VANCOMYCIN" then
                   call symput("VANCOMYCIN_" || trim(route), QAD);
          end;
    else if cmiss(of lag1_ab1-lag1_ab&nab.) = &nab. and
            cmiss(of lag2_ab1-lag2_ab&nab.) ^= &nab.
            then do;
                 if find_ab2 = 0 and prev_ab2 = 0 then do;
                     QAD = 1;
                     if Antibiotic ^= "VANCOMYCIN" then call symput(Antibiotic, QAD);
                     else if antibiotic = "VANCOMYCIN" then
                        call symput("VANCOMYCIN_" || trim(route), QAD);
                 end;
                 else if prev_ab2 = 1 and Antibiotic ^= "VANCOMYCIN" and first.Day then do;
                     QAD = symget(Antibiotic) + 2;
                     call symput(Antibiotic, QAD);
                 end;
                 else if prev_ab2 = 1 and Antibiotic ^= "VANCOMYCIN" and first.Day = 0 then do;
                     QAD + 0;
                     call symput(Antibiotic, QAD);
                 end;
                 else if prev_ab2 = 1 and Antibiotic = "VANCOMYCIN" and first.Day then do;
                     QAD = symget("VANCOMYCIN_" || trim(Route)) + 2;
                     call symput("VANCOMYCIN_" || trim(Route), QAD);
                 end;
                 else if prev_ab2 = 1 and Antibiotic = "VANCOMYCIN" and first.Day = 0 then do;
                     QAD + 0;
                     call symput("VANCOMYCIN_" || trim(Route), QAD);
                 end;
    end;

    /* Reset macro variables after last record for Admission_ID, Patient_ID, bcx_drawn_day  */

    if last.QAD_start then do;
         %let i = 1;
         %do %while(%length(%scan(&ALL_ANTIBIOTIC, &i)));
              call symput("%scan(&ALL_ANTIBIOTIC, &i.)", 1);
              %let i = %eval(&i + 1);
         %end;
         call symput("VANCOMYCIN_IV", 1);
         call symput("VANCOMYCIN_PO", 1);
    end;

    /* Reset QAD_max value */
    QAD_max = max(QAD, QAD_max);

run;

/*%delete_ds(dslist = antibiotic_qad_all5);*/

%mend calc_qad;

%calc_qad;

/* Remove cases where 1st QAD is outside of +/- days from blood culture date */
data antibiotic_qad_all5b;
    set antibiotic_qad_all5a;
    if ~(bcx_drawn_day - 2 <= Day - QAD + 1 <= bcx_drawn_day + 2)
         then QAD = 0;
    if QAD ^= 0;
run;

/* Remove cases when QAD was reset to 1 in a later day */
proc sql;
    create table QAD_max_seq as 
        select Admission_ID,
               Patient_ID,
               bcx_drawn_day,
               QAD_start,
               max(QAD) as QAD_max,
               max(Day) as Day_max
        from antibiotic_qad_all5b
        group by Admission_ID, Patient_ID, bcx_drawn_day, QAD_start;
quit;

proc sql;
    create table antibiotic_qad_all5c as
    select a.* 
    from antibiotic_qad_all5b a,
         QAD_max_seq b
    where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
          a.bcx_drawn_day = b.bcx_drawn_day and a.QAD_start = b.QAD_start and
          a.QAD - a.Day = b.QAD_max - b.Day_max;
quit;

/*%delete_ds(dslist = antibiotic_qad_all5a);*/

/* Select only cases with at least 1 QAD with IV route in days 1-4 */
proc sql;
    create table qad_iv_min as 
        select Admission_ID,
               Patient_ID,
               bcx_drawn_day,
               QAD_start,
               min(QAD) as QAD_IV_min
        from antibiotic_qad_all5c
        where Route = 'IV'
        group by Admission_ID, Patient_ID, bcx_drawn_day, QAD_start;
quit;

proc sql;
    create table ab_qad_all6_ehr as
             select a.*
                 from antibiotic_qad_all5c a,
                      qad_iv_min b
         where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
               a.bcx_drawn_day = b.bcx_drawn_day and a.QAD_start = b.QAD_start and
               1 <= b.QAD_IV_min <= 4;
quit;

/* Retrieve hospital_id, encounter_type, discharge_date, discharge_dispo, */
/* from basic dataset                                                     */
proc sql;
    create table antibiotic_qad_hosp1 (compress=yes) as
        select a.*,
               b.hospital_id,
               b.Encounter_type,
               b.Discharge_date,
               b.Discharge_dispo
        from ab_qad_all6_ehr a
        left join basic_fnl b
           ON a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID;
quit;

/*%delete_ds(dslist = basic:);*/

/* Separate antibiotic results by encounter type */
data antibiotic_qad_ip1 (compress=yes)  antibiotic_qad_ed1 (compress=yes);
    set antibiotic_qad_hosp1;
    if day > discharge_date then delete;
    if Encounter_type = 'I' then output antibiotic_qad_ip1; /*Inpatient*/
    else if Encounter_type = 'E' then output antibiotic_qad_ed1;  /*E=Emergency Department*/
run;

/*%delete_ds(dslist = antibiotic_qad_hosp1);*/

%macro qad_gelt4(type);

    /* Create datasets for encounters with at least 1 QAD >= 4 days        */
    /* and for those with no QAD >= 4 days, for a specified encounter type */

    /* Select encounters with QAD >= 4 days */
    proc sql;
        create table antibiotic_qad_&type.1_ge4  (compress=yes)  as
            select a.*
            from antibiotic_qad_&type.1 a,
            (select distinct Admission_ID,
                             Patient_ID,
                             bcx_drawn_day,
                             QAD_start
             from antibiotic_qad_&type.1
             where QAD >= 4
             ) b
             where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
                   a.bcx_drawn_day = b.bcx_drawn_day and a.QAD_start = b.QAD_start;
    quit;

    /* Select ED encounters with max QAD < 4 days */
    proc sql;
        create table antibiotic_qad_&type.1_lt4  (compress=yes)  as
            select a.*
                from antibiotic_qad_&type.1 a,
                (select distinct Admission_ID,
                                 Patient_ID,
                                 bcx_drawn_day,
                                 QAD_start,
                                 max(QAD) as qad_max
                 from antibiotic_qad_&type.1
                 group by Admission_ID, Patient_ID, bcx_drawn_day, QAD_start
                 having qad_max < 4
                 ) b
            where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
                  a.bcx_drawn_day = b.bcx_drawn_day and a.QAD_start = b.QAD_start;
    quit;

%mend qad_gelt4;

%qad_gelt4(type = IP);
%qad_gelt4(type = ED);

%macro qadenc_ge4(type);

    /* Select all data for a Admission_ID, Patient_ID, bcx_drawn_day  */
    /* where at least 1 QAD >= 4 and determine 1st QAD                */

    /* Select encounters with QAD >= 4 days */
    /* Modified 06/17/2016 */
    /* replace antibiotic_qad_IP1 with antibiotic_qad_&type.1 */ 
    proc sql;
        create table antibiotic_qad_&type.1_ge4  (compress=yes)  as
            select a.*
            from antibiotic_qad_&type.1 a,
            (select distinct Admission_ID,
                             Patient_ID,
                             bcx_drawn_day,
                             QAD_start
             from antibiotic_qad_&type.1
             where QAD >= 4
             ) b
             where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
                   a.bcx_drawn_day = b.bcx_drawn_day and a.QAD_start = b.QAD_start;
    quit;

    /* Find first QAD for sequence of antibiotics with QAD >= 4 */
    proc sql;
        create table antibiotic_qad_&type.2_ge4  (compress=yes)  as
            select Admission_ID,
                   Patient_ID,
                   bcx_drawn_day,
                   QAD_start,
                   min(Day - QAD + 1) as QAD_first
            from antibiotic_qad_&type.1_ge4
            where QAD >= 4
            group by Admission_ID, Patient_ID, bcx_drawn_day, QAD_start;
    quit;

    /* Retrieve information for 1st QAD and combine with antibiotic, QAD data */
    proc sql;
        create table antibiotic_qad_&type.3_ge4   (compress=yes)  as
            select a.*,
                   b.QAD_first
            from antibiotic_qad_&type.1_ge4 a
            LEFT JOIN antibiotic_qad_&type.2_ge4 b
                ON a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
                   a.bcx_drawn_day = b.bcx_drawn_day and a.QAD_start = b.QAD_start;
    quit;

    proc sort data = antibiotic_qad_&type.3_ge4;
    by Admission_ID Patient_ID bcx_drawn_day QAD_start Day Antibiotic route;
    run;

%mend qadenc_ge4;

%qadenc_ge4(type = IP);
%qadenc_ge4(type = ED);

/*%delete_ds(dslist = antibiotic_qad_IP1 antibiotic_qad_ED1);*/

/* If patient has < 4 QADs, patient qualifies if death, discharge to another hospital, */
/* or outpatients occurs before 4 QADs have elapsed, then patient can qualify for      */
/* suspected infection with < 4 QADs so long as they have consecutive QADs until day   */
/* of, or 1 day prior to, death or discharge                                           */

%macro qadenc_lt4(type, dischval);

    /* Select all data for an Admission_ID, Patient_ID, bcx_drawn_day   */
    /* where patient dies and Day within 1 day of discharge date        */
    proc sql;
        create table qad_&type.1_lt4_dis   (compress=yes)  as
            select a.*
                from antibiotic_qad_&type.1_lt4 a,
                (select distinct Admission_ID,
                                 Patient_ID,
                                 bcx_drawn_day,
                                 QAD_start
                 from antibiotic_qad_&type.1_lt4
                 where Discharge_dispo IN(&dischval) and Day >= Discharge_date - 1
                 ) b
                 where a.Admission_ID = b.Admission_Id and a.Patient_Id = b.Patient_ID and
                       a.bcx_drawn_day = b.bcx_drawn_day and a.QAD_start = b.QAD_start;
    quit;

    /* Find first QAD for sequence of antibiotics with death and Day    */
    /* within 1 day of discharge date                                   */
    proc sql;
        create table qad_&type.2_lt4_dis  (compress=yes)  as
            select Admission_ID,
                   Patient_ID,
                   bcx_drawn_day,
                   QAD_start,
                   min(Day - QAD + 1) as QAD_first
            from qad_&type.1_lt4_dis
            where Day >= Discharge_date - 1
            group by Admission_ID, Patient_ID, bcx_drawn_day, QAD_start;
    quit;

    /* Retrieve information for 1st QAD and combine with antibiotic, QAD data */
    proc sql;
        create table qad_&type.3_lt4_dis  (compress=yes)  as
            select a.*,
                   b.QAD_first
            from qad_&type.1_lt4_dis a
            LEFT JOIN qad_&type.2_lt4_dis b
                ON a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
                   a.bcx_drawn_day = b.bcx_drawn_day and a.QAD_start = b.QAD_start;
    quit;

/*    %delete_ds(dslist = qad_&type.1_lt4_dis qad_&type.2_lt4_dis);*/

%mend qadenc_lt4;

%qadenc_lt4(type = IP, dischval = %str(2, 4, 5));
%qadenc_lt4(type = ED, dischval = %str(2, 4, 5));

data qad_infection_all1  (compress=yes);  
    set antibiotic_qad_ip3_ge4 qad_ip3_lt4_dis
        antibiotic_qad_ed3_ge4 qad_ed3_lt4_dis;
run;

/*%delete_ds(dslist = antibiotic_qad_ip3_ge4 qad_ip3_lt4_dis*/
/*        antibiotic_qad_ed3_ge4 qad_ed3_lt4_dis);*/

proc sql;
    create table raw.ehr_infection_V2 (compress=yes) as 
        select  Admission_ID,
                Patient_ID,
                bcx_drawn_day,
                min(QAD_first) as QAD_first,
                1 as infection
        from qad_infection_all1
        group by Admission_ID, Patient_ID, bcx_drawn_day;
quit; 


/*%delete_ds(dslist = qad_infection_all1);*/

/************************************************************************************************/

proc sql;
    create table bloodculture as 
        select distinct a.Admission_Id,
                        a.Patient_ID,
                        a.bcx_drawn_day,
                        b.Discharge_Date
        from raw.bloodculture a,
             basic_fnl b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID;
quit;

/**********************************************************************/
/* Initiation of a new vasopressor within +/- 2 days of Blood Culture */
/**********************************************************************/

/* Retrieve vasopressor data from pharmacy data */
proc sql;
    create table vasopressor1 as 
        select distinct a.Admission_ID, a.Patient_ID,
                        upcase(a.Med) as Med,
                        a.Day
        from raw.medication a,
             basic_fnl b
        where upcase(a.med) IN("NOREPINEPHRINE", "DOPAMINE", "EPINEPHRINE", "PHENYLEPHRINE",
                               "VASOPRESSIN") and
               a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              a.Day <= b.Discharge_date
        order by Admission_ID, Patient_ID, med, Day;
quit;

/* Determine if vasopressor is new */
/* 5/27/2016, separate vasopressor_new = 1 into later data step */
data vasopressor2(drop = diff);
    set vasopressor1;
    by Admission_ID  Patient_ID med;
    diff = ifn(first.med = 0, day - lag(Day), .);
    if diff ^= 1 then vasopressor_new = 1;
    else vasopressor_new = 0;

run;

/* 5/27/2016: Add step for selecting new vasopressor */
data vasopressor3;
    set vasopressor2;
    if vasopressor_new = 1;
run;

/* Select cases with new vapopressor in +/- 2 day window for blood culture */
proc sql;
    create table vasopressor4 as 
        select distinct a.*,
                        b.bcx_drawn_day
        from vasopressor3 a,
             bloodculture b
        where  a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              -2 <= a.Day - b.bcx_drawn_day <= 2;
quit;

proc sql;
    create table vasopressor_fnl as 
        select  Admission_ID, Patient_ID, 
               bcx_drawn_day,
               min(Day) as vasopressor_min_day,
               max(vasopressor_new) as vasopressor_new
        from vasopressor4
        group by Admission_ID, Patient_ID, bcx_drawn_day;
quit;

/* Find cases with 2 consecutive vasopressor days or death, discharge to hospice */
/* 1 day following vasopressor                                                   */
/* First of 2 consecutive vasopressors must be within +/- 2 days of BC           */

proc sql;
    create table basic_bc as 
       select distinct a.Admission_ID,
                       a.Patient_ID,
                       b.bcx_drawn_day
       from basic_fnl a,
            bloodculture b
       where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID ;
quit;

/* Since last day of consecutive vasopressors, compare to -1 to 3 day window from */
/* blood culture                                                                  */

proc sql;
    create table vasopressor2c as 
        select distinct a.*,
                        b.bcx_drawn_day
        from vasopressor1 a,
             basic_bc b
            where  a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
                  -2 <= a.Day - b.bcx_drawn_day <= 3
        order by Admission_ID, Patient_ID, bcx_drawn_day, Med, Day;
quit;

/* Add data step 5/27/16 */
proc sql;
    create table vasopressor2d as
        select a.*,
               b.vasopressor_new
        from vasopressor2c a
        LEFT JOIN vasopressor2 b
          ON a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
             a.Med = b.Med and a.Day = b.Day;
quit;

proc sort data = vasopressor2d;                                                                                                                                     
by Admission_ID Patient_ID bcx_drawn_day Med;                                                                                                                        
run;  

/* Revise data step 5/27/16, use new vasopressors */
data vasopressor2e;                                                                                                     
    set vasopressor2d;                                                                                                                  
    by Admission_ID Patient_ID bcx_drawn_day Med;                                                                                       
    diff = ifn(first.Med = 0, Day - lag(Day), .);                                                                                       
    if diff = 1 and lag(vasopressor_new) = 1 then vaso_cons = 1;                                                                                                     
    else vaso_cons = 0;                                                                                                                 
                                                                                                                                        
    if vaso_cons = 1;                                                                                                                   
run;       

/* Find minimum day of consecutive vasopressors       */
/* Added: 5/26/2016                                   */
/* Use day-1, since last of consecutive days was used */
proc sql;
    create table vasopressor2f as 
        select Admission_ID,
               Patient_ID,
               bcx_drawn_day,
               min(day-1) as vaso_cons_min_day
        from vasopressor2e
        group by Admission_ID, Patient_ID, bcx_drawn_day;
quit;

/* Select different vasopressors occur in consecutive days, and both are new */
/* Use window from -2 to +3 days from blood culture since last of two days   */
/* is chosen                                                                 */
proc sql;
    create table vasopressor4d as 
        select distinct a.*,
                        b.bcx_drawn_day
        from vasopressor3 a,
             bloodculture b
        where  a.Admission_ID = b.Admission_ID and  a.Patient_ID = b.Patient_ID and
              -2 <= a.Day - b.bcx_drawn_day <= 3;
quit;

proc sort data = vasopressor4d; by Admission_ID  Patient_ID bcx_drawn_day Day; run;

/* Revise 5/27/2016, add lag(vasopressor_new) for vaso_cons */
data vasopressor4e(drop = diff);
    set vasopressor4d;
    by Admission_ID Patient_ID bcx_drawn_day Day;
    diff = ifn(first.Patient_ID = 0, Day - lag(Day), .);
    if diff = 1 and lag(vasopressor_new) = 1 then vaso_cons = 1;
    else vaso_cons = 0;

    if vaso_cons = 1;
run;

/* Find minimum day of consecutive different vasopressors */
/* Added 5/26/2016                                        */
/* Use day-1, since last of consecutive days was used     */
proc sql;
    create table vasopressor4f as 
        select Admission_ID, Patient_ID,
               bcx_drawn_day,
               min(day-1) as vaso_cons_min_day
        from vasopressor4e
        group by  Admission_ID, Patient_ID, bcx_drawn_day;
quit;

/* Find minimum day of consecutive different vasopressors */


/* Retrieve basic info */
/* 5/27/2016: Add med to table */
proc sql;
    create table basic_vaso as 
        select distinct a.Admission_ID,
                        a.Patient_ID,
                        a.bcx_drawn_day,
                        a.Med,
                        a.Day,
                        b.discharge_date,
                        b.Discharge_dispo,
                        b.Encounter_type
        from vasopressor2c a
        LEFT JOIN basic_fnl b
            ON a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID;
quit;

/* Check if vasopressor occured on day of or day prior to death or discharge to hospice */
/* 5/26/2016, add code of 2 for discharge_dispo                                         */
/* 5/27/2016: add med                                                                   */
proc sql;
    create table vasopressor_death as 
        select a.Admission_ID,
               a.Patient_ID,
               a.bcx_drawn_day,
               a.Med,
               a.Day,
               a.Discharge_date,
               a.Discharge_dispo,
               1 as vaso_cons
        from basic_vaso a
        where -2 <= a.bcx_drawn_day - a.Day <= 2 and
              a.Day >= a.discharge_date - 1 and
              a.Encounter_type IN('I','E') and a.Discharge_dispo IN(2, 4,5);
quit;

/* 5/27/2016: Add condition for new vasopressor */
proc sql;
    create table vasopressor_death2 as
       select a.*
       from vasopressor_death a,
            vasopressor3 b
       where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
             a.Med = b.Med and a.Day = b.Day;
quit;

proc sql;
    create table vasopressor_death_fnl as 
        select Admission_ID,
               Patient_ID,
               bcx_drawn_day,
               min(day) as vaso_cons_min_day
        from vasopressor_death2
        group by Admission_ID, Patient_ID, bcx_drawn_day;
quit;


/* Find minimum day of vasopressor occuring on day of or day prior to death */

/* combine Vasopressors on consecutive days or those occuring on day of day prior to */
/* death/hospice discharge                                                           */
/* Revised: 5/26/2016, added vaso_cons_min_day variable                              */
proc sql;
    create table vaso_cons as 
       select Admission_ID, Patient_ID,
	      bcx_drawn_day,
              min(vaso_cons_min_day) as vaso_cons_min_day ,
              1 as vaso_cons
       from
       (select Admission_ID, Patient_ID, bcx_drawn_day, vaso_cons_min_day from vasopressor2f
           UNION CORR
       select Admission_ID, Patient_ID, bcx_drawn_day, vaso_cons_min_day from vasopressor4f
           UNION CORR
       select Admission_ID, Patient_ID, bcx_drawn_day, vaso_cons_min_day from vasopressor_death_fnl)
       group by Admission_ID,Patient_ID, bcx_drawn_day;
quit;

proc sort data = vaso_cons; by Admission_ID Patient_ID bcx_drawn_day; run;



*******************************************************************************
* Initiation of invasive mechanical ventilation                               *
*******************************************************************************;

** Mechanical ventilation procedure codes and CPT codes;
proc sql;
    create table mechvent_proc1 as  
        select distinct a.Admission_ID,
                        a.Patient_ID,
                        a.Day,
                        1 as mechvent
        from raw.mechvent a,
             basic_fnl b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              a.Day <= b.Discharge_date
        order by Admission_ID, Patient_ID, day;
quit;

** Select cases without mechanical ventilation in previous day;
data mechvent_proc3(drop = diff);  
    set mechvent_proc1;
    by Admission_ID Patient_ID;
    diff = ifn(first.Patient_ID = 0, day - lag(day), .);
    if diff ^= 1;
run;

proc sql;
    create table mechvent_proc4 as 
        select distinct a.Admission_ID,
                        a.Patient_ID,
                        a.day,
                        a.Mechvent,
                        b.bcx_drawn_day
        from mechvent_proc3 a,
             bloodculture b
        where a.Admission_Id = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              -2 <= a.day - b.bcx_drawn_day <= 2;
quit;

proc sql;
    create table mechvent_fnl as 
        select distinct a.Admission_ID,
                        a.Patient_ID,
                        a.bcx_drawn_day,
                        min(day) as mechvent_min_day,
                        max(mechvent) as mechvent
        from mechvent_proc4 a
        group by Admission_ID, Patient_ID, bcx_drawn_day;
quit;

/******************************************************************************/
/* Serum Lactate >= 2.0 mg/dl                                                 */
/******************************************************************************/
proc sql;
    create table serum_lactate1 as
        select distinct a.Admission_ID,
                        a.Patient_ID,
                        a.Day
        from raw.laboratory a,
             basic_fnl b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              2.0 <= a.lactate_max and a.Day <= b.Discharge_date;
quit;

** Select cases with lactate >= 2 in +/- 2 days of blood culture;
proc sql;
    create table serum_lactate2 as 
        select a.*,
               b.bcx_drawn_day
        from serum_lactate1  a,
             bloodculture b
        where a.Admission_ID = b.Admission_ID and  a.Patient_ID = b.Patient_ID and
              -2 <= a.Day - b.bcx_drawn_day <= 2;
quit;

proc sql;
    create table lactate_fnl as 
        select distinct Admission_ID,
                        Patient_ID,
                        bcx_drawn_day,
                        min(Day) as lactate_min_day,
                        1 as lactate
        from serum_lactate2
        group by Admission_ID, Patient_ID, bcx_drawn_day;
quit;

************************************************************************************
* Calculate baseline values for Creatinine, eGFR, Total Bilirubin, and Platelets   *
************************************************************************************;

proc sql;
    create table baseline_comm_lab1 as 
        select a.Admission_ID,
               a.Patient_ID,
               a.bcx_drawn_day,
               a.Discharge_date,
               b.Day,
               b.Cr_min,
               b.Tbili_min,
           /*    CASE WHEN b.Egfr_max > 60 THEN 60
                    ELSE b.Egfr_max
               END as Egfr_max,*/
               b.Plt_max
        from bloodculture a,
             raw.laboratory b
        where a.Admission_ID = b.Admission_ID and a.Patient_Id = b.Patient_ID and
              -1 <= b.Day <= a.Discharge_Date + 2;
quit;

** Community baseline levels;
proc sql;
    create table baseline_comm_lab2 as  
        select Admission_ID,
               Patient_ID,
               bcx_drawn_day,
               min(Cr_min) as Cr_baseline,
               min(Tbili_min) as Tbili_baseline,
               /*max(Egfr_max) as Egfr_baseline,*/
               max(Plt_max) as Plt_baseline
        from baseline_comm_lab1
        group by Admission_ID, Patient_ID, bcx_drawn_day;
quit;

** Combine basic data with blood culture data;
proc sql;
    create table basic_bc as 
       select distinct a.Admission_ID,
                       a.Patient_ID,
                       b.bcx_drawn_day
       from basic_fnl a,
            bloodculture b
       where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID;
quit;

** Hospital baseline levels;
proc sql;
    create table baseline_hosp_lab1 as 
        select a.Admission_ID,
               a.Patient_ID,
               a.bcx_drawn_day,
               b.Day,
               b.Cr_min,
               b.Tbili_min,
              /* CASE WHEN b.Egfr_max > 60 THEN 60
                    ELSE b.Egfr_max
               END as Egfr_max,*/
               b.Plt_max
        from basic_bc a,
             raw.laboratory b
        where  a.Admission_ID = b.Admission_ID and a.Patient_Id = b.Patient_ID and
              -2 <= b.Day - a.bcx_drawn_day <= 2;
quit;

proc sql;
    create table baseline_hosp_lab2 as 
        select Admission_ID,
               Patient_ID,
               bcx_drawn_day,
               min(Cr_min) as Cr_baseline,
               min(Tbili_min) as Tbili_baseline,
               /*max(Egfr_max) as Egfr_baseline,*/
               max(Plt_max) as Plt_baseline
        from baseline_hosp_lab1
        group by Admission_ID, Patient_ID, bcx_drawn_day;
quit;

***********************************************************************************
* Retrieve lab data within +/- 2 days of blood culture                            *
***********************************************************************************;

* Cases with lab data within +/- 2 days of BC window;
proc sql;
    create table bc_lab1 as 
        select distinct b.Admission_ID,
                        b.Patient_ID,
                        a.bcx_drawn_day,
                        b.Day,
                        b.Cr_max,
                        b.Tbili_max,
                      /*  b.egfr_min, */
                        b.Plt_min
        from bloodculture a,
             raw.laboratory b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              -2 <= b.Day - a.bcx_drawn_day <= 2;
quit;


***********************************************************************************
* Select cases with doubling of creatinine or decrease by >= 50% of eGFR relative *
* to baseline(community)                                                          *
***********************************************************************************;

** Renal disease cases;
proc sql;
    create table renal_disease as 
        select distinct Admission_ID,
                        Patient_ID
        from raw.diagnosis
        where Diagnosiscode9= "5856" or Diagnosiscode10='N186';
quit;


%macro mk_cregfr(type);

    * Cases with creatinine >= 2 * baseline value;
    * For hospital based infections, additional join on bcx_drawn_date is needed;
    proc sql;
        create table &type._cr1 (compress=yes) as
            select a.Admission_ID,
                   a.Patient_ID,
                   a.bcx_drawn_day,
                   a.Day,
                   a.Cr_max,
                   b.Cr_baseline
            from bc_lab1 a,
                baseline_&type._lab2 b
            where  a.Patient_ID = b.Patient_ID and
                  a.bcx_drawn_day = b.bcx_drawn_day
                  and

               (
                  (a.Cr_max >= 2 * b.Cr_baseline and a.Cr_max > 0 and
                   b.Cr_baseline > .)
                );
    quit;

    proc sql;
        create table &type._cr2 (compress=yes) as
            select Admission_ID,
                   Patient_ID,
                   bcx_drawn_day,
                   min(Day) as Cr_&type._min_day /*format mmddyy10.*/,
                   1 as Cr_&type.
            from &type._cr1
            group by Admission_ID, Patient_ID, bcx_drawn_day;
    quit;

    proc sql;
        create table &type._egfr1 (compress=yes) as
            select a.Admission_ID,
                   a.Patient_ID,
                   a.bcx_drawn_day,
                   a.Day
                  /* a.egfr_min,
                   b.egfr_baseline */
            from bc_lab1 a,
                baseline_&type._lab2 b
            where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
                  a.bcx_drawn_day = b.bcx_drawn_day
                 /*   and

             (
                  (b.egfr_baseline >= 2 * a.egfr_min and b.egfr_baseline > 0 and
                   a.egfr_min > .)
                )*/;
    quit;

    proc sql;
        create table &type._egfr2 (compress=yes) as
            select Admission_ID,
                   Patient_ID,
                   bcx_drawn_day
                   /*min(Day) as egfr_&type._min_day,
                   1 as egfr_&type.*/
            from &type._egfr1
            group by Admission_ID, Patient_ID, bcx_drawn_day;
    quit;

    ** Exclude cases with renal_disease;
    proc sql;
        create table egfr_&type._keep (compress=yes) as
            select Admission_ID, Patient_ID from &type._egfr2
                EXCEPT CORR
            select Admission_ID, Patient_ID from renal_disease;
    quit;

    proc sql;
        create table cr_&type._keep (compress=yes) as
            select Admission_ID, Patient_ID from &type._cr2
                EXCEPT CORR
            select Admission_ID, Patient_ID from renal_disease;
    quit;

    proc sql;
        create table &type._cr_fnl (compress=yes) as
            select a.*
            from &type._cr2 a,
                 cr_&type._keep b
            where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID;
    quit;

    proc sql;
        create table &type._egfr_fnl (compress=yes) as
            select a.*
            from &type._egfr2 a,
                 egfr_&type._keep b
            where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID;
    quit;

%mend mk_cregfr;

%mk_cregfr(type = COMM);
%mk_cregfr(type = HOSP);

***********************************************************************************
* Select cases with Total bilirubin >= 2.0 mg/dL and increase by 100% from        *
* baseline(community)                                                             *
***********************************************************************************;

%macro mk_tbili(type);

    * Cases with Total bilirum >= 2.0 and Total bilirum >= 2 * baseline value;
    * For hospital based infections, additional join on bcx_drawn_date is needed;
    * For hospital based infections, additional join on bcx_drawn_date is needed;

    proc sql;
        create table &type._tbili1 (compress=yes) as
            select a.Admission_ID,
                   a.Patient_ID,
                   a.bcx_drawn_day,
                   a.Day,
                   a.Tbili_max,
                   b.Tbili_baseline
             from bc_lab1 a,
                 baseline_&type._lab2 b
             where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
                   a.bcx_drawn_day = b.bcx_drawn_day and
                   a.Tbili_max >= 2.0 and
                  (a.Tbili_max >= 2 * b.Tbili_baseline and a.Tbili_max > 0 and
                   b.Tbili_baseline > .
                   );
    quit;

    proc sql;
        create table &type._tbili_fnl (compress=yes) as
            select distinct Admission_ID,
                            Patient_ID,
                            bcx_drawn_day,
                            min(Day) as tbili_&type._min_day /*format mmddyy10.*/,
                            1 as tbili_&type.
            from &type._tbili1
            group by Admission_ID, Patient_ID, bcx_drawn_day;
    quit;

%mend mk_tbili;

%mk_tbili(type = COMM);
%mk_tbili(type = HOSP);

***********************************************************************************
* Select cases with Platelet count < 100 cells/ul and decline by 50% from         *
* baseline(community)                                                             *
***********************************************************************************;

%macro mk_platelet(type);

    proc sql;
        create table &type._platelet1 (compress=yes) as
            select a.Admission_ID,
                   a.Patient_ID,
                   a.bcx_drawn_day,
                   a.Day,
                   a.Plt_min,
                   b.Plt_baseline
             from bc_lab1 a,
                 baseline_&type._lab2 b
             where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
                   a.bcx_drawn_day = b.bcx_drawn_day and
                   . < a.Plt_min < 100 and b.Plt_baseline >= 100 and
                  (a.Plt_min <= 0.5 * b.Plt_baseline and
                   a.Plt_min > .
                   );
    quit;

    proc sql;
        create table &type._platelet_fnl (compress=yes) as
            select Admission_ID,
                   Patient_ID,
                   bcx_drawn_day,
                   min(Day) as Plt_&type._min_day /*format mmddyy10.*/,
                   1 as plt_&type.
            from &type._platelet1
            group by Admission_Id, Patient_ID, bcx_drawn_day;
    quit;

%mend mk_platelet;

%mk_platelet(type = COMM);
%mk_platelet(type = HOSP);

*************************************************************************************
* Combine organ dysfunction criteria with infection data                            *
*************************************************************************************;

proc sort data = raw.ehr_infection_V2 out = infection;
by Admission_ID Patient_ID bcx_drawn_day;
run;

*comm_egfr_fnl hosp_egfr_fnl   egfr_comm egfr_hosp egfr_comm_min_day egfr_hosp_min_day egfr_comm egfr_hosp;
data raw.ehr_organ_dys_v2 (drop = i); 
    merge vasopressor_fnl work.MECHVENT_FNL lactate_fnl comm_cr_fnl hosp_cr_fnl
           comm_tbili_fnl hosp_tbili_fnl
          comm_platelet_fnl hosp_platelet_fnl vaso_cons
          infection(keep =Admission_ID Patient_ID bcx_drawn_day QAD_first);
    by Admission_ID  Patient_ID bcx_drawn_day;
    array orgvar vasopressor_new vaso_cons mechvent lactate cr_comm cr_hosp
               
                 tbili_comm tbili_hosp plt_comm plt_hosp;
    do over orgvar;
        if orgvar = . then orgvar = 0;
    end;

    /* Fix lab variables, comm variables require minimum day IN(1,2), */
    /* hosp variables require minimum day >= 3                        */
    /* 5/26/2016: set min_day variables to missing if criteria not met */
    array mincomm [3] cr_comm_min_day  tbili_comm_min_day plt_comm_min_day;
    array minhosp [3] cr_hosp_min_day  tbili_hosp_min_day plt_hosp_min_day;
    array commvar [3] cr_comm  tbili_comm plt_comm;
    array hospvar [3] cr_hosp  tbili_hosp plt_hosp;
    do i = 1 to 3;
        if ~(. < bcx_drawn_day < 3 or . < QAD_first < 3) then do;
            commvar[i] = 0;
            mincomm[i] = .;
        end;
    end;

    do i = 1 to 3;
        if ~(bcx_drawn_day >= 3 or QAD_first >= 3) then do;
           hospvar[i] = 0;
           minhosp[i] = .;
        end;
    end;
run;

/**********************************************************************************************/


proc sql;
    create table basic_bc1 (compress=yes) as 
        select distinct a.Admission_ID,
                       a.Patient_ID,
                        a.bcx_drawn_day,
                        b.Discharge_Date
        from raw.bloodculture a,
             basic_fnl b
        where a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID;
quit;


proc sql;
    create table basic_bc2 (compress=yes) as 
        select  a.Admission_ID, a.Patient_ID,
               a.bcx_drawn_day,
               coalesce(b.Infection, 0) as Infection,
               b.QAD_first,
               coalesce(c.vasopressor_new, 0) as vasopressor_new,
               c.vasopressor_min_day,
               coalesce(c.mechvent, 0) as mechvent,
               c.mechvent_min_day,
               coalesce(c.lactate, 0) as lactate,
               c.lactate_min_day,
               coalesce(c.cr_comm, 0) as cr_comm,
               c.cr_comm_min_day,
              /* coalesce(c.egfr_comm, 0) as egfr_comm,
               c.egfr_comm_min_day, */
               coalesce(c.cr_hosp, 0) as cr_hosp,
               c.cr_hosp_min_day,
              /* coalesce(c.egfr_hosp, 0) as egfr_hosp,
               c.egfr_hosp_min_day, */
               coalesce(c.tbili_comm, 0) as tbili_comm,
               c.tbili_comm_min_day,
               coalesce(c.tbili_hosp, 0) as tbili_hosp,
               c.tbili_hosp_min_day,
               coalesce(c.plt_comm, 0) as plt_comm,
               c.plt_comm_min_day,
               coalesce(c.plt_hosp, 0) as plt_hosp,
               c.plt_hosp_min_day,
               coalesce(c.vaso_cons) as vaso_cons,
               c.vaso_cons_min_day
   from basic_bc1 a
        LEFT JOIN raw.ehr_infection_V2 b
           ON a.Admission_ID = b.Admission_ID and a.Patient_ID = b.Patient_ID and
              a.bcx_drawn_day = b.bcx_drawn_day
        LEFT JOIN raw.ehr_organ_dys_V2 c
           ON a.Admission_ID = c.Admission_ID and a.Patient_ID = c.Patient_ID and
              a.bcx_drawn_day = c.bcx_drawn_day;
quit;


/* Modified 07/05/16: define community sepsis before hospital sepsis definitions      */
/* Modified 07/05/16: use bcx_drawn_day < 3 instead of bcx_drawn_day IN(-2,-1,0,1,2)  */
/*                    for community definitions                                       */
/* Hospital-Onset Infection: (BC + 4 QADs) AND BC Day and First QAD occur on or after */
/*                           day3 of admission                                        */

data raw.ehr_sepsis_def_V2 (keep = Admission_ID Patient_ID bcx_drawn_day hosp_infection comm_infection
                                  hosp_sepsis comm_sepsis hosp_strict1_sepsis comm_strict1_sepsis
                                  hosp_strict2_sepsis comm_strict2_sepsis hosp_sepsis_nolactate
                                  comm_sepsis_nolactate hosp_sepsis_nopreslac comm_sepsis_nopreslac
                                  hosp_strict1_sepsis_nolac comm_strict1_sepsis_nolac
                                  hosp_strict2_sepsis_nolac comm_strict2_sepsis_nolac
                                  hosp_shock1_sepsis comm_shock1_sepsis hosp_shock2_sepsis
                                  comm_shock2_sepsis hosp_shock3_sepsis comm_shock3_sepsis
                                  hosp_shock4_sepsis comm_shock4_sepsis hosp_simple1_sepsis
                                  comm_simple1_sepsis hospcomm_infection hospcomm_sepsis
                                  hospcomm_strict1_sepsis hospcomm_strict2_sepsis
                                  hospcomm_sepsis_nolactate hospcomm_sepsis_nopreslac
                                  hospcomm_strict1_sepsis_nolac hospcomm_strict2_sepsis_nolac
                                  hospcomm_shock1_sepsis hospcomm_shock2_sepsis
                                  hospcomm_shock3_sepsis hospcomm_shock4_sepsis
                                  hospcomm_simple1_sepsis infection);
    set basic_bc2;
*WW;
*max(egfr_comm, egfr_hosp),;
*cr_egfr_hosp_min_day = min(cr_hosp_min_day, egfr_hosp_min_day);
*cr_egfr_comm_min_day = min(cr_comm_min_day, egfr_comm_min_day);
    num_organ_dys = sum(vasopressor_new, mechvent, lactate, max(cr_comm, cr_hosp),
                         max(tbili_comm, tbili_hosp),
                        max(plt_comm, plt_hosp));

    cr_egfr_hosp_min_day = cr_hosp_min_day;
    cr_egfr_comm_min_day = cr_comm_min_day;

    /* Hospital infection */
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 then hosp_infection = 1;
    else hosp_infection = 0;

    /* Community infection */
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) then comm_infection = 1;
    else comm_infection = 0;

    /* Community sepsis */
    if infection = 1 and num_organ_dys >= 1 and 
      (bcx_drawn_day < 3 or QAD_first < 3) and
       (
           (vasopressor_new = 1 and vasopressor_min_day < 3) or
           (mechvent = 1 and mechvent_min_day < 3) or 
           (lactate = 1 and lactate_min_day < 3) or
           (cr_comm = 1 and cr_comm_min_day < 3) or
           (cr_hosp = 1 and cr_hosp_min_day < 3) or
           /*(egfr_comm = 1 and egfr_comm_min_day < 3) or
           (egfr_hosp = 1 and egfr_hosp_min_day < 3) or */
           (tbili_comm = 1 and tbili_comm_min_day < 3) or
           (tbili_hosp = 1 and tbili_hosp_min_day < 3) or
           (plt_comm = 1 and plt_comm_min_day < 3) or
           (plt_hosp = 1 and plt_hosp_min_day < 3)
       )  
       then comm_sepsis = 1;
    else comm_sepsis = 0;


    /* Hospital sepsis */
    if infection = 1 and num_organ_dys >= 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and
       ( 
          (vasopressor_new = 1) or 
          (mechvent = 1) or 
          (lactate = 1) or
          (max(cr_comm,cr_hosp) = 1) or
         /* (max(egfr_comm,egfr_hosp) = 1) or */
          (max(tbili_comm,tbili_hosp) = 1) or 
          (max(plt_comm,plt_hosp) = 1) 
        ) then hosp_sepsis = 1;
    else if infection = 1 and num_organ_dys >= 1 and (bcx_drawn_day < 3 or QAD_first < 3) and
         comm_sepsis = 0 and 
       ( 
          (vasopressor_new = 1) or 
          (mechvent = 1) or 
          (lactate = 1) or
          (max(cr_comm, cr_hosp) = 1) or
          /*(max(egfr_comm, egfr_hosp) = 1) or */
          (max(tbili_comm, tbili_hosp) = 1) or 
          (max(plt_comm, plt_hosp) = 1) 
        ) then hosp_sepsis = 1;
    else hosp_sepsis = 0;

    /* Community sepsis, strict #1 */
*WW;
* max(cr_comm, egfr_comm) = 1 and min(cr_comm_min_day, egfr_comm_min_day) < 3,;
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and 
          sum(vasopressor_new = 1,
              mechvent = 1 and mechvent_min_day < 3,
              lactate = 1 and lactate_min_day < 3,
              cr_comm = 1 and cr_comm_min_day < 3,
              tbili_comm = 1 and tbili_comm_min_day < 3,
              plt_comm = 1 and plt_comm_min_day < 3
              ) >= 2 
          then comm_strict1_sepsis = 1;
    else comm_strict1_sepsis = 0;

    /* Hospital sepsis, strict #1 */
*WW;
*max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1, ;
*max(cr_comm, egfr_comm) = 1 and min(cr_comm_min_day, egfr_comm_min_day) < 3,;
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and 
          sum(vasopressor_new = 1, 
              mechvent = 1, 
              lactate = 1,
              max(cr_comm, cr_hosp) = 1, 
              max(tbili_comm, tbili_hosp) = 1, 
              max(plt_comm, plt_hosp) = 1
              ) >= 2
          then hosp_strict1_sepsis = 1;
    else if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and comm_strict1_sepsis = 0 and
          sum(vasopressor_new = 1, 
              mechvent = 1, 
              lactate = 1,
              max(cr_comm, cr_hosp) = 1, 
              max(tbili_comm, tbili_hosp = 1), 
              max(plt_comm, plt_hosp) = 1
              ) >= 2
          then hosp_strict1_sepsis = 1;
    else hosp_strict1_sepsis = 0;

    /* Community sepsis, strict #2 */
*WW;
*max(cr_comm, egfr_comm) = 1 and min(cr_comm_min_day, egfr_comm_min_day) < 3,;
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and
         (
          sum(mechvent = 1 and mechvent_min_day < 3, 
              lactate = 1 and lactate_min_day < 3, 
              cr_comm = 1 and cr_comm_min_day < 3,
              tbili_comm = 1 and tbili_comm_min_day < 3, 
              plt_comm = 1 and plt_comm_min_day < 3
              ) >= 2
           or
           (vasopressor_new = 1 and vasopressor_min_day < 3)
          )
          then comm_strict2_sepsis = 1;
    else comm_strict2_sepsis = 0;

    /* Hospital sepsis, strict #2 */
*WW;
*max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1,;
*max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1,;
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and
          (
           sum(mechvent = 1, 
               lactate = 1, 
               max(cr_comm, cr_hosp) = 1,
               max(tbili_comm, tbili_hosp) = 1, 
               max(plt_comm, plt_hosp) = 1
               ) >= 2 or
           (vasopressor_new = 1)
          )
          then hosp_strict2_sepsis = 1;
    else if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and comm_strict2_sepsis = 0 and
          (
           sum(mechvent = 1, 
               lactate = 1, 
               max(cr_comm, cr_hosp) = 1,
               max(tbili_comm, tbili_hosp) = 1, 
               max(plt_comm, plt_hosp) = 1
               ) >= 2 or
           (vasopressor_new = 1)
          )
          then hosp_strict2_sepsis = 1;
    else hosp_strict2_sepsis = 0;

    /* Community sepsis without lactate */
*WW;
*(max(cr_comm, egfr_comm) = 1 and min(cr_comm_min_day, egfr_comm_min_day) < 3) or;
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and
          (
           (vasopressor_new = 1 and vasopressor_min_day < 3) or 
           (mechvent = 1 and mechvent_min_day < 3) or 
           (cr_comm = 1 and cr_comm_min_day < 3) or
           (tbili_comm = 1 and tbili_comm_min_day < 3) or 
           (plt_comm = 1 and plt_comm_min_day < 3)
           )
          then comm_sepsis_nolactate = 1;
    else comm_sepsis_nolactate = 0;


    /* Hospital sepsis without lactate */
*WW;
*(max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1)or;
*(max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1) or;
*(max(cr_comm, egfr_comm) = 1 and min(cr_comm_min_day, egfr_comm_min_day) < 3;
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and
          (
           (vasopressor_new = 1) or
           (mechvent = 1) or 
           (max(cr_comm, cr_hosp) = 1)or
           (max(tbili_comm,tbili_hosp) = 1) or 
           (max(plt_comm, plt_hosp) = 1)
          )
          then hosp_sepsis_nolactate = 1;
    else if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and comm_sepsis_nolactate = 0 and 
          (
           (vasopressor_new = 1) or 
           (mechvent = 1) or 
           (max(cr_comm, cr_hosp) = 1) or
           (max(tbili_comm, tbili_hosp) = 1) or 
           (max(plt_comm, plt_hosp) = 1)
           )
          then hosp_sepsis_nolactate = 1;
    else hosp_sepsis_nolactate = 0;

    /* Community sepsis without pressor or lactate */
*WW FIX OCT 4 2016;
*removed extra set of () in code ;
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and
          (
           (mechvent = 1 and mechvent_min_day < 3) or 
           (cr_comm=1 and cr_comm_min_day < 3) or
           (tbili_comm = 1 and tbili_comm_min_day < 3) or 
           (plt_comm = 1 and plt_comm_min_day < 3)
           ) then comm_sepsis_nopreslac = 1;
    else comm_sepsis_nopreslac = 0;


    /* Hospital sepsis without pressor or lactate */
*WW;
*(max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1) or;
*(max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1 ;
*max(cr_comm, egfr_comm) = 1 and min(cr_comm_min_day, egfr_comm_min_day) < 3,;
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and
          (
           (mechvent = 1) or 
           (max(cr_comm, cr_hosp) = 1) or
           (max(tbili_comm, tbili_hosp) = 1) or 
           (max(plt_comm, plt_hosp) = 1)
           ) then hosp_sepsis_nopreslac = 1;
    else if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and comm_sepsis_nopreslac = 0 and
          (
           (mechvent = 1) or 
           (max(cr_comm, cr_hosp) = 1) or
           max(tbili_comm, tbili_hosp) = 1 or 
           max(plt_comm, plt_hosp) = 1
           ) then hosp_sepsis_nopreslac = 1;
    else hosp_sepsis_nopreslac = 0;

    /* Community sepsis, strict #1 without lactate */
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and 
          sum(vasopressor_new = 1 and vasopressor_min_day < 3, 
              mechvent = 1 and mechvent_min_day < 3, 
              cr_comm = 1 and cr_comm_min_day < 3,
              tbili_comm = 1 and tbili_comm_min_day < 3, 
              plt_comm = 1 and plt_comm_min_day < 3
              ) >= 2
          then comm_strict1_sepsis_nolac = 1;
    else comm_strict1_sepsis_nolac = 0;

    /* Hospital sepsis, strict #1 without lactate */
*WW;
*max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1,;
*max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1,;
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and
          sum(vasopressor_new = 1, 
              mechvent = 1, 
              max(cr_comm, cr_hosp) = 1,
              max(tbili_comm, tbili_hosp) = 1, 
              max(plt_comm, plt_hosp) = 1
              ) >= 2
          then hosp_strict1_sepsis_nolac = 1;
    else if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and comm_strict1_sepsis_nolac = 0 and
          sum(vasopressor_new = 1, 
              mechvent = 1, 
              max(cr_comm, cr_hosp) = 1,
              max(tbili_comm, tbili_hosp) = 1, 
              max(plt_comm, plt_hosp) = 1
              ) >= 2
          then hosp_strict1_sepsis_nolac = 1;
    else hosp_strict1_sepsis_nolac = 0;

    /* Community sepsis, strict #2 without lactate */
*WW;
*max(cr_comm, egfr_comm) = 1 and min(cr_comm_min_day, egfr_comm_min_day) < 3, ;
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and
         (
          sum(mechvent = 1 and mechvent_min_day < 3, 
              cr_comm = 1 and cr_comm_min_day < 3, 
              tbili_comm = 1 and tbili_comm_min_day < 3,
              plt_comm = 1 and plt_comm_min_day < 3
              ) >= 2
           or
           (vasopressor_new = 1)
          )
          then comm_strict2_sepsis_nolac = 1;
    else comm_strict2_sepsis_nolac = 0;

    /* Hospital sepsis, strict #2 without lactate */
*WW;
*max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1, ;
*max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1, ;
*max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1, ;
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and
          (
           sum(mechvent = 1, 
               max(cr_comm, cr_hosp) = 1, 
               max(tbili_comm, tbili_hosp) = 1,
               max(plt_comm, plt_hosp) = 1
               ) >= 2 or
           (vasopressor_new = 1)
          )
          then hosp_strict2_sepsis_nolac = 1;
    else if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and comm_strict2_sepsis_nolac = 0 and
         (
          sum(mechvent = 1, 
              max(cr_comm, cr_hosp) = 1, 
              max(tbili_comm, tbili_hosp) = 1,
              max(plt_comm, plt_hosp) = 1
              ) >= 2
           or
           (vasopressor_new = 1)
          )
          then hosp_strict2_sepsis_nolac = 1;
    else hosp_strict2_sepsis_nolac = 0;

    /* Community sepsis, shock #1 */
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and
       vasopressor_new = 1 and vasopressor_min_day < 3 and lactate = 1 and lactate_min_day < 3
        then comm_shock1_sepsis = 1;
    else comm_shock1_sepsis = 0;

    /* Hospital sepsis, shock #1 */
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and
       vasopressor_new = 1 and lactate = 1 then hosp_shock1_sepsis = 1;
    else if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and comm_shock1_sepsis = 0 and
       vasopressor_new = 1 and lactate = 1 then hosp_shock1_sepsis = 1;
    else hosp_shock1_sepsis = 0;

    /* Community sepsis, shock #2 */
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and
       vasopressor_new = 1 and vasopressor_min_day < 3 then comm_shock2_sepsis = 1;
    else comm_shock2_sepsis = 0;

    /* Hospital sepsis, shock #2 */
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and
       vasopressor_new = 1 then hosp_shock2_sepsis = 1;
    else if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and comm_shock2_sepsis = 0 and
       vasopressor_new = 1 then hosp_shock2_sepsis = 1;
    else hosp_shock2_sepsis = 0;

    /* Community sepsis, shock #3 */
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and
       vaso_cons = 1 and vaso_cons_min_day < 3 then comm_shock3_sepsis = 1;
    else comm_shock3_sepsis = 0;

    /* Hospital sepsis, shock #3 */
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and
       vaso_cons = 1 then hosp_shock3_sepsis = 1;
    else if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and comm_shock3_sepsis = 0 and
       vaso_cons = 1 then hosp_shock3_sepsis = 1;
    else hosp_shock3_sepsis = 0;

    /* Community sepsis, shock #4 */
*WW;
*max(cr_comm, egfr_comm) = 1 and min(cr_comm_min_day, egfr_comm_min_day) < 3,;
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and
       (vasopressor_new = 1 and vasopressor_min_day < 3) and
       sum(mechvent = 1 and mechvent_min_day < 3, 
           lactate = 1 and lactate_min_day < 3, 
           cr_comm = 1 and cr_comm_min_day < 3,
           tbili_comm = 1 and tbili_comm_min_day < 3, 
           plt_comm = 1 and plt_comm_min_day < 3
           ) >= 1 
       then comm_shock4_sepsis = 1;
    else comm_shock4_sepsis = 0;

    /* Hospital sepsis, shock #4 */
*WW;
*max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1,;
*max(cr_comm, egfr_comm, cr_hosp, egfr_hosp) = 1,;
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and
       vasopressor_new = 1 and 
       sum(mechvent = 1,  
           lactate = 1, 
           max(cr_comm, cr_hosp) = 1,
           max(tbili_comm, tbili_hosp) = 1, 
           max(plt_comm, plt_hosp) = 1
           ) >= 1 then hosp_shock4_sepsis = 1;
    else if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and comm_shock4_sepsis = 0 and
       vasopressor_new = 1 and
       sum(mechvent = 1, 
           lactate = 1, 
           max(cr_comm, cr_hosp) = 1,
           max(tbili_comm, tbili_hosp) = 1, 
           max(plt_comm, plt_hosp) = 1) >= 1 
       then hosp_shock4_sepsis = 1;
    else hosp_shock4_sepsis = 0;

    /* Community simple sepsis, shock #1 */
    if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and
       (
           (vasopressor_new = 1 and vasopressor_min_day < 3) or
           (mechvent = 1 and mechvent_min_day < 3) or 
           (cr_comm = 1 and cr_comm_min_day < 3) 
       ) then comm_simple1_sepsis = 1;
    else comm_simple1_sepsis = 0;

    /* Hospital simple sepsis, shock #1 */
    if infection = 1 and bcx_drawn_day >= 3 and QAD_first >= 3 and
       (
         (vasopressor_new = 1) or
         (mechvent = 1) or 
         (max(cr_comm,cr_hosp) = 1)
       )
         then hosp_simple1_sepsis = 1;
    else if infection = 1 and (bcx_drawn_day < 3 or QAD_first < 3) and comm_simple1_sepsis = 0 and
       (
           vasopressor_new = 1 or
           mechvent = 1 or 
           max(cr_comm, cr_hosp) = 1 
       ) then hosp_simple1_sepsis = 1;
    else hosp_simple1_sepsis = 0;

    /* Create hospital/community variables */
    array hospvar (13) hosp_infection hosp_sepsis
                       hosp_strict1_sepsis hosp_strict2_sepsis
                       hosp_sepsis_nolactate hosp_sepsis_nopreslac
                       hosp_strict1_sepsis_nolac hosp_strict2_sepsis_nolac
                       hosp_shock1_sepsis hosp_shock2_sepsis
                       hosp_shock3_sepsis hosp_shock4_sepsis
                       hosp_simple1_sepsis;
     array commvar (13) comm_infection comm_sepsis
                        comm_strict1_sepsis comm_strict2_sepsis
                        comm_sepsis_nolactate comm_sepsis_nopreslac
                        comm_strict1_sepsis_nolac comm_strict2_sepsis_nolac
                        comm_shock1_sepsis comm_shock2_sepsis
                        comm_shock3_sepsis comm_shock4_sepsis
                        comm_simple1_sepsis;
     array hospcomm (13) hospcomm_infection hospcomm_sepsis
                         hospcomm_strict1_sepsis hospcomm_strict2_sepsis
                         hospcomm_sepsis_nolactate hospcomm_sepsis_nopreslac
                         hospcomm_strict1_sepsis_nolac hospcomm_strict2_sepsis_nolac
                         hospcomm_shock1_sepsis hospcomm_shock2_sepsis
                         hospcomm_shock3_sepsis hospcomm_shock4_sepsis
                         hospcomm_simple1_sepsis;

     do i = 1 to 13;
          hospcomm[i] = max(hospvar[i], commvar[i]);
     end;
run;


PROC FREQ DATA= raw.ehr_sepsis_def_V2 order=freq;
TABLE  hosp_sepsis comm_sepsis ;
RUN;

DATA sepsis; 
SET  raw.ehr_sepsis_def_V2;
if hosp_sepsis=1 or comm_sepsis=1;
keep hosp_sepsis  Infection  hosp_infection  comm_infection  comm_sepsis patient_id bcx_drawn_day Admission_ID;
RUN;

PROC SORT DATA=sepsis  nodupkey out=raw.hospcomm_sepsis_undup_v2 ;
BY  patient_id bcx_drawn_day Admission_ID hosp_sepsis comm_sepsis;
RUN;
