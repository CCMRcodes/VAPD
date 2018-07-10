/*This example codes uses all diagnosis codes and group them into Angus definition of Sepsis indicators*/

/* Date Modified: 7/2/2018
   Author: Shirley Wang */

/******* ANGUS DEFINITION OF SEPSIS ***********/
libname final 'INSERT FOLDER PATH';

%let year=20142017;

/*icd-10 infection and organ dysfunction codes are saved in the Excel file: Angus_codes then imported into SAS and saved as a
SAS dataset named: angus_icd9_icd10_diag_codes */

/*vapd.angus_icd9_icd10_diag_codes*/
PROC FREQ DATA=final.angus_icd9_icd10_diag_codes  order=freq;
TABLE ind; /*1867 infection and 44 Dysfunction codes*/
RUN;

DATA  icd10_infection; /*1867*/
SET final.angus_icd9_icd10_diag_codes ;
if ind='Infection';
keep ind icd10;
RUN;

PROC SORT DATA= icd10_infection nodupkey ; /*1135*/
BY  icd10;
RUN;

/*make a macro icd10_infection Codes list*/
proc sql;
	select  catt("'", icd10, "'")  /*1135*/
	INTO :icd10_infection separated by ','
	from icd10_infection;
quit; 
%put &icd10_infection;

/*icd-10 Dysfunction codes*/
DATA  icd10_Dysfunction; /*44*/
SET final.angus_icd9_icd10_diag_codes ;
if ind='Dysfunction';
keep ind icd10;
RUN;

PROC SORT DATA= icd10_Dysfunction nodupkey ; /*43*/
BY  icd10;
RUN;

/*make a macro icd10_Dysfunction Codes list*/
proc sql;
	select  catt("'", icd10, "'")  /*43*/
	INTO :icd10_Dysfunction separated by ','
	from icd10_Dysfunction;
quit; 
%put &icd10_Dysfunction;



/**** USE VAPD Dataset to get all patient, hospitalization dates and diagnosis codes ****/
data all_diagnosis; 
set final.vapd_daily&year._v3;
keep patienticn sta6a datevalue newadmitdate newdischargedate specialtytransferdate  specialtydischargedate DiagnosisCode_1-DiagnosisCode_26;
run;

PROC SORT DATA=all_diagnosis;
BY  patienticn  datevalue newadmitdate newdischargedate specialtytransferdate  specialtydischargedate;
RUN;


/*left join mechvent_hosp variable*/
PROC SQL;
	CREATE TABLE all_diagnosis_V1 (compress=yes)  AS 
	SELECT A.*, B.proccode_mechvent_hosp as mechvent
	FROM   all_diagnosis A
	LEFT JOIN final.VAPD_mechvent_hosp_06292018 B  /*this sas dataset has the mechianical ventilation indicator at the hospitalization level*/
		ON A.patienticn =B.patienticn and a.newadmitdate=b.newadmitdate and a.newdischargedate=b.newdischargedate;
QUIT;


data final.vapd_angus_sepsis  (compress=yes); 
set all_diagnosis_V1 ;
array DiagnosisCode_ (26)  DiagnosisCode_1 - DiagnosisCode_26;  /*our VAPD has a maximum of 26 diagnosis codes*/
   do i = 1 to dim(DiagnosisCode_);

/* Detect Infection Codes */
if  substr(DiagnosisCode_{i}, 1, 3) in
               ('001','002','003','004','005','008','009','010','011','012',
               '013','014','015','016','017','018','020','021','022','023',
               '024','025','026','027','030','031','032','033','034','035',
               '036','037','038','039','040','041','090','091','092','093',
               '094','095','096','097','098','100','101','102','103','104',
               '110','111','112','114','115','116','117','118','320','322',
               '324','325','420','421','451','461','462','463','464','465',
               '481','482','485','486','494','510','513','540','541','542',
               '566','567','590','597','601','614','615','616','681','682',
               '683','686','730')  OR

           substr(DiagnosisCode_{i}, 1, 4) in
                ('5695','5720','5721','5750','5990','7110','7907',
               '9966','9985','9993')  OR

           substr(DiagnosisCode_{i}, 1, 5) in
               ('49121','56201','56203','56211','56213','56983')
			OR  DiagnosisCode_{i} in (&icd10_infection)  /*add in icd10 codes*/
	then infection = 1; /* Detect Infection Codes */

/* Detect Acute Organ Dysfunction Diagnosis Codes */
if   substr(DiagnosisCode_{i}, 1, 3) in
               ('458','293','570','584')  OR
           (substr(DiagnosisCode_{i}, 1, 4) in
               ('7855','3483','3481','2874','2875','2869','2866','5734'))
		OR DiagnosisCode_{i} in (&icd10_Dysfunction) /*add in icd10*/
		then  organ_dysfunction = 1;

/* Detect explicit diagnosis of severe sepsis or septic shock */
if substr(DiagnosisCode_{i}, 1, 5) in ('99592','78552') or DiagnosisCode_{i} in  ( 'R6520','R6521') 
then explicit_dx = 1; /* Detect explicit diagnosis of severe sepsis or septic shock */
		
/* Detect Acute Organ Dysfunction Procedure Codes (Mechanical ventilation)*/
if  explicit_dx NE 1 and organ_dysfunction NE 1 then do;
   if mechvent=1 then organ_dysfunction = 1; 
	end;

/* Aggregate the pieces */
if infection = 1 and organ_dysfunction = 1  then implicit_dx = 1;
if explicit_dx = 1 or implicit_dx = 1 then Angus = 1;
   else Angus = 0;

end;
run;

/*remove duplicates before joining to VAPD*/
PROC SORT DATA=final.vapd_angus_sepsis  nodupkey; 
BY  patienticn newadmitdate newdischargedate specialtytransferdate specialtydischargedate infection organ_dysfunction explicit_dx implicit_dx Angus;
RUN;


/*** End of Angus definition of sepsis indicators, match those newly created indicators back to VAPD ***/
