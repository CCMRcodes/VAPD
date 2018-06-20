/*STEP 2 of micro biology labs and cultures*/

/*Goal: for every patient-day, indicate (1=yes, 0=no) whether the patient had other micro biology labs or cultures done that day*/
/*There will be 2 final datasets: 1) other micro biology labs,  2) cultures*/

libname sepsis ''; /*insert file path/directory*/
/*Note, update code to your own study name in step 3: [INSERT STUDY NAME]*/

/*update all macro variables*/
%let studyname= ; /*insert your own study name path in datawarehouse (CDW)*/
%let data_source= " " ;/*insert your own data source*/
%let startdate = '20140101'; /*indicate start date*/
%let enddate = '20180101';/*indicate end date*/


/*download dflt.LabChemFinal3_14_18 into a sas table to do data management*/
PROC SQL ; 
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 
create table sepsis.LabChemFinal3_14_18 as select * FROM CONNECTION TO CDW1 
(SELECT t1.patientsid,t1.sta3n,t1.labchemtestname,t1.labchemspecimendatetime,t1.topography,t1.labchemresultvalue,
t1.labchemresultnumericvalue,t1.loincsid
FROM [INSERT STUDY NAME].[dflt].[LabChemFinal3_14_18] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 

/*download dflt.OrderedCulturesFinal3_14_18 into a sas table to do data management*/
PROC SQL ;  
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ) ; 

create table sepsis.OrderedCulturesFinal3_14_18 as select * FROM CONNECTION TO CDW1 
(SELECT t1.* FROM [INSERT STUDY NAME].[dflt].[OrderedCulturesFinal3_14_18] as t1) as t1;
DISCONNECT FROM CDW1 ;
quit; 


***--we realized that we need a file with ONLY blood cultures, so need to identify
which test/toppography combinations are blood culture;

**Create 1 file combining both datasets;
data all ;*(keep=patientsid dv1 hasmicro); 
   set sepsis.LabChemFinal3_14_18 (in=in1 keep=patientsid sta3n labchemspecimendatetime labchemtestname topography)
       sepsis.OrderedCulturesFinal3_14_18 (in=in2 keep=patientsid sta3n specimentakendatetime labchemtestname topography 
         collectionsample rename=(specimentakendatetime=labchemspecimendatetime));
dv1=datepart(labchemspecimendatetime);
if in1 then source='L';
if in2 then source='M';
format dv1 mmddyy10.;
if patientsid=. then delete;
hasmicro=1;
run;

proc sql; select distinct labchemtestname,topography
from all where topography in ('BLOOD','SERUM','PLASMA');
quit;

***Sent the above to PI in excel file then...

***PI marked which of these to keep as blood culture--import from excel to determine which to keep;
proc import file='INSERT PATH.xlsx'
dbms=xlsx out=bloodcult replace;
run;

proc sql;
  create table addkeep as 
  select a.*,b.keep
  from all a left join bloodcult b
  on a.labchemtestname=b.labchemtestname and a.topography=b.topography;
  quit;
**save blood cultures and other micro to separate files;

PROC FREQ DATA=addkeep  order=freq;
TABLE keep ;
RUN;

DATA  missing_keep; 
SET  addkeep;
if keep=.;
RUN;


/********************************************************/
data bloodcult other_micro;
set addkeep;
if keep=1 then output bloodcult;
if keep=0 or keep=. then output other_micro;
run;

**do all of below for both bloodcult and other_micro;

/*get unique patienticn for blood cultures dataset*/
proc sql;
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ); 
	create table bloodcult2 (compress=yes) as  
	SELECT * FROM CONNECTION TO CDW1 ( 
	select a.*, b.patienticn
	from bloodcult a
	left join [INSERT STUDY NAME].[src].[CohortCrosswalk] b on a.PatientSID=b.PatientSID;
);
DISCONNECT FROM CDW1;
QUIT;

/*change patienticn into numeric*/ 
DATA bloodcult3 (rename=patienticn2=patienticn);
SET bloodcult2 ;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;


/*get unique patienticn for microbiology labs dataset*/
proc sql;
CONNECT TO OLEDB  AS CDW1 ( PROVIDER=SQLNCLI11  DATASOURCE=&data_source.
   PROPERTIES=('INITIAL CATALOG'=CDWWORK 'INTEGRATED SECURITY'=SSPI)
   READBUFF=5000  CURSOR_TYPE=STATIC  defer=yes  utilconn_transient=yes
   CONNECTION=GLOBAL ); 
	create table other_micro2  (compress=yes) as  
	SELECT * FROM CONNECTION TO CDW1 ( 
	select a.*, b.patienticn
	from other_micro a
	left join [INSERT STUDY NAME].[src].[CohortCrosswalk] b on a.PatientSID=b.PatientSID;
);
DISCONNECT FROM CDW1;
QUIT;

/*change patienticn into numeric*/  
DATA other_micro3 (rename=patienticn2=patienticn);
SET other_micro2;
patienticn2 = input(patienticn, 10.);
drop patienticn;
RUN;

/*remove duplicates by patient and date*/
proc sort data=bloodcult3 nodupkey out=sepsis.has_bloodcult;  /*save final dataset*/
by patienticn dv1;
run;

DATA sepsis.bloodculture (rename= patienticn=patient_id rename=dv1=bcx_drawn_day); 
SET sepsis.has_bloodcult ;
keep patienticn dv1;
RUN;

proc sort data=other_micro3 nodupkey out=sepsis.has_oth_micro; /*save final dataset*/
by  patienticn dv1;
run;


