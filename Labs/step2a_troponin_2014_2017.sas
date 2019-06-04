/*	Below code downloads lab data from CDW into a SAS dataset  

 	Date modified: 4/08/19
	Author: Sarah Seelye	*/


/* The %LIB_ASSIGN macro would correspond to this: */

libname sepsis '~~~';  /*you can create your own folder Sarah and save lab datasets there*/

%LIB_ASSIGN( LIB_REF=aiwas ,  LIBPATH=~~~,
             SERVER=vhacdwfpcfs02,  SHARE=Projects ) ;

proc datasets lib=~~~;
proc datasets lib=~~~;
run;

/* import data from SQL */ 
data sepsis.troponin_I_T_20142017 /*insert dataset name from SQL*/; /**/
set i8021dft.Troponin20142017_sms20190322 /*insert dataset name from SQL*/;
run; /*3,447,896 obs*/ 

data sepsis.troponin_labname_20142017 /*insert dataset name from SQL*/; /**/
set i8021dft.troponin_labname_ss20190507 /*insert dataset name from SQL*/;
run; /*3,447,896 obs*/ 

/* remove duplicate labs */
proc sort data=sepsis.troponin_I_T_20142017 nodupkey;
	by PatientSID Sta3n LabChemResultNumericValue LabChemSpecimenDateTime; 
run; /*317 dups*/

/* change patienticn into numeric, create new labspecimen date variable, clean up units*/
data troponin2014_2017_V2 (compress=yes rename=patienticn2=patienticn);
set sepsis.troponin_I_T_20142017;
patienticn2 = input(patienticn, 10.);
LabSpecimenDate=datepart(LabChemSpecimenDateTime); /*convert datetime to date*/
format LabSpecimenDate mmddyy10.;
units2=upcase(units); /*turn all units to upper case*/
units3=compress(units2, '.'); /*removes '.' in units*/
clean_unit = compress(units2); /*removes all blanks (by default - specify options to remove other chars) */
drop patienticn units2 units3 units;
run;

/*check units and topography of dataset*/
proc freq data=troponin2014_2017_V2 order=freq;
table topography clean_unit;
run; 

/*what are the labchem values for the 'pos/neg' clean_unit*/
proc freq data=troponin2014_2017_V2 order=freq;
table LabChemResultNumericValue LabChemResultValue;
where clean_unit='POS/NEG'; *drop these below;
run; 

/*check missing labs*/
data missing_unit; 
set troponin2014_2017_V2;
if clean_unit='';
run; /*missing=21,443*/

PROC MEANS DATA=missing_unit MIN MAX MEAN MEDIAN Q1 Q3 NMISS N;
VAR LabChemResultNumericValue; 
RUN;

/*keep only those with blood topography and acceptable clean_unit*/
DATA troponin2014_2017_V3 (compress=yes); 
SET troponin2014_2017_V2;
if Topography in ('BODY FLUID, NOS') or clean_unit in ('POS/NEG', '%PCV', 'G/DL', '<0.05', 'NG/DL', '0.00-0.40', 'NF/ML', 'NG.ML')
then delete;
RUN; /*3,443,396; dropped 4183 obs*/

proc freq data=troponin2014_2017_V3 order=freq;
table clean_unit;
run;

/*look how the labchemvalues are coded for the weird units (ng/ml; mg/dl; etc)*/
proc freq data=troponin2014_2017_V3 order=freq;
table LabChemResultNumericValue;
where clean_unit='MG/DL';
run;

proc freq data=troponin2014_2017_V3 order=freq;
table LabChemResultNumericValue;
where clean_unit='NG/ML';
run;

/*check values of lab chem result & check % missing*/
proc means data=troponin2014_2017_V2 MIN MAX MEAN MEDIAN Q1 Q3 NMISS N;
VAR LabChemResultNumericValue; /*42% missing*/
RUN;


proc freq data=troponin2014_2017_V2 order=freq;
table RefHigh;
run; 


/*	recode LabChemResultValues that appear with </> as follows:  	*/
/*				trop<0.1 => 0.01									*/
/*				trop<0.3 => 0.29									*/
/*				trop>35  => 50										*/	

proc freq data=troponin2014_2017_V3 order=freq;
table LabChemResultValue;
where LabChemResultNumericValue=.;
run;

data troponin2014_2017_V4;
set troponin2014_2017_V3;
if LabChemResultValue in	('canc', 'NONDIAGNOSTIC', 'comment', 'HEMOLYZED', 'I',
							'NOTREPORTED', 'NOT REPORTED', 'notreported', 'Cancelled',	
							'TEST ERROR', 'pending', 'Canc', 'CANCELD', 'TNP', 'No Result',
							'qns', 'CANC', 'COMMENT', 'PENDING', 'QNS', 'canc.')
then delete;
labchemresult_char = compress(LabChemResultValue, "<>=", "k"); /*keep everything that is a <, >, =*/
labchemresult_flag =.;
	if labchemresult_char in ('<', '>', '<=', '=<') then labchemresult_flag=1;
	else labchemresult_flag=0;
new_lab_val1=compress(LabChemResultValue, "0123456789.", "k"); /*keep all digits and decimal places*/
new_lab_val2=input(new_lab_val1, informat.);
new_lab_val3=new_lab_val2;
	if labchemresult_flag=1 AND labchemresult_char='<' AND new_lab_val2<0.1 then new_lab_val3=0.01;
new_lab_val4=new_lab_val3;
	if labchemresult_flag=1 AND labchemresult_char='<' AND (new_lab_val3<=0.3 AND new_lab_val3>0.01) then new_lab_val4=0.29;
new_lab_val5=new_lab_val4;
	if labchemresult_flag=1 AND labchemresult_char='>' AND new_lab_val4>=35 then new_lab_val5=50;
run;

proc freq data=troponin2014_2017_V4 order=freq;
table labchemresult_flag labchemresult_char;
run;

proc freq data=troponin2014_2017_V4 order=freq;
table new_lab_val5;
where LabChemResultNumericValue=.;
run;

proc means data=troponin2014_2017_V4 MIN MAX MEAN MEDIAN Q1 Q3 NMISS N;
VAR new_lab_val5; /*1.9% missing*/
RUN;

/*convert units to NG/ML; drop MMOL/L & MG/DL*/
data troponin2014_2017_V5;
set troponin2014_2017_V4;
new_lab_val6=new_lab_val5;
	if clean_unit='NG/L' then new_lab_val6=LabChemResultNumericValue/1000;
new_lab_val7=new_lab_val6;
	if clean_unit='PG/ML' then new_lab_val7=LabChemResultNumericValue/1000;
if clean_unit in ('MMOL/L', 'MG/DL') then delete;
run;

proc freq data=troponin2014_2017_V5 order=freq;
table new_lab_val7;
where LabChemResultNumericValue=.;
run;

proc means data=troponin2014_2017_V5 MIN MAX MEAN MEDIAN Q1 Q3 NMISS N;
VAR new_lab_val7; /*<0.1% missing*/
RUN;

proc freq data=troponin2014_2017_V5 order=freq;
table new_lab_val7*LabChemResultValue;
where LabChemResultNumericValue=.;
run;

/*rename new_lab_val7 and drop other created variables*/
data troponin2014_2017_V6 (compress=yes);
set troponin2014_2017_V5;
drop new_lab_val1-new_lab_val6 LabChemResultNumericValue;
run;

data troponin2014_2017_V7 (compress=yes);
set troponin2014_2017_V6;
LabChemResultNumericValue=new_lab_val7;
drop new_lab_val7 labchemresult_char;
run;

proc means data=troponin2014_2017_V7 MIN MAX MEAN MEDIAN Q1 Q3 NMISS N;
VAR LabChemResultNumericValue; /*n=3,409,797; <0.1% missing*/
RUN;

/*************************/
/* Recode RefHigh Values */
/*************************/

proc freq data=troponin2014_2017_V7 order=freq;
table RefHigh;
run;

data troponin2014_2017_V8;
set troponin2014_2017_V7;
RefHigh_char = compress(RefHigh, "<>='*", "k"); /*keep everything that is a <, >, =*/
RefHigh_flag =.;
	if RefHigh_char in ('<', '>', '<=', '=<', '*', '"') then RefHigh_flag=1;
	else RefHigh_flag=0;
RefHigh_new1=compress(RefHigh, "0123456789.", "k"); /*keep all digits and decimal places*/
RefHigh_new2=RefHigh_new1;
	if RefHigh=' 0.08' then RefHigh_new2='0.08';
RefHigh_new3=input(RefHigh_new2, informat.);
run;

proc freq data=troponin2014_2017_V8 order=freq;
table RefHigh_flag RefHigh_new3;
where RefHigh_flag=1;
run;

proc freq data=troponin2014_2017_V8 order=freq;
table RefHigh RefHigh_new3; *6.6% missing RefHigh_new3;
run;

proc freq data=troponin2014_2017_V8 order=freq;
table LabChemResultNumericValue;
where RefHigh='"POSITIVE"'; *all POSITIVES are missing LabChemValue;
run;


/***************/
/* CREATE %ULN */
/***************/

data troponin2014_2017_V9;
set troponin2014_2017_V8;
ULN=(LabChemResultNumericValue/RefHigh_new3);
format ULN 10.4;
RefHigh_numeric=RefHigh_new3;
drop RefHigh_new1-RefHigh_new3 RefHigh_char;
run;

/******************/
/*CHECK NEW VALUES*/
/******************/

proc freq data=troponin2014_2017_V9 order=freq;
table RefHigh RefHigh_numeric LabChemResultNumericValue; *6.6% missing RefHigh_numeric;
run;

proc means data=troponin2014_2017_V9 MIN MAX MEAN MEDIAN P10 P90 NMISS N;
var RefHigh_numeric LabChemResultNumericValue ULN; *7.08% missing ULN;
run;

/*compare values for those with and without missing units*/
data missing_unit2;
set troponin2014_2017_V9;
if clean_unit='';
run;

proc means data=missing_unit2 MIN MAX MEAN MEDIAN P1 P10 P90 P99 NMISS N;
var RefHigh_numeric LabChemResultNumericValue ULN; 
run; *missing units have missing RefHigh values as well; 

proc freq data=missing_unit2 order=freq;
table RefHigh RefHigh_numeric;
run;

proc means data=troponin2014_2017_V9 MIN MAX MEAN MEDIAN P1 P10 P90 P99 NMISS N;
var RefHigh_numeric LabChemResultNumericValue ULN; *7.08% missing ULN_pct;
run;

/*investigating the units for the very high LabChem values*/
proc freq data=troponin2014_2017_V9 order=freq;
table clean_unit;
where LabChemResultNumericValue>1.0;
run;

proc freq data=sepsis.troponin_I_T_20142017 order=freq;
table units;
where LabChemResultNumericValue>5.0;
run;

proc freq data=troponin2014_2017_V2 order=freq;
table LabChemResultNumericValue*clean_unit;
where clean_unit in('NG/L', 'PG/ML');
run;

proc means data=troponin2014_2017_V2 MIN MAX MEAN MEDIAN P1 P10 P90 P99 NMISS N;
var LabChemResultNumericValue;
where clean_unit='NG/ML';
run;

/*investigate the missing values for RefHigh*/
proc sort data=troponin2014_2017_V9 out=refhighmiss;
by Sta3n LabChemTestSID;
where RefHigh_numeric=.;
run;

proc sort data=troponin2014_2017_V9 out=refhighnomiss;
by Sta3n LabChemTestSID;
where RefHigh_numeric NE .;
run;

/* recode the missing RefHigh values by RefHigh values for those with the same labchemtestsids within an sta3n */
data troponin10; 
set troponin2014_2017_V9;
by Sta3n LabChemTestSID;

proc freq data=refhighnomiss order=freq;
table RefHigh*LabChemTestSID;
by Sta3n LabChemTestSID;
run;

proc freq data=refhighmiss order=freq;
table LabChemTestSID;
by Sta3n;
run;

/*	FINISH DATA CLEANING AND REFHIGH IMPUTATION IN STATA -- SEE DO FILE 'step2b_troponin_2014_2017'; 



