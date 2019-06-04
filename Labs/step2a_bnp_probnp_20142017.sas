/*	Below code downloads lab data from CDW into a SAS dataset  

 	Date modified: 3/22/19
	Author: Sarah Seelye	*/


libname sepsis '~~';  /*you can create your own folder Sarah and save lab datasets there*/

%LIB_ASSIGN( LIB_REF=aiwas ,  LIBPATH=~~~,
             SERVER=vhacdwfpcfs02,  SHARE=Projects ) ;

proc datasets lib=~~~;
proc datasets lib=~~~;
run;


/* import data from SQL */ 
data sepsis.bnp_probnp_20142017_sms20190321; 
set i8021dft.bnp_probnp_20142017_sms20190321;
run; /*1,629,547 obs*/ 

data sepsis.bnp_labchem; 
set i8021dft.bnp_sms20190321;
run; /*1,629,547 obs*/


/* remove duplicate labs */
proc sort data=sepsis.bnp_probnp_20142017_sms20190321 nodupkey;
	by PatientSID Sta3n LabChemResultNumericValue LabChemSpecimenDateTime; 
run; /*4 dups, n=1,629,543*/

/* change patienticn into numeric, create new labspecimen date variable, clean up units*/
data bnp_20142017V1 (compress=yes rename=patienticn2=patienticn);
set sepsis.bnp_probnp_20142017_sms20190321;
patienticn2 = input(patienticn, 10.);
LabSpecimenDate=datepart(LabChemSpecimenDateTime); /*convert datetime to date*/
format LabSpecimenDate mmddyy10.;
units2=upcase(units); /*turn all units to upper case*/
units3=compress(units2, '.'); /*removes '.' in units*/
clean_unit = compress(units2); /*removes all blanks (by default - specify options to remove other chars) */
drop patienticn units2 units3 units;
run;

/*count number of missing LOINCS*/
proc means data=bnp_20142017V1 MIN MAX MEAN MEDIAN NMISS N;
VAR LOINCSID; 
where LOINCSID=-1;
RUN; /*0.1% missing LOINCS*/

/*check the units and topography*/
proc freq data=bnp_20142017V1 order=freq;
table topography clean_unit;
run;

/*check labs w/ missing units*/
data missing_unit; 
set bnp_20142017V1;
if clean_unit='';
run; /*missing=1777*/

data missing_refhigh;
set bnp_20142017V1;
if RefHigh='';
run;

proc means data=missing_unit MIN MAX MEAN MEDIAN Q1 Q3 NMISS N;
VAR LabChemResultNumericValue;
where clean_unit='';
run;

/*drop all clean_units that are not 'PG/ML' and non-blood topographies*/
data bnp_20142017V2 (compress=yes);
set bnp_20142017V1;
if topography in ('PLEURAL FLUID') or clean_unit notin ('PG/ML', '')
then delete;
run; *dropped 58, n=1,629,485; 

proc freq data=bnp_20142017V2 order=freq;
table topography clean_unit;
run;

/*check the number of missing for RefHigh and RefLow and export to excel*/
ODS TAGSETS.ExcelXP 
file="~~~.xls"
style=minimal
options ( Orientation = 'landscape'
FitFoPage = 'yes'
Pages_FitWidth = '1'
Pages_FitHeigh = '100'); *open excel;

proc freq data=bnp_20142017V2 order=freq;
table RefHigh RefLow;
run; /*RefHigh miss=270,731 (out of 1,629,485; 16.6%)

ods target.excelxp close; *close excel;

/*create %ULN variable using labchemresultnumericvalues and RefHigh*/

*first create RefHigh numeric variable;
data bnp_20142017V3 (compress=yes);
set bnp_20142017V2;
RefHigh_num=input(RefHigh, informat.);
run;

*comparing missing values for RefHigh & RefHigh_num;
ODS TAGSETS.ExcelXP 
file="~~~.xls"
style=minimal
options ( Orientation = 'landscape'
FitFoPage = 'yes'
Pages_FitWidth = '1'
Pages_FitHeigh = '100'); *open excel;

proc freq data=bnp_20142017V3 order=freq;
table RefHigh RefHigh_num;
run;

ods target.excelxp close; *close excel;

*change special characters of RefHigh so they don't appear as missing in the numeric;
data bnp_20142017V4 (compress=yes); 
set bnp_20142017V3;
RefHigh_2=RefHigh;
if (RefHigh='<125') then RefHigh_2='125';
RefHigh_3=RefHigh_2;
if (RefHigh_2='"< 100"') then RefHigh_3='100';
RefHigh_4=RefHigh_3;
if (RefHigh_3='<450') then RefHigh_4='450';
RefHigh_5=RefHigh_4;
if (RefHigh_4='"<125"') then RefHigh_5='125';
RefHigh_6=RefHigh_5;
if (RefHigh_5='""<100""') then RefHigh_6='100';
RefHigh_7=RefHigh_6;
if (RefHigh_6='"<450"') then RefHigh_7='450';
RefHigh_8=RefHigh_7;
if (RefHigh_7='"</=450"') then RefHigh_8='450';
RefHigh_9=RefHigh_8;
if (RefHigh_8='""<100 pg/mL""') then RefHigh_9='100';
RefHigh_10=RefHigh_9;
if (RefHigh_9='"">/=900""') then RefHigh_10='900';
RefHigh_11=RefHigh_10;
if (RefHigh_10='"">450""') then RefHigh_11='450';
RefHigh_12=RefHigh_11;
if (RefHigh_11='"-100"') then RefHigh_12='100';
RefHigh_13=RefHigh_12;
if (RefHigh_12='</=450') then RefHigh_13='450';
RefHigh_14=compress(RefHigh_13, '"SEE INTERP"');
RefHigh_15=compress(RefHigh_14, '"SEE COMMENT"');
RefHigh_16=compress(RefHigh_15, '"See Interpretation below"');
RefHigh_17=compress(RefHigh_16, '"See Eval"');
RefHigh_18=input(RefHigh_17, informat.);
drop RefHigh_2-RefHigh_17 RefHigh_num;
run;

proc freq data=bnp_20142017V4 order=freq;
table RefHigh RefHigh_18;
run;

data bnp_20142017V5 (compress=yes); 
set bnp_20142017V4;
RefHigh_num=RefHigh_18;
drop RefHigh_18;
run;

*comparing missing values for RefHigh & RefHigh_num;
ODS TAGSETS.ExcelXP 
file="~~~.xls"
style=minimal
options ( Orientation = 'landscape'
FitFoPage = 'yes'
Pages_FitWidth = '1'
Pages_FitHeigh = '100'); *open excel;

proc freq data=bnp_20142017V5 order=freq;
table RefHigh RefHigh_num;
run;

ods target.excelxp close; *close excel;

/* % ULN */
data bnp_20142017V6 (compress=yes);
set bnp_20142017V5;
ULN_pct=(LabChemResultNumericValue/RefHigh_num) ; 
run;

proc freq data=bnp_20142017V6 order=freq;
table RefHigh_num;
run; /*missing=280,569 (17.2%)*/

proc means data=bnp_20142017V6 MEAN MEDIAN Q1 Q3 MIN MAX NMISS N;
var ULN_pct; /*20.1% missing*/
run;

/** To reduce high % missing of %ULN, recode LabChemResultNumericValues & RefHigh missing values **/

/*labchemresultnumericvalue*/
proc means data=bnp_20142017V6 MIN MAX MEAN MEDIAN Q1 Q3 NMISS N;
VAR LabChemResultNumericValue; /*3.5% of total dataset missing labchem result values*/
RUN;

proc freq data=bnp_20142017V6 order=freq;
table LabChemResultValue;
where LabChemResultNumericValue=.;
run;


/*change special characters in LabChemResultValue and recode LabChemResultValues that have </> values.
this will serve to remove missingness that appears in the LabChemResultNumericValue variable.
recode </> values for LabChemResultValue as follows:
<2 to <18 => 1
>18 to <35 => 20
<100 => 90
>400 to >5000 => 5000
20,000, 30,000, 35,000 => 35,000
70,000 to 175,000 => 175,000
*/

/*step 1: remove special characters in string variable*/
data bnp_20142017V7 (compress=yes); 
set bnp_20142017V6;
if LabChemResultValue in('canc',
						'comment', 
						'NOTREPORTED',
						'NONREPORTABLE',
						'HEMOLYZED',
						'NOT REPORTABLE',
						'pending',
						'No Result',
						'CANC',
						'GT',
						'TNP',
						'Cancelled',
						'NOT REPORTED',
						'cancel',
						'Canc')
						then delete; /*dropped 5117*/
labchemresultval2=LabChemResultValue;
labchemresultval3=compress(labchemresultval2, '<');
labchemresultval4=compress(labchemresultval3, '>');
labchemresultval5=compress(labchemresultval4, 'pg/mL');
labchemresultval6=compress(labchemresultval5, '(GT)');
labchemresultval7=compress(labchemresultval6, '\');
labchemresultval8=compress(labchemresultval6, ',');
labchemresultval9=labchemresultval8;
if (labchemresultval8='\452') then labchemresultval9='452';
labchemresultval10=labchemresultval9;
if (labchemresultval9='48.14.') then labchemresultval10='48.14';
labchemresultval11=input(labchemresultval10, informat.);
run; /*1,624,368*/

/*check that all of the values that are missing on LabChemResultNumericValue are now in numeric form for conversion*/
proc freq data=bnp_20142017V7 order=freq;
table labchemresultval10;
where LabChemResultNumericValue=.; 
run;

/*step 2: recode values for labchemresultval11 as specified 
<2 to <18 => 1
>18 to <35 => 20
<100 => 90
>400 to >5000 => 5000
20,000, 30,000, 35,000 => 35,000
70,000 to 175,000 => 175,000 */
data bnp_20142017V8 (compress=yes); 
set bnp_20142017V7;
labchemresultval12=labchemresultval11;
if (LabChemResultNumericValue=. AND (labchemresultval11>=2 AND labchemresultval11<=18)) then labchemresultval12=1;
labchemresultval13=labchemresultval12;
if (LabChemResultNumericValue=. AND (labchemresultval12>18 AND labchemresultval12<=35)) then labchemresultval13=20;
labchemresultval14=labchemresultval13;
if (LabChemResultNumericValue=. AND labchemresultval13=100) then labchemresultval14=90;
labchemresultval15=labchemresultval14;
if (LabChemResultNumericValue=. AND (labchemresultval14>=4000 AND labchemresultval14<=5000)) then labchemresultval15=5000;
labchemresultval16=labchemresultval15;
if (LabChemResultNumericValue=. AND (labchemresultval15=20000 OR labchemresultval15=30000 OR labchemresultval15=35000)) then labchemresultval16=35000;
labchemresultval17=labchemresultval16;
if (LabChemResultNumericValue=. AND (labchemresultval16>=70000 AND labchemresultval16<=175000)) then labchemresultval16=175000;
run;


proc means data=bnp_20142017V8 MIN MAX MEAN MEDIAN Q1 Q3 NMISS N;
VAR labchemresultval17 LabChemResultNumericValue; 
RUN;

proc freq data=bnp_20142017V8 order=freq;
table RefHigh;
where LabChemResultNumericValue>4000;
run; /*missing=280,569 (17.2%)*/


/* Identify Missing RefHigh*/
proc freq data=bnp_20142017V8 order=freq;
table RefHigh_num RefHigh;
run; 


/*Will need to recode ULN_pct*/
proc means data=bnp_20142017V8 MEAN MEDIAN Q1 Q3 MIN MAX NMISS N;
var ULN_pct; 
run;


/*	NOTE: Finished cleaning BNP in Stata. See do file "step2b_bnp_probnp_20142017" for final BNP data cleaning steps 
	and to view a copy of the dataset 'all_bnp_hi_lo_2014_2017', which will be used to merge in with VAPD cohort. */



