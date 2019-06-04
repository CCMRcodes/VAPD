/*	Below code downloads lab data from CDW into a SAS dataset  

 	Date modified: 3/15/19
	Author: Sarah Seelye	*/


/* The %LIB_ASSIGN macro would correspond to this: */

libname sepsis '~~';  

%LIB_ASSIGN( LIB_REF=aiwas ,  LIBPATH=~~~,
             SERVER=vhacdwfpcfs02,  SHARE=Projects ) ;

proc datasets lib=~~~;
proc datasets lib=~~~;
run;

/* import data from SQL */ 

data sepsis.procalcitonin2014_2017 /*insert dataset name from SQL*/; /**/
set i8021dft.procalcitonin2014_2017 /*insert dataset name from SQL*/;
run; /*139710 obs*/ 


	/* NOTE: Find SAS dataset in "Servers/SASApp/Libraries/SEPSIS/name of dataset" */

/* remove duplicate labs */
proc sort data=sepsis.procalcitonin2014_2017 nodupkey;
	by PatientSID Sta3n LabChemResultNumericValue LabChemSpecimenDateTime; 
run; /*0 dups*/

/* change patienticn into numeric, create new labspecimen date variable, clean up units*/
data procalcitonin2014_2017_V2 (compress=yes rename=patienticn2=patienticn);
set sepsis.procalcitonin2014_2017;
patienticn2 = input(patienticn, 10.);
LabSpecimenDate=datepart(LabChemSpecimenDateTime); /*convert datetime to date*/
format LabSpecimenDate mmddyy10.;
units2=upcase(units); /*turn all units to upper case*/
units3=compress(units2, '.'); /*removes '.' in units*/
clean_unit = compress(units2); /*removes all blanks (by default - specify options to remove other chars) */
drop patienticn units2 units3 units;
run;

/*check units and topography of dataset*/
proc freq data=procalcitonin2014_2017_V2 order=freq;
table topography clean_unit;
run; 

/*check values of lab chem result*/
proc means data=procalcitonin2014_2017_V2 MIN MAX MEAN MEDIAN Q1 Q3 NMISS N;
VAR LabChemResultNumericValue; 
RUN;

/*check missing labs*/
data missing_unit; 
set procalcitonin2014_2017_V2;
if clean_unit='';
run; /*missing=1554*/

PROC MEANS DATA=missing_unit MIN MAX MEAN MEDIAN Q1 Q3 NMISS N;
VAR LabChemResultNumericValue; 
RUN;

/*keep only those with blood topography and acceptable clean_unit*/
DATA procalcitonin2014_2017_V3 (compress=yes); 
SET procalcitonin2014_2017_V2;
if Topography notin ('PLASMA', 'SERUM','BLOOD', 'SERUM 1') or clean_unit notin ('NG/ML','')
then delete;
RUN; /*139477 obs, dropped 233 obs*/

/*double-check lab value ranges & missing*/
DATA procalcitonin2014_2017_V4 (compress=yes); 
SET procalcitonin2014_2017_V3;
proc means MIN MAX MEAN MEDIAN P1 P5 P10 P20 P40 P60 P80 P90 P95 P99 NMISS N;
VAR LabChemResultNumericValue; 
RUN; /*44% missing LabChemResultNumericValue*/

/*****************************************************************************************************

	NOTE: 

	LabChemResultNumericValue has missing values because the numeric variable is based on the 
	variable "LabChemResultValue" in text format, which includes values such as "<0.05", and as such,
	appears as missing in numeric format. We will need to be aware of this for future lab pulls
	and make sure to verify missing values on "LabChemResultNumericValue" and update them as 
	necessary. 

	For procalcitonin, we are changing all values as follows (per communication with PI):
	<0.05 	=>	0
	0.05-1 	=> 	keep specific values
	>1		=>	N+1 (for values greater than 1)

******************************************************************************************************/

proc freq data=procalcitonin2014_2017_V4 order=freq;
table LabChemResultValue;
where LabChemResultNumericValue=.; 
run; 


/*change missing in LabChemResultNumericValue to "0" if "<0.1" in "LabChemResultValue"*/
data procalcitonin2014_2017_V8 (compress=yes); 
set procalcitonin2014_2017_V4;
labchemresultval2=LabChemResultValue;
labchemresultval3=compress(labchemresultval2, 'comment');
labchemresultval4=compress(labchemresultval3, 'SEE NOTE');
labchemresultval5=compress(labchemresultval4, 'SCANNED REPORT');
labchemresultval6=compress(labchemresultval5, 'HEMOLYZED');
labchemresultval7=compress(labchemresultval6, 'TNP');
labchemresultval8=compress(labchemresultval7, 'co');
labchemresultval9=compress(labchemresultval8, 'pending');
labchemresultval10=compress(labchemresultval9, 'CANCEL');
labchemresultval11=compress(labchemresultval10, 'Canc');
labchemresultval12=compress(labchemresultval11, 'ICTERIC');
labchemresultval13=compress(labchemresultval12, 'NEG');
labchemresultval14=compress(labchemresultval13, 'ng/ml'); /*removes all ng/ml*/
labchemresultval15=compress(labchemresultval14, '>'); /*removes all >; will assume value listed (eg >200 = 200; >199=199)*/
labchemresultval16=compress(labchemresultval15, 'M');
labchemresultval17=compress(labchemresultval16, 'H');
labchemresultval18=compress(labchemresultval17, 'dL');
labchemresultval19=compress(labchemresultval18, 'NGML');
labchemresultval20=compress(labchemresultval19, ' '); 
labchemresultval21=labchemresultval20;
if (labchemresultval20='<0.05') then labchemresultval21='0';
labchemresultval22=labchemresultval21;
if (labchemresultval21='<.05') then labchemresultval22='0';
labchemresultval23=labchemresultval22;
if (labchemresultval22='<=0.05') then labchemresultval23='0';
labchemresultval24=labchemresultval23;
if (labchemresultval23='<0.02') then labchemresultval24='0';
labchemresultval25=labchemresultval24;
if (labchemresultval24='<0.020') then labchemresultval25='0';
labchemresultval26=labchemresultval25;
if (labchemresultval25='<0.03') then labchemresultval26='0';
labchemresultval27=compress(labchemresultval26, '<');
labchemresultnum2=input(labchemresultval27, informat.);
LabChemResultNumericValue_flag=LabChemResultNumericValue;
if (LabChemResultNumericValue=.) then LabChemResultNumericValue_flag=1;
else LabChemResultNumericValue_flag=0;
labchemresultchar2=labchemresultval27;
drop labchemresultval2-labchemresultval27;
run;

/*cross-check missing to confirm flag variable*/
proc means data=procalcitonin2014_2017_V8 N NMISS;
VAR LabChemResultNumericValue; 
RUN;

proc freq data=procalcitonin2014_2017_V8 order=freq;
table LabChemResultNumericValue_flag; 
RUN;

/*recode labchemresultnum2 to account for '>' values in the string variable, 
such that values >N (when N>=1) are >N+1, per note above*/
data procalcitonin2014_2017_V9 (compress=yes);
set procalcitonin2014_2017_V8;
labchemresultgreaterthan=compress(LabChemResultValue, '>', 'K'); /*keep labchemresultvalue w/ '>'*/ 
run;

proc freq data=procalcitonin2014_2017_V9 order=freq;
table LabChemResultValue;
where labchemresultgreaterthan='>'; /*319 obs with '>' string*/
run;

data procalcitonin2014_2017_V10 (compress=yes);
set procalcitonin2014_2017_V9;
labchemresultnum3=labchemresultnum2;
if (labchemresultgreaterthan='>' and labchemresultnum2>1) then labchemresultnum3=labchemresultnum2+1;
else labchemresultnum3=labchemresultnum2;
run;

proc freq data=procalcitonin2014_2017_V10 order=freq;
table labchemresultnum3;
where (labchemresultgreaterthan='>') and (labchemresultnum3>2); /*304 obs recoded (n=14 were strings w/ values less than 1)*/
run;

proc freq data=procalcitonin2014_2017_V10 order=freq;
table LabChemResultValue;
where (labchemresultgreaterthan='>') and (labchemresultnum3>2); /*304 obs recoded*/
run;

data procalcitonin2014_2017_V11 (compress=yes);
set procalcitonin2014_2017_V10;
LabChemResultValue_recode=labchemresultchar2;
LabChemResultNumericValue_recode=labchemresultnum3;
drop labchemresultnum2 labchemresultnum3 labchemresultchar2 labchemresultgreaterthan;
run;

proc means data=procalcitonin2014_2017_V11 MIN MAX MEAN MEDIAN P1 P5 P10 P20 P40 P60 P80 P90 P95 P99 NMISS N;
var LabChemResultNumericValue_recode; /*after recoding variables, NMissing=976 out of 139,477 (0.7%) */
run;

proc means data=procalcitonin2014_2017_V11 MIN MAX MEAN MEDIAN P1 P5 P10 P20 P40 P60 P80 P90 P95 P99 NMISS N;
var LabChemResultNumericValue; /*after recoding variables, NMissing=976 out of 139,477 (0.7%) */
run;

/*create HIGH & LOW values by patient and date */
PROC SQL;
CREATE TABLE all_procal_hi_lo_2014_2017 (compress=yes)  AS  
SELECT *, max(LabChemResultNumericValue_recode) as hi_procalcitonin_daily, min(LabChemResultNumericValue_recode) as lo_procalcitonin_daily
FROM procalcitonin2014_2017_V11
GROUP BY patienticn, LabSpecimenDate
ORDER BY patienticn, LabSpecimenDate;
QUIT; 

/*remove dupicate high and low values by patient-day before left join to VAPD cohort*/
PROC SORT DATA=all_procal_hi_lo_2014_2017  nodupkey out=sepsis.all_procal_hi_lo_2014_2017; 
BY  patienticn LabSpecimenDate hi_procalcitonin_daily lo_procalcitonin_daily;
RUN; 

