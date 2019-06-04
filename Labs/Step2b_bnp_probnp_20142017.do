/* 
Sarah Seelye
5.14.19
Imputing missing values for BNP - RefHigh
*/

clear all
cap log close
cap more off

cd "~\labs\BNP"
use "bnp_20142017_forstata20190510.dta"
set logtype text
log using "BNP_imputingRefHigh", replace


** check the RefHigh values for each sta3n **
bysort sta3n: tab refhigh_num, m

** check the average value for labchemresult for each sta3n & RefHigh **
bysort sta3n refhigh_num: sum labchemresultnumericvalue

** create a flag for sta3ns that have missing refhigh values **
gen flag = 1 if refhigh_num==.
replace flag = 0 if refhigh_num!=.

** count number of missing refhighs at each sta3n with any missing refhighs **
preserve
keep if flag==1
keep patienticn sta3n labchemtestsid refhigh_num refhigh
bysort sta3n labchemtestsid: gen sta3n_flagnum = _n
bysort sta3n labchemtestsid: egen sta3n_flagcnt = max(sta3n_flagnum)
tab sta3n if sta3n_flagcnt>100 //23 sta3ns with labchemtestsid's having over 100 missing
tab labchemtestsid sta3n if sta3n_flagcnt>100 & refhigh==""
restore

** merge with labchemtestname file **
format labchemsid %15.0g
merge 1:m patientsid labchemtestsid labchemspecimendatetime using "bnp_labchem_20190430"

tab _merge flag //58,207 not merged from master; 10,349 (0.64%) not merged from 
				//master that also do not have refhigh number
drop if _merge==2
drop _merge

** merge with vapd to bring in dobs **
merge m:1 patienticn using "vapd_dobs"
sort _merge
drop if _merge==2
drop _merge

* age of patient at time of lab draw
gen age = (labspecimendate-dob)/365

* recode values as indicated in 'chartreview_BNP_refranges'
//517
tab labchemtestname refhigh_num if sta3n==517, m 
table labchemtestsid refhigh_num if sta3n==517, m 
replace refhigh_num=50 if refhigh_num==. & sta3n==517 & (labchemtestname=="ZZBNP ANCILLARY" | labchemtestsid==1200042728)
tab labchemtestname refhigh_num if sta3n==517, m 

//521
tab labchemtestname refhigh_num if sta3n==521, m 
table labchemtestsid  if sta3n==521
replace refhigh_num=125 if refhigh_num==. & sta3n==521 & (labchemtestname=="proBNP" | labchemtestsid==1200088133) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==521 & (labchemtestname=="proBNP" | labchemtestsid==1200088133) & age>=75
tab labchemtestname refhigh_num if sta3n==521, m 

//528
tab labchemtestname refhigh_num if sta3n==528, m 
table labchemtestsid if labchemtestname=="BNP (BU)"
replace refhigh_num=200 if refhigh_num==. & sta3n==528 & (labchemtestname=="BNP (BU)" | labchemtestsid==1400589820) 
tab labchemtestname refhigh_num if sta3n==528, m 

//538
tab labchemtestname refhigh_num if sta3n==538, m 
table labchemtestsid  if sta3n==538
replace refhigh_num=125 if refhigh_num==. & sta3n==538 & (labchemtestname=="NT-proBNP" | labchemtestsid==1200000844) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==538 & (labchemtestname=="NT-proBNP" | labchemtestsid==1200000844) & age>=75
tab labchemtestname refhigh_num if sta3n==538, m 

//539
tab labchemtestname refhigh_num if sta3n==539, m 
table labchemtestsid  if sta3n==539 & labchemtestname=="NT-PRO BNP"
replace refhigh_num=125 if refhigh_num==. & sta3n==539 & (labchemtestname=="NT-PRO BNP" | labchemtestsid==1200128351) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==539 & (labchemtestname=="NT-PRO BNP" | labchemtestsid==1200128351) & age>=75
tab labchemtestname refhigh_num if sta3n==539, m 

//568
tab labchemtestname refhigh_num if sta3n==568, m 
table labchemtestsid  if sta3n==568 & labchemtestname=="NT-proBNP"
replace refhigh_num=125 if refhigh_num==. & sta3n==568 & (labchemtestname=="NT-proBNP" | labchemtestsid==1000103249) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==568 & (labchemtestname=="NT-proBNP" | labchemtestsid==1000103249) & age>=75
tab labchemtestname refhigh_num if sta3n==568, m 

//573
tab labchemtestname refhigh_num if sta3n==573, m 
table labchemtestsid  if sta3n==573 & labchemtestname=="NT-proBNP"
replace refhigh_num=125 if refhigh_num==. & sta3n==573 & (labchemtestname=="NT-proBNP" | labchemtestsid==1200105894) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==573 & (labchemtestname=="NT-proBNP" | labchemtestsid==1200105894) & age>=75
tab labchemtestname refhigh_num if sta3n==573, m 

//581
tab labchemtestname refhigh_num if sta3n==581, m 
table labchemtestsid  if sta3n==581 & labchemtestname=="NT-proBNP"
replace refhigh_num=450 if refhigh_num==. & sta3n==581 & (labchemtestname=="NT-proBNP" | labchemtestsid==1200112330) & age<50
replace refhigh_num=900 if refhigh_num==. & sta3n==581 & (labchemtestname=="NT-proBNP" | labchemtestsid==1200112330) & age>=50 & age<75
replace refhigh_num=1800 if refhigh_num==. & sta3n==581 & (labchemtestname=="NT-proBNP" | labchemtestsid==1200112330) & age>=75
tab labchemtestname refhigh_num if sta3n==581, m 

//603
tab labchemtestname refhigh_num if sta3n==603, m 
table labchemtestsid  if sta3n==603 & labchemtestname=="NT-PROBNP"
replace refhigh_num=125 if refhigh_num==. & sta3n==603 & (labchemtestname=="NT-PROBNP" | labchemtestsid==1200087904) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==603 & (labchemtestname=="NT-PROBNP" | labchemtestsid==1200087904) & age>=75
tab labchemtestname refhigh_num if sta3n==603, m 

//614
tab labchemtestname refhigh_num if sta3n==614, m 
table labchemtestsid  if sta3n==614 & labchemtestname=="NT-proBNP (Abbott)"
replace refhigh_num=125 if refhigh_num==. & sta3n==614 & (labchemtestname=="NT-proBNP (Abbott)" | labchemtestsid==1200112779) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==614 & (labchemtestname=="NT-proBNP (Abbott)" | labchemtestsid==1200112779) & age>=75
tab labchemtestname refhigh_num if sta3n==614, m 

//636
tab labchemtestname refhigh_num if sta3n==636, m 
table labchemtestsid  if sta3n==636 
replace refhigh_num=125 if refhigh_num==. & sta3n==636 & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==636 & age>=75
tab labchemtestname refhigh_num if sta3n==636, m 

//652
tab labchemtestname refhigh_num if sta3n==652, m 
table labchemtestsid  if sta3n==652 
replace refhigh_num=125 if refhigh_num==. & sta3n==652 & labchemtestsid==1200063578 & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==652 & labchemtestsid==1200063578 & age>=75
tab labchemtestname refhigh_num if sta3n==652, m 

//653
tab labchemtestname refhigh_num if sta3n==653, m 
table labchemtestsid  if sta3n==653 & labchemtestname=="PRO-BNP"
replace refhigh_num=125 if refhigh_num==. & sta3n==653 & (labchemtestname=="PRO-BNP" | labchemtestsid==800078245) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==653 & (labchemtestname=="PRO-BNP" | labchemtestsid==800078245) & age>=75
tab labchemtestname refhigh_num if sta3n==653, m 

//659
tab labchemtestname refhigh_num if sta3n==659, m 
table labchemtestsid  if sta3n==659 & labchemtestname=="POC BNP"
replace refhigh_num=100 if refhigh_num==. & sta3n==659 & (labchemtestname=="POC BNP" | labchemtestsid==1200132098) 
tab labchemtestname refhigh_num if sta3n==659, m 

//667
tab labchemtestname refhigh_num if sta3n==667, m 
table labchemtestsid  if sta3n==667 
replace refhigh_num=200 if refhigh_num==. & sta3n==667 & labchemtestsid==1000017449
tab labchemtestname refhigh_num if sta3n==667, m 

//688
tab labchemtestname refhigh_num if sta3n==688, m 
table labchemtestsid  if sta3n==688 & labchemtestname=="NT-ProBNP"
replace refhigh_num=125 if refhigh_num==. & sta3n==688 & (labchemtestname=="NT-ProBNP" | labchemtestsid==1400591962) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==688 & (labchemtestname=="NT-ProBNP" | labchemtestsid==1400591962) & age>=75
tab labchemtestname refhigh_num if sta3n==688, m 

* check number of missing RefHigh values 
tab refhigh_num, m  //3.15% miss - investigate these further 
tab sta3n flag if refhigh_num==. //appears some didn't make it to chart review

* investigating those with missing refhigh_num >150
tab labchemtestname refhigh_num if sta3n==442, m  //NT-PROBNP, 125, 450
table labchemtestsid refhigh_num if sta3n==442, m  //NT-PROBNP, 125, 450

tab labchemtestname refhigh_num if sta3n==459, m  // 128 missing - unclear how to recode them based on labchemtestname

tab labchemtestname refhigh_num if sta3n==534, m  //BNP, 100
table labchemtestsid refhigh_num if sta3n==534, m  //BNP, 100

tab labchemtestname refhigh_num if sta3n==552, m  //NT-proBNP, 125, 450
table labchemtestsid refhigh_num if sta3n==552, m  //NT-proBNP, 125, 450

tab labchemtestname refhigh_num if sta3n==558, m  //BNP, 100
table labchemtestsid refhigh_num if sta3n==558, m  //BNP, 100

tab labchemtestname refhigh_num if sta3n==618, m  //NT-PRO BNP, 450, 900, 1800
table labchemtestsid refhigh_num if sta3n==618, m  //NT-PRO BNP, 450, 900, 1800

tab labchemtestname refhigh_num if sta3n==672, m  //NT-PRO BNP, 125, 450
table labchemtestsid refhigh_num if sta3n==672, m  //NT-PRO BNP, 125, 450

* recode the missing refhighs for the above sta3ns by identified cutpoints 
//442
tab labchemtestname refhigh_num if sta3n==442, m 
table labchemtestsid  if sta3n==442 & labchemtestname=="NT-PROBNP"
replace refhigh_num=125 if refhigh_num==. & sta3n==442 & (labchemtestname=="NT-PROBNP" | labchemtestsid==800006062) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==442 & (labchemtestname=="NT-PROBNP" | labchemtestsid==800006062) & age>=75
tab labchemtestname refhigh_num if sta3n==442, m 

//534
tab labchemtestname refhigh_num if sta3n==534, m  
table labchemtestsid refhigh_num if sta3n==534, m 
replace refhigh_num=100 if refhigh_num==. & sta3n==534 & (labchemtestname=="BNP" | labchemtestsid==1200065140) 
tab labchemtestname refhigh_num if sta3n==534, m 

//552
tab labchemtestname refhigh_num if sta3n==552, m 
table labchemtestsid  if sta3n==552 & labchemtestname=="NT-proBNP"
replace refhigh_num=125 if refhigh_num==. & sta3n==552 & (labchemtestname=="NT-proBNP" | labchemtestsid==1200129454) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==552 & (labchemtestname=="NT-proBNP" | labchemtestsid==1200129454) & age>=75
tab labchemtestname refhigh_num if sta3n==552, m 

//558
tab labchemtestname refhigh_num if sta3n==558, m  
table labchemtestsid refhigh_num if sta3n==558, m 
table labchemtestname refhigh_num if sta3n==558, m 
replace refhigh_num=100 if refhigh_num==. & sta3n==558 & (labchemtestname=="B-TYPE NATRIURETIC PEPTIDE" | labchemtestsid==1200051013) 
tab labchemtestname refhigh_num if sta3n==558, m 

//618
tab labchemtestname refhigh_num if sta3n==618, m 
table labchemtestsid  if sta3n==618 & labchemtestname=="NT-PRO BNP"
replace refhigh_num=450 if refhigh_num==. & sta3n==618 & (labchemtestname=="NT-PRO BNP" | labchemtestsid==1000103782) & age<50
replace refhigh_num=900 if refhigh_num==. & sta3n==618 & (labchemtestname=="NT-PRO BNP" | labchemtestsid==1000103782) & age>=50 & age<75
replace refhigh_num=1800 if refhigh_num==. & sta3n==618 & (labchemtestname=="NT-PRO BNP" | labchemtestsid==1000103782) & age>=75
tab labchemtestname refhigh_num if sta3n==618, m 

//672
tab labchemtestname refhigh_num if sta3n==672, m 
table labchemtestsid  if sta3n==672 & labchemtestname=="PRO-BNP"
replace refhigh_num=125 if refhigh_num==. & sta3n==672 & (labchemtestname=="PRO-BNP" | labchemtestsid==1200080273) & age<75
replace refhigh_num=450 if refhigh_num==. & sta3n==672 & (labchemtestname=="PRO-BNP" | labchemtestsid==1200080273) & age>=75
tab labchemtestname refhigh_num if sta3n==672, m 

* re-check number of missing RefHigh values 
tab refhigh_num, m  //0.03% miss 
tab sta3n flag if refhigh_num==. 

* create uln variable
gen uln = labchemresultnumeric/refhigh_num
sum uln
count
display 1571397/1624369 //3.3% missing - try to lower more
drop uln 

* look at labchemresultvalues that are still missing
tab labchemresultvalue if labchemresultnumeric==.

* remove special characters from labchemresultvalues to replace missing labchemresultnumericvalues
gen labchemresultnumeric_rcd = labchemresultvalue 
replace labchemresultnumeric_rcd = subinstr(labchemresultnumeric_rcd, ">", "",.)
replace labchemresultnumeric_rcd = subinstr(labchemresultnumeric_rcd, "<", "",.)
replace labchemresultnumeric_rcd = subinstr(labchemresultnumeric_rcd, "\", "",.)
replace labchemresultnumeric_rcd = subinstr(labchemresultnumeric_rcd, "(GT)", "",.)
replace labchemresultnumeric_rcd = subinstr(labchemresultnumeric_rcd, "pg/mL", "",.)
replace labchemresultnumeric_rcd = subinstr(labchemresultnumeric_rcd, ",", "",.)
replace labchemresultnumeric_rcd = subinstr(labchemresultnumeric_rcd, " ", "",.)

destring labchemresultnumeric_rcd, gen(labchemresultnumeric_destr) force
tab labchemresultnumeric_rcd if labchemresultnumericvalue==.

* recode destringed labchemresultnumeric value to replace special characters as:
	* <2-<18 		=> 1
	* >18-<35		=> 20
	* <100			=> 90
	* >4000->5000 	=> 5000
	* 20k, 30k, 35k	=> 35k
	* 70k, 175k		=> 175k
replace labchemresultnumeric_destr=1 if labchemresultnumeric_destr>0 & labchemresultnumeric_destr<18 & labchemresultnumericvalue==. 
replace labchemresultnumeric_destr=20 if labchemresultnumeric_destr>=18 & labchemresultnumeric_destr<=35 & labchemresultnumericvalue==. 
replace labchemresultnumeric_destr=90 if labchemresultnumeric_destr>35 & labchemresultnumeric_destr<=100 & labchemresultnumericvalue==. 
replace labchemresultnumeric_destr=5000 if labchemresultnumeric_destr>=4000 & labchemresultnumeric_destr<=5000 & labchemresultnumericvalue==. 
replace labchemresultnumeric_destr=35000 if (labchemresultnumeric_destr==20000 | labchemresultnumeric_destr==30000 | labchemresultnumeric_destr==35000) & labchemresultnumericvalue==. 
replace labchemresultnumeric_destr=175000 if (labchemresultnumeric_destr==70000 | labchemresultnumeric_destr==175000) & labchemresultnumericvalue==. 

tab labchemresultnumeric_destr if labchemresultnumericvalue==.
sum labchemresultnumeric_destr labchemresultnumericvalue, detail

* create a flag variable for the labchemresultnumeric values that were changed
gen labchemresultnumeric_flag = 1 if labchemresultnumeric_destr!= labchemresultnumericvalue
replace labchemresultnumeric_flag = 0 if labchemresultnumeric_flag==.
tab labchemresultnumeric_flag

* replace labchemresultnumeric with the new version
drop labchemresultnumericvalue
gen labchemresultnumericvalue = labchemresultnumeric_destr
drop labchemresultnumeric_destr labchemresultnumeric_rcd

sum labchemresultnumericvalue

* create uln variable
gen uln = labchemresultnumericvalue/refhigh_num
sum uln //<0.1% missing

* descriptives of BNP by missing & non-missing units
sum labchemresultnumericvalue if clean_unit=="", detail
sum refhigh_num if clean_unit=="", detail
sum uln if clean_unit=="", detail

sum labchemresultnumericvalue if clean_unit=="PG/ML", detail
sum refhigh_num if clean_unit=="PG/ML", detail
sum uln if clean_unit=="PG/ML", detail

sum labchemresultnumericvalue, detail
sum refhigh_num, detail
sum uln, detail

* create daily high/low uln values for each patient
bysort patienticn labspecimendate: egen hi_bnp_uln_daily = max(uln)
bysort patienticn labspecimendate: egen lo_bnp_uln_daily = min(uln)

* remove dupicate high and low values by patient-day before merge w/ VAPD cohort
duplicates report patienticn labspecimendate hi_bnp lo_bnp
duplicates drop patienticn labspecimendate hi_bnp lo_bnp, force

* drop variables that we no longer need
drop 	flag sta3n_flag labchemtestname loincsid age dob		///
		labchemresultval2-labchemresultval17 uln_pct 

* save file to merge with VAPD		
save "all_bnp_hi_lo_2014_2017.dta", replace

log close
