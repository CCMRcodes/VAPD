/* 
Sarah Seelye
5.16.19
Imputing missing values for Troponin's RefHigh, and finishing the data cleaning
steps from the SAS program, Step2_troponin_2014_2017.
*/

clear all
cap log close
cap more off

cd " "
use "troponin_forstata_20190515.dta"
set logtype text
log using "troponin_step2b", replace

* missing RefHigh_numeric
tab RefHigh_numeric, m //6.62% missing
tab RefHigh if RefHigh_numeric==., m

********************************************************************************
** impute missing RefHigh_numeric using values from same labchemtestsid & sta3n
********************************************************************************

* first determine if LabChemTestSIDs by Sta3n have the same RefHigh values
sort Sta3n LabChemTestSID RefHigh_numeric
bysort Sta3n LabChemTestSID: gen refhigh_diff = 1 if RefHigh_numeric!=RefHigh_numeric[_n-1]
bysort Sta3n LabChemTestSID: gen labchemsid_n = _n
replace refhigh_diff = 0 if labchemsid_n==1
replace refhigh_diff = . if RefHigh_numeric==.
bysort Sta3n LabChemTestSID: egen refhigh_morethanone = max(refhigh_diff) 

* replace refhigh_numeric with values from the same labchemtestsid if there is not 
* more than one refhigh value listed for a given labchemtestsid
sort Sta3n LabChemTestSID RefHigh_numeric //51,667 missing RefHighs can be replaced
sort Sta3n LabChemTestSID RefHigh_numeric
bysort Sta3n LabChemTestSID: replace RefHigh_numeric = RefHigh_numeric[_n-1] if RefHigh_numeric==. & refhigh_morethanone==0 
	//51,667 missing RefHigh values replaced
tab RefHigh_numeric refhigh_morethanone , missing

drop refhigh_diff labchemsid_n refhigh_morethanone

* next, investigate RefHigh values that are still missing
tab RefHigh_numeric, m //5.1% still missing (173,921)

* are the missing RefHighs from the same Sta3n?
tab Sta3n if RefHigh_numeric==. //yes, some Sta3ns contribute a lot of missingness

* are there multiple labchemtestsids with missing RefHighs from a single Sta3n?
table LabChemTestSID if Sta3n==436 & RefHigh_numeric==. //only 1 labchemtestsid
table LabChemTestSID if Sta3n==438 & RefHigh_numeric==. //2
table LabChemTestSID if Sta3n==528 & RefHigh_numeric==. //2
table LabChemTestSID if Sta3n==542 & RefHigh_numeric==. //3, but only 2 w/ more than 100
table LabChemTestSID if Sta3n==554 & RefHigh_numeric==. //2, but only 1 w/ more than 100
table LabChemTestSID if Sta3n==561 & RefHigh_numeric==. //1
table LabChemTestSID if Sta3n==568 & RefHigh_numeric==. //2
table LabChemTestSID if Sta3n==589 & RefHigh_numeric==. //3, but only 1 w/ more than 100
table LabChemTestSID if Sta3n==596 & RefHigh_numeric==. //1
table LabChemTestSID if Sta3n==618 & RefHigh_numeric==. //2, but only 1 w/ more than 100
table LabChemTestSID if Sta3n==646 & RefHigh_numeric==. //1
table LabChemTestSID if Sta3n==653 & RefHigh_numeric==. //1
table LabChemTestSID if Sta3n==667 & RefHigh_numeric==. //1
table LabChemTestSID if Sta3n==675 & RefHigh_numeric==. //1
table LabChemTestSID if Sta3n==693 & RefHigh_numeric==. //1
		
************************************************************************************
** create a dataset for Kate to do chart reviews for imputing the missing RefHigh **  
************************************************************************************

preserve
tab Sta3n if RefHigh_numeric==. //173,921 missing
keep if RefHigh_numeric==.
count

* only keep those LabChemTestSIDs with more than 100 missing RefHigh_numeric values
bysort Sta3n LabChemTestSID: gen refhigh_miss=_n 
bysort Sta3n LabChemTestSID: egen refhigh_maxmiss = max(refhigh_miss)
keep if refhigh_maxmiss>100 //298 dropped
tab  LabChemTestSID Sta3n // 4 sta3ns have more than 1 labchemtestsid w/ >100 missing

order Sta3n LabChemTestSID RefHigh_numeric refhigh_miss refhigh_maxmiss
drop refhigh_miss refhigh_maxmiss

* bring LabChemTestNames into dataset
merge m:1 LabChemTestSID Sta3n using "troponin_labname_20142017"
drop if _merge==2
count

* save dataset to export to SAS and merge with patient identifiers for 
* random sample and chart review
save "troponin_RefHigh_chartReview", replace

restore

		
********************************************************************************
** Change missing RefHigh values based on Kate's chart review. See excel 	  **
** spreadsheet in I: drive 'chartreview_trop_RefHighs' 						  **  
********************************************************************************
//436
table LabChemTestSID if Sta3n==436 & RefHigh_numeric==. 
tab LabChemTestName RefHigh_num if Sta3n==436, m 
replace RefHigh_num=0.04 if RefHigh_num==. & Sta3n==436 & LabChemTestSID==800006787
tab LabChemTestName RefHigh_num if Sta3n==436, m 

//438
table LabChemTestSID if Sta3n==438 & RefHigh_numeric==. //2
replace RefHigh_num=0.03 if RefHigh_num==. & Sta3n==438 & LabChemTestSID==1000103449
replace RefHigh_num=0.03 if RefHigh_num==. & Sta3n==438 & LabChemTestSID==1000103450
table LabChemTestSID RefHigh_num if Sta3n==438

//528
table LabChemTestSID if Sta3n==528 & RefHigh_numeric==. //2
replace RefHigh_num=0.3 if RefHigh_num==. & Sta3n==528 & LabChemTestSID==1400051415
replace RefHigh_num=0.04 if RefHigh_num==. & Sta3n==528 & LabChemTestSID==1400564575
table LabChemTestSID RefHigh_num if Sta3n==528 

//542
table LabChemTestSID if Sta3n==542 & RefHigh_numeric==. //3, but only 2 w/ more than 100
replace RefHigh_num=0.4 if RefHigh_num==. & Sta3n==542 & LabChemTestSID==1400033168
replace RefHigh_num=0.4 if RefHigh_num==. & Sta3n==542 & LabChemTestSID==1400067157
table LabChemTestSID RefHigh_num if Sta3n==542 

//554
table LabChemTestSID if Sta3n==554 & RefHigh_numeric==. //2, but only 1 w/ more than 100
replace RefHigh_num=0.04 if RefHigh_num==. & Sta3n==554 & LabChemTestSID==800033336
table LabChemTestSID RefHigh_num if Sta3n==554 

//561
table LabChemTestSID if Sta3n==561 & RefHigh_numeric==. //1
replace RefHigh_num=0.05 if RefHigh_num==. & Sta3n==561 & LabChemTestSID==1400026509
table LabChemTestSID RefHigh_num if Sta3n==561 

//568
table LabChemTestSID if Sta3n==568 & RefHigh_numeric==. //2
replace RefHigh_num=0.03 if RefHigh_num==. & Sta3n==568 & LabChemTestSID==1000002376
replace RefHigh_num=0.03 if RefHigh_num==. & Sta3n==568 & LabChemTestSID==1000087783
table LabChemTestSID RefHigh_num if Sta3n==568

//589
table LabChemTestSID if Sta3n==589 & RefHigh_numeric==. //3, but only 1 w/ more than 100
replace RefHigh_num=0.02 if RefHigh_num==. & Sta3n==589 & LabChemTestSID==1000060741
table LabChemTestSID RefHigh_num if Sta3n==589

//596
table LabChemTestSID if Sta3n==596 & RefHigh_numeric==. //1
replace RefHigh_num=0.03 if RefHigh_num==. & Sta3n==596 & LabChemTestSID==1200091868
table LabChemTestSID RefHigh_num if Sta3n==596

//618
table LabChemTestSID if Sta3n==618 & RefHigh_numeric==. //2, but only 1 w/ more than 100
replace RefHigh_num=0.02 if RefHigh_num==. & Sta3n==618 & LabChemTestSID==1000088917
table LabChemTestSID RefHigh_num if Sta3n==618

//646
table LabChemTestSID if Sta3n==646 & RefHigh_numeric==. //1
replace RefHigh_num=0.04 if RefHigh_num==. & Sta3n==646 & LabChemTestSID==1400057591
table LabChemTestSID RefHigh_num if Sta3n==646

//653
table LabChemTestSID if Sta3n==653 & RefHigh_numeric==. //1
replace RefHigh_num=0.09 if RefHigh_num==. & Sta3n==653 & LabChemTestSID==800006717
table LabChemTestSID RefHigh_num if Sta3n==653

//667
table LabChemTestSID if Sta3n==667 & RefHigh_numeric==. //1
replace RefHigh_num=0.03 if RefHigh_num==. & Sta3n==667 & LabChemTestSID==1000008020
table LabChemTestSID RefHigh_num if Sta3n==667

//675
table LabChemTestSID if Sta3n==675 & RefHigh_numeric==. //1
replace RefHigh_num=0.03 if RefHigh_num==. & Sta3n==675 & LabChemTestSID==1200135121
table LabChemTestSID RefHigh_num if Sta3n==675

//693
table LabChemTestSID if Sta3n==693 & RefHigh_numeric==. //1
replace RefHigh_num=0.03 if RefHigh_num==. & Sta3n==693 & LabChemTestSID==1400051254
table LabChemTestSID RefHigh_num if Sta3n==693


** Check Missing RefHigh Values **
sum RefHigh_numeric
tab RefHigh_numeric, m //0.01% missing (n=298)


********************************************************************************
** 						CHECK DESCRIPTIVES AGAIN 							  **
********************************************************************************

** Change Units for RefHigh (NG/L & PG/ML) **

* investigate unusual values
sum RefHigh_numeric, detail
sum RefHigh_numeric if RefHigh_numeric==100
tab clean_unit if RefHigh_numeric==100 // PG/ML need to be converted 
tab clean_unit // NG/L also needs to be converted
sum RefHigh_numeric if clean_unit!="NG/L" & clean_unit!="PG/ML", detail
sum RefHigh_numeric if clean_unit=="NG/ML" | clean_unit=="NG/ML.", detail

* recode string for NG/ML.
tab clean_unit
replace clean_unit="NG/ML" if clean_unit=="NG/ML."

* replace RefHigh values so that they're on the NG/ML scale
sum RefHigh_numeric if clean_unit=="PG/ML", detail
replace RefHigh_numeric = RefHigh_numeric/1000 if clean_unit=="PG/ML"
sum RefHigh_numeric if clean_unit=="PG/ML", detail

sum RefHigh_numeric if clean_unit=="NG/L", detail
replace RefHigh_numeric = RefHigh_numeric/1000 if clean_unit=="NG/L"
sum RefHigh_numeric if clean_unit=="NG/L", detail

* recode strings to NG/ML
replace clean_unit="NG/ML" if clean_unit=="PG/ML"
replace clean_unit="NG/ML" if clean_unit=="NG/L"

tab clean_unit, m

** Create New ULN **
lookfor uln
drop uln
gen uln = LabChemResultNumericValue/RefHigh_numeric

** Troponin - Missing Units **
sum LabChemResultNumericValue if clean_unit=="", detail
sum RefHigh_numeric if clean_unit=="", detail
sum uln if clean_unit=="", detail

** Troponin - Non-Missing Units **
sum LabChemResultNumericValue if clean_unit!="", detail
sum RefHigh_numeric if clean_unit!="", detail
sum uln if clean_unit!="", detail


** Descriptives by LabChemTestName **
preserve
bysort LabChemTestName Topography clean_unit: egen LabChemMedian = median(LabChemResultNumericValue)
bysort LabChemTestName Topography clean_unit: egen LabChemP10 = pctile(LabChemResultNumericValue), p(10)
bysort LabChemTestName Topography clean_unit: egen LabChemP90 = pctile(LabChemResultNumericValue), p(90)
bysort LabChemTestName Topography clean_unit: egen RefHighMedian = median(RefHigh_num)
bysort LabChemTestName Topography clean_unit: egen RefHighP10 = pctile(RefHigh_num), p(10)
bysort LabChemTestName Topography clean_unit: egen RefHighP90 = pctile(RefHigh_num), p(90)
bysort LabChemTestName Topography clean_unit: gen LabChemCount = _n
bysort LabChemTestName Topography clean_unit: egen LabChemNum = max(LabChemCount)
keep LabChemTestName Topography clean_unit LabChemMedian LabChemP10 LabChemP90 RefHighMedian RefHighP10 RefHighP90 LabChemNum
drop if LabChemTestName==""
duplicates drop
gsort -LabChemMedian
restore

		/* HP Review 5/21/29: keep all LabChemTestNames */

* create daily high/low uln values for each patient
bysort patienticn LabSpecimenDate: egen hi_trop_uln_daily = max(uln)
bysort patienticn LabSpecimenDate: egen lo_trop_uln_daily = min(uln)

* remove dupicate high and low values by patient-day before merge w/ VAPD cohort
duplicates report patienticn LabSpecimenDate hi_trop lo_trop
duplicates drop patienticn LabSpecimenDate hi_trop lo_trop, force

* save file to merge with VAPD		
save "all_trop_hi_lo_2014_2017.dta", replace

log close

		/* Note:	Following PI review, we will keep all LabChemTestNames.
					Next steps: convert saved file to SAS format, 
					complete random sample of spot checks, and merge into VAPD. */
