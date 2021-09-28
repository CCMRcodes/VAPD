/*Below SQL code will pull Lactate labs from CDW*/
/*Step 2 will download the saved Lactate lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 12/7/2018
   Author: Shirley Wang */

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('59032-3','30242-2','51829-0','30241-4','14118-4','32693-4','2518-9','19239-3','19240-1','2519-7','32132-3','32133-1','2524-7')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('_POC ABG LA', 'AT- LACTATE', 'ATS LACTIC ACID', 'BG LACTATE', 'CVICU-LACTIC ACID', 'GAS-LACTATE', 'GAS-LACTATE(SICU)', 
'GEM-Lactate', 'iLACTATE', 'ISTAT LACTATE', 'iSTAT LACTATE', 'I-STAT LACTATE', 'LACT ACID (.5-2.2mmol/L)-DON"T USE', 'LACTATE', 'Lactate', 'LACTATE  (MA)', 
'LACTATE - ARTERIAL {i-STAT}', 'Lactate (ABG)', 'LACTATE (ART)', 'LACTATE (BLOOD GAS)', 'Lactate (Gas)', 'Lactate (Gas)(Pre-3/29/18)', 'LACTATE (ISTAT)', 'LACTATE (LAB)', 
'LACTATE (OR)', 'LACTATE (POC)', 'LACTATE (VEN)', 'LACTATE (VENOUS BLOOD)', 'Lactate ABL', 'LACTATE BLOOD(POC STL)', 'LACTATE(...03/2009)*CI', 'LACTATE,ARTERIAL BLOOD', 
'Lactate..', 'LACTATE---BLOOD/FLUID', 'Lactate-iStat', 'LACTATE-P(BLOOD GAS)*', 'LACTATE-POC', 'LACTATE-SALISBURY ABG', 'LACTATE-WB', 'LACTIC ACD', 'LACTIC ACID', 
'LACTIC ACID  (STL-PB)', 'LACTIC ACID (ARTERIAL)', 'LACTIC ACID (B) DC"d 9/4/7', 'LACTIC ACID (B-GAS)', 'LACTIC ACID (BU/SY)', 'LACTIC ACID (CN/AL/BH)', 
'LACTIC ACID (D/C 8/1/13)', 'LACTIC ACID (dc"d 6-10-09)', 'LACTIC ACID (DCed 2.1.15', 'LACTIC ACID (DCT 3/2015)', 'LACTIC ACID (FV)', 'LACTIC ACID (FVAMC only)', 
'LACTIC ACID (IN HOUSE)', 'LACTIC ACID (LABCORP)PRIOR TO 12/08', 'LACTIC ACID (mmol/L)', 'LACTIC ACID (NEW)', 'LACTIC ACID (OLD)', 'LACTIC ACID (PLASMA)', 
'LACTIC ACID (plasma)', 'LACTIC ACID (PLASMA)~disc 8/13', 'LACTIC ACID (Q)(For CBOC use only)', 'LACTIC ACID (QUEST)', 'LACTIC ACID (QUEST)(dc"d)', 'LACTIC ACID (Sanford)365', 
'LACTIC ACID (UNSPECIFIED)', 'LACTIC ACID (VENOUS)', 'LACTIC ACID (WR)(dc"d 9/30/11)', 'LACTIC ACID Dc"d 11/8/10', 'LACTIC ACID II DC"D', 'LACTIC ACID SPL', 
'LACTIC ACID(..4/17)*IC', 'LACTIC ACID(1/DAY)', 'LACTIC ACID(DCd 2.17.15)', 'LACTIC ACID(POST 6/4/97)', 'LACTIC ACID(PRIOR TO 11/5/15)', 'LACTIC ACID(Roseburg)', 
'LACTIC ACID*', 'LACTIC ACID* THRU 3/31/18', 'LACTIC ACID*IA', 'LACTIC ACID*NE', 'LACTIC ACID, FLUID', 'LACTIC ACID, PLASMA', 'LACTIC ACID, ROUTINE', 'LACTIC ACID, STAT', 
'LACTIC ACID,BBC', 'LACTIC ACID,BLOOD', 'LACTIC ACID,CSF', 'LACTIC ACID,PLASMA', 'LACTIC ACID,PLASMA(QUEST)', 'LACTIC ACID.', 'Lactic-Gas', 'LC LACTIC ACID', 'MMC LACTIC ACID', 
'POC LACTATE', 'POC LACTIC ACID', 'POC-LACTATE(PRE 9/15/16)', 'POC-LACTIC ACID', 'STAT LACTIC ACID', 'zLACTIC ACID (DC 1-12)', 'zLACTIC ACID (NA,KX)', 'ZZ LACTIC ACID (DCT:031111)', 
'ZZLACTATE I-STAT (MA)', 'ZZLACTIC ACID (SY)(<1/21/07)', 'ZZLACTIC ACID DC 11-29-2011', 'ZZ-LACTIC ACID-QUEST', 'ZZZLactate.', 'ZZZLACTATE-P(BLOOD GAS)' )


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT distinct a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Lactate2014_2017
FROM  src.Chem_PatientLabChem AS A
INNER JOIN #loinc b on  a.Loincsid=b.Loincsid 
LEFT JOIN [CDWWork].[Dim].[topography] AS d ON A.TopographySID =D.TopographySID
	WHERE a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'  and a.CohortName='Cohort20210503' 

UNION

SELECT distinct a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
FROM src.Chem_PatientLabChem a         
INNER JOIN #labtestnames b ON a.labchemtestsid=b.labchemtestsid 
LEFT JOIN [CDWWork].[Dim].[topography] AS d ON A.TopographySID =D.TopographySID
     WHERE /* loincsid=-1 and   */  
      a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'  and a.CohortName='Cohort20210503' 


/*get unique PatientICN*/
select distinct a.*, b.PatientICN
into dflt.Lactate2014_2017
from #Lactate2014_2017 a
left join Src.SPatient_SPatient b on a.patientsid=b.PatientSID


--download dflt talbe into a SAS table to do further data management
-- then
drop table dflt.Lactate2014_2017

