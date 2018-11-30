/*Below SQL code will pull Lactate labs from CDW*/
/*Step 2 will download the saved Lactate lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 10/11/2018
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
WHERE labchemtestname in ('LACTIC ACID', 'LACTATE', 'POC LACTATE', 'GAS-LACTATE(SICU)', 'GEM-Lactate', 'LACTIC ACID (B-GAS)', 
'LACTATE---BLOOD/FLUID', 'LACTATE-WB', 'Lactate..', 'Lactate', 'Lactic-Gas', 'LACTIC ACID (CN/AL/BH)', 'LACTATE (LAB)', 'LACTATE (POC)', 
'LACTIC ACID (NEW)', 'LACTIC ACID*', 'LACTIC ACID (BU/SY)', 'BG LACTATE', 'LACTATE-P(BLOOD GAS)*', 'LACTIC ACID,BLOOD', 'I-STAT LACTATE', 'LACTIC ACID (plasma)', 
'LACTIC ACID (mmol/L)', 'ZZZLactate.', 'LACTIC ACID*IA', 'LACTIC ACID(PRIOR TO 11/5/15)', 'GAS-LACTATE', 'LACTIC ACID  (STL-PB)', 'LACTATE (VEN)', 
'LACTATE-SALISBURY ABG', 'LACTIC ACID(1/DAY)', 'ISTAT LACTATE', 'LACTATE (ART)', 'POC-LACTIC ACID', 'LACTATE (OR)', 'LACTIC ACID,PLASMA', 
'Lactate (Gas)', 'LACTATE (VENOUS BLOOD)', 'ATS LACTIC ACID', 'LACTATE BLOOD(POC STL)', 'LACTIC ACID(POST 6/4/97)', 'LACTATE  (MA)', 'LACTIC ACID(..4/17)*IC', 
'LACTIC ACID (DCed 2.1.15', 'LACTIC ACID (VENOUS)', 'Lactate ABL', 'LACTIC ACID (FV)', 'iSTAT LACTATE', 'LACTIC ACID (PLASMA)', 'LACTATE (ISTAT)', 
'LACTIC ACID(DCd 2.17.15)', 'LACTIC ACID(Roseburg)', 'LACTIC ACD', 'CVICU-LACTIC ACID', 'AT- LACTATE', 'iLACTATE', 'POC LACTIC ACID', 'LACTATE,ARTERIAL BLOOD', 
'LACTIC ACID, PLASMA', 'LACTIC ACID (UNSPECIFIED)', 'LACTIC ACID (FVAMC only)', 'LACTIC ACID (IN HOUSE)', 'Lactate (ABG)', 'LACTIC ACID.', 'LACTATE-POC', 
'LACTATE (BLOOD GAS)', 'LACTIC ACID,BBC', 'LACTIC ACID (Q)(For CBOC use only)', 'LACTIC ACID (ARTERIAL)',
'_POC ABG LA', 'LACTIC ACID (TOMAH ONLY)', 'LACTIC ACID,PLASMA(QUEST)', 'LACTIC ACID (CBOCS ONLY)', 
'LACTIC ACID (DCT 3/2015)', 'LACTIC ACID(WX)', 'MMC LACTIC ACID', 'STAT LACTIC ACID', 'LACTIC ACID WR', 'LACTIC ACID (Quest)[Inactive050316]', 
'Lactate/Pyruvate Ratio', 'LACTIC ACID (CDH)', 'CBOC-LACTIC ACID (DC"D 1/1/17)', 'LACTIC ACID*NE', 'LACTIC ACID,YALE', 'LACTIC ACID (QUEST) (DC"d 25Oct2017)', 'LACTIC ACID (WR)', 'LACTIC ACID [QUEST]', 'LACTIC ACID, PLASMA (004770)', 
'POC-LACTATE(PRE 9/15/16)', 'LACTIC ACID (Sanford)365', 'LACTATE - AMLK ROUTINE', 'LACTIC ACID (PLASMA)~disc 8/13', 'LACTIC ACID S/O', 
'SALEM LACTIC ACID  (PB', 'ZZZLACTATE-P(BLOOD GAS)', 'Lactic Acid (Ref Lab)')


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Lactate2014_2017
FROM  src.Chem_PatientLabChem AS A
INNER JOIN #loinc b on  a.Loincsid=b.Loincsid 
LEFT JOIN [CDWWork].[Dim].[topography] AS d ON A.TopographySID =D.TopographySID
	WHERE a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'

UNION

SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
FROM src.Chem_PatientLabChem a         
INNER JOIN #labtestnames b ON a.labchemtestsid=b.labchemtestsid 
LEFT JOIN [CDWWork].[Dim].[topography] AS d ON A.TopographySID =D.TopographySID
     WHERE /* loincsid=-1 and   */  
      a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'


/*get unique PatientICN*/
select a.*, b.PatientICN
into dflt.Lactate2014_2017
from #Lactate2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management