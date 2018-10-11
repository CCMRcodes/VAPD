/*Below SQL code will pull WBC labs from CDW*/
/*Step 2 will download the saved WBC lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 10/11/2018
   Author: Shirley Wang */

use /*INSERT STUDY NAME*/
go

/*pull in all WBC_loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('26464-8','49498-9','6690-2','804-5')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('WBC',
'WBC (REFERENCE LAB)',
'ZWBC (RETIRED 6/29/05)',
'.WBC (MINOT AFB)DC 6/8/10', 
'AUTO WBC','CBC/WBC EX',
'COOK WBC',
'CORRECTED WBC',
'CORRECTED WBC-------0',
'C-WBC-CBOC',
'HOPC-WBC',
'LEG WBC',
'MH WBC (WET PREP)',
'MN WBC',
'NAVY STAT WBC',
'NEW WBC',
'OB-WBC',
'Q-WBC DC"D',
'T-CELL WBC',
'TOTAL WBC',
'Total WBC',
'Total WBC Count (AML)',
'Wbc',
'WBC------------------',
'WBC  -',
'WBC - SCAN Dc"d 1-21-08',
'WBC (AA)',
'WBC (AUTOMATED)',
'WBC (AUTOMATED) WR',
'WBC (BEFORE 5/9/06)',
'WBC (DO NOT USE)',
'WBC (FCM)',
'WBC (FOR ANC CALC.)',
'WBC (FV)',
'WBC (LABCORP)',
'WBC (MV)',
'WBC (ORS)',
'WBC (REFERENCE LAB)',
'WBC (RESEARCH PANEL) (TO 6/13/05)',
'WBC (thru 10/6/09)',
'WBC (V2)',
'WBC {Reference Lab}',
'WBC {St. George}',
'WBC AUTO',
'WBC AUTO  -',
'WBC COUNT',
'WBC COUNT (K/uL)',
'WBC Dc"d 1-21-08',
'WBC SCAN Dc"D 4-9-09',
'WBC(CBOC)',
'WBC(EST)',
'WBC(PRE-2/2/12)',
'WBC*',
'WBC/uL',
'WBC2',
'WBC-auto (V1FC)',
'WBC--------------CSFO',
'WBC-FL',
'WBC"S',
'Z++WBC-OUTSIDE LAB',
'ZHS WBC',
'ZSJWBC(DC"D 5-10)',
'ZWBC (RETIRED 6/29/05)',
'zzz WBC(BRAD)',
'WBC (for CD4/CD8)',
'z*INACT*WBC (4-1-10)',
'ZSJUAWBC(DC"D 5-10)',
'WHITE BLOOD CELLS Thru 2/12/07',
'TOTAL WHITE BLOOD COUNT',
'WHITE CELL COUNT',
'WHITE CELLS, TOTAL')


/*pull WBC loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #WBC2014_2017
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
     WHERE /* loincsid=-1 and */    
      a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'


/*get unique PatientICN*/
select a.*, b.PatientICN
into dflt.WBC2014_2017
from #WBC2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download talbe dflt.WBC2014_2017 into a SAS table to do further data management