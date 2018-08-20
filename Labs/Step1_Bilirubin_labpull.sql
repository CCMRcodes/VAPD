/*Below SQL code will pull Bilirubin labs from CDW*/
/*Step 2 will download the saved Bilirubin lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 8/20/2018
   Author: Shirley Wang */

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('14631-6','1975-2','42719-5','54363-7','59827-6','59828-4','35194-0','77137-8')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('TOT. BILIRUBIN', 'TOTAL BILIRUBIN', 'BILIRUBIN, TOTAL', 'BILIRUBIN,TOTAL', 'BILIRUBIN,TOTAL (V2)', 'BILIRUBIN TOTAL', 
'TOTAL BILIRUBIN*', 'TOTAL BILIRUBIN*IA', 'TOT.BILIRUBIN', 'T.BILIRUBIN', 'BILIRUBIN TOTAL, SERUM', 'Bilirubin', 'TOTAL BILIRUBIN (FV)', 'TOT BILIRUBIN', 
'TOT.BILIRUBIN,SERUM', 'TOTAL BILIRUBIN (CX)', 'BILIRUBIN, TOTAL-----', 'T.BILI', 'TOT BILIRUBIN(DCed 2.1.15', 'BILIRUBIN (DC"D 6/17)', 'TBIL(D/C 6/7/17)', 
'ZTOT. BILIRUBIN', 'BILIRUBIN,TOTAL,Blood', 'BILIRUBIN, TOTAL, SERUM', 'BILI,TOTAL', 'TOTAL BILIRUBIN, PLASMA', 'TOT. BILIRUBIN(DCd 2.17.15)', 'BILI TOTAL', 
'TOT. BILIRUBIN(BMT)', 'TOTAL BILIRUBIN (FS)', 'TOT. BILIRUBIN(LUF)', 'TBIL', 'TOT. BILIRUBIN(KTY)', 'TOT. BILIRUBIN(TMB)', 'P-TOTAL BILIRUBIN(I)', 
'TOTAL BILIRUBIN (MV)*INACT(1-1-15)', 'D-TOTAL BILIRUBIN', 'Bilirubin,Total-LC', 'MN BILI, TOTAL', 'PB TOTAL BILIRUBIN', 'W-BILIRUBIN TOTAL', 'FS-TOTAL BILIRUBIN* (V2)', 'REF-Bilirubin, Total', 
'BR-TOTAL BILI', 'LF-TOTAL BILIRUBIN* (V2/Q)', 'ELD TOTAL BILIRUBIN', 'TOT. BILIRUBIN (QUEST)', 'TOTAL BILIRUBIN---O', 'MMC TOTAL BILIRUBIN', 'BILIRUBIN,TOTAL(LABCORP)', 
'BILIRUBIN,TOTAL-Q', 'TOTAL BILIRUBIN QUEST', 'BILIRUBIN, TOTAL (LC)', 'ZZBILIRUBIN-LCA (D/C 11/16/17)', 'LRL TOTAL BILIRUBIN', 'Bilirubin, Total LC', 
'Bilirubin, Total (Quest)', 'TOTAL BILIRUBIN-LC', 'BILIRUBIN, TOTAL (AML)', 'TOTAL BILIRUBIN -', 'TOTAL BILIRUBIN (FIB)', 'TOTAL BILIRUBIN (QUEST),blood', 
'TOTAL BILI (REF LAB)', 'BILIRUBIN,TOTAL (LC)', 'TOTAL BILIRUBIN (Ref.Lab)', 'SALEM TOTAL BILIRUBIN  (PB)', 'BILIRUBIN TOTAL (TAMC)', 'MH BILIRUBIN TOTAL', 
'BILIRUBIN,TOTAL (Q)', 'BILIRUBIN, TOTAL-RBL', 'HATT-T.BILI', 'LEG T BILI', 'BILIRUBIN, TOTAL (FIBRO)')


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Bilirubin2014_2017
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
     WHERE /*loincsid=-1 and   */  
      a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'


/*get unique PatientICN*/
select a.*, b.PatientICN
into dflt.Bilirubin2014_2017
from #Bilirubin2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management