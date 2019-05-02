/*Below SQL code will pull ALT labs from CDW*/
/*Step 2 will download the saved ALT lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 4/3/2019
   Author: Shirley Wang */

use INSERT STUDY NAME
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('76625-3','16324-6','1742-6','1744-2','1743-4','77144-4','44785-4','48134-1','76625-3','77144-4')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('ALT', 'SGPT', 'ALT (SGPT)', 'ALT/SGPT', 'ALANINE AMINOTRANSFERASE', 'ALT(SGPT)', 'ALT (V2)', 
'SGPT(ALT)', 'SGPT (ALT)', 'ALT*', 'ALT*IC', 'ALT (667)', 'ALT/GPT', 'ALANINE AMINOTRANSFERASE (SGPT)', 'SGPT/ALT', 
'ALT(...11/18)*NE', 'ALT (SGPT) - NEW', 'ALT (FV)', 'SGPT (CX)', 'SGPT (DCed 2.1.15', 'ALT,Blood', 'ALT(D/C 6/7/17)', 'ALANINE AMINO-TRANSFERASE', 
'ALT -LX THRU 3/31/18', 'ALT-CP', 'ALT, PLASMA', 'ALT(DC,11/18/14)', 'ALT(BMT)', 'ALT(LUF)', 'ALT (FS)', 'ALT(KTY)', 'ALT(TMB)', 
'P-ALT(I)', 'ALT (MV)*INACT(1-1-15)', 'ALT-MO', 'D-SGPT (ALT)(dc"d)', 'ALT-LC', 'MN ALT (SGPT)', 'PB ALT', 'W-ALANINE AMINOTRANSFERASE', 
'FS-ALT* (V2)', 'REF-SGPT', 'ALT (SGPT)(REF)', 'BR-ALT', 'ALT (SGPT) P5P', 'LF-ALT* (V2/Q)', 'ALT (SGPT) P5P (LC)', 'ALT (QUEST)', 
'ALT---O', 'ALT(SGPT)(LABCORP)', 'MMC ALT', 'ELD ALT (SGPT)', 'LRL ALT(SGPT)', 'ZZ-ALT-LCA(DO NOT USE)', 'ALT (SGPT) LC', 
'ALT (QUEST)', 'SALEM ALT  (PB)', 'ALT (SGPT) P5P-Q', 'ALT(SGPT)QUEST', 'ALT -', 'ALT QUEST', 'ALT (QUEST),blood', 'ALT-Q', 'ALT (REF LAB)', 'ALT (FIBROSIS)')


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #ALT2014_2017
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
     WHERE a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'


/*get unique PatientICN*/
select a.*, b.PatientICN
into dflt.ALT2014_2017
from #ALT2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

/*compress tables*/
alter table dflt.ALT2014_2017 
rebuild partition=ALL
with
(data_compression=page)
--download dflt talbe into a SAS table to do further data management, code in step 2