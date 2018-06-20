/*Below SQL code will pull Platelets labs from CDW*/
/*Step 2 will download the saved Platelets lab pull table into SAS dataset for further cleaning*/

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('13056-7','26515-7','26516-5','777-3','778-1','49497-1')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('PLT', 'PLATELET COUNT', 'PLATELETS', 'PLT (V2)', 'PLT*', 'PLATELET', 'PLTS', 'PLT CT', 'Pltct', 'PLATELETS:', 'PLT3', 
'PLATELET COUNT-------', 'PLATELET (AA)', 'PLT (FV)', 'PLT(D/C 5/25/17)', 'PLATELET CT', 'PLATELET COUNT  -', 'PLATELET~disc 10/14', 'PLT (XN2000)', 
'PLT (COUNT)', 'PLT COUNT', 'PLT-AUTO', 'PLT(BMT)', 'PLT(LUFKIN)', 'PLATELET (TOPC)', 'PLT (FS)', 'PLT(s)', 'PLT(KTY)', 'PLT(TMB)', 'PLT (MV)*INACT(1-1-15)', 
'P-PLATELET COUNT', 'PLT (ESTM)', 'D-Platelets', 'MN PLT', 'MANUAL PLATELETS', 'PB PLT', 'W-PLATELETS', 'plt, lca', 'PLT (HR)', 'PLTCOUNT-COAG PANEL-O', 
'PLT-PIERRE', 'BR-PLT', 'PLT COUNT ESTIMATE', 'PLT (CD4/CD8)', 'PLATELET (BLUE TOP)', 'PLATELET ONLY(auto)', 'OR PLATELET', 'PLT-MMC', 'PLATELET IN CITRATE ANTICOAGULANT', 
'PLATELET CNT (BLUE TOP)', 'LRL PLATELET', 'PLATELET COUNT FOR PLT CLUMPS', 'PLT, BLUE TOP*', 'PLATELETS (LABCORP)', 'CIT PLATELET', 'ELD PLT', 
'PLATELETS (096925)', 'PLATELETS', 'CITRATED PLT', 'PLTS (LC) -', 'PLATELET (CITRATED)', 'PLATELET COUNT (CITRATE)', 'PLATELET COUNT-BLUE TOP', 'PLT (LABCORP)', 
'SALEM PLATELETS-PB', 'PLT-BTT', '*PLT COUNT', 'HATT-PLT', '(FFTH) PLT', 'Sp.Pl.(Blue)', 'CITRATE PLATELET COUNT', '(STRONG) PLT', 'CITRATED PLATELET COUNT', 
'PLT-ACL', '_PLT (UW)', '_PLT-BTT (LOW PLT ONLY)', 'MH PLATELET COUNT', 'LEG PLT', 'PLATELET BLUE', 'PLT (CDH)', 'PLATELETS(LABCORP)', 'TAMC PLT', 
'PLT (NMMC)')


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Platelets2014_2017
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
     WHERE loincsid=-1 and     
      a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'


/*get unique PatientICN*/
select a.*, b.PatientICN
into dflt.Platelets2014_2017
from #Platelets2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management