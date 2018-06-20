/*Below SQL code will pull Hematocrit labs from CDW*/
/*Step 2 will download the saved Hematocrit lab pull table into SAS dataset for further cleaning*/

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('20570-8', '24360-0', '31100-1', '32354-3', '41654-5', '41655-2', '42908-4', '4544-3', '4545-0', '48703-3', 
'62241-5', '71829-6', '71830-4', '71831-2', '71832-0', '71833-8')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('HCT', 'HEMATOCRIT', 'HCT (V2)','Hematocrit', 'HCT3', 'HEMATOCRIT----------O', 'NEW HCT', 'HCT,Blood', 'HCT (FV)', 
'POC HCT', 'HCT(D/C 5/25/17)', 'HEMATOCRIT~disc 10/14', 'HCT (XN2000)','GAS-HCT(SICU)', 'I-STAT HCT', 'POC HEMATOCRIT', 'GEM-Hct (measured)', 
'iHEMATOCRIT (new)', 'AT- HCT', 'POC-HEMATOCRIT (HCT)', 'HEMATOCRIT (POC)', 'HCT, ANC', 'HCT (B-GAS CALC)', 'ABG HCT', 'ANCILLARY HEMATOCRIT', 
'iHCT', 'HCT-COOX-Sea', 'HCT(BMT)', 'HCT-GAS', 'ISTAT HCT', 'POC-Hct', 'HCT(LUFKIN)', 'Hct', 'iHEMATOCRIT', 'HCT (TOPC)', 'HCT(s)', 'HCT (FS)', 
'Hct (BLOOD)', 'HCT (GAS)', 'POC HCT ISTAT','CALCULATED HEMATOCRIT', 'HCT(BG)', 'ISTAT-HCT', 'HCT - (BLOOD GAS)', 'Hct--POC', 
'HCT-POC', 'GEM-Hct(calculated)', 'Hct-iSTAT', 'HCT(KTY)', 'HCT ABG (STL)', 'GAS-HCT', 'WB HCT', 'HCT(TMB)', 'POC-HEMATOCRIT', 'HCT (MV)*INACT(1-1-15)', 
'P-HCT', 'HCT (POC)', 'HEMATOCRIT (ISTAT)', 'CVICU-HCT', 'I-STAT, HCT (STL-MA)', 'Hematocrit-i', 'HEMATOCRIT-POC*ic', 'iSTAT HEMATOCRIT', 
'HCT (iSTAT)', 'ISTAT HEMATOCRIT', 'ATS HCT (GAS)', 'I-STAT HEMATOCRIT', 'HCT (O.R.)', 'HCT POC (BH/BU)', 'HCT(POC)', 'D-HCT', 'MN HCT', 
'HCT {i-STAT}', 'POC-HEMATOCRIT OR', 'I-HEMATOCRIT', 'PB HCT', '.HCT (istat)', 'W-HEMATOCRIT', 'Hct.', 'i-Hematocrit', 'HEMATOCRIT-Q', 'hct, lca', 
'HCT (LC)', 'HCT (HR)', '_POC HCT', 'Hct (Quest)','HCT-PIERRE', 'Patient HCT', 'BR-HCT', 'HCT (Blood Gases)', 'spun HCT(micro hct)', 'AT-HCT', 'Hct (ATS)', 'HEMATOCRIT-QUEST', 
'_HCT (I-STAT)', 'QUEST-HCT','REF-Hematocrit', 'HCT {Reference Lab}', 'Hct, Red Blood Cell', 'Hct(BG)', 'HCT-MMC', 'QUEST HEMATOCRIT', 'LRL HEMATOCRIT', 
'.HEMATOCRIT(SL)', 'HEMATOCRIT (LABCORP)', 'HCT-SPL', 'ELD HCT', 'HEMATOCRIT (096925)', 'HCT (LC) -', '.HCT', 'HCT (LABCORP)', 'SALEM HCT-PB', 
'HCT, RBC.', 'HATT-HCT', '(FFTH) HCT', 'HCT---o', 'HCT (Hgb Dis Pan)', 'HEMATOCRIT POC', 'HCT (HGB ELECTRO)', '(STRONG) HCT', 'Hct (ISTAT)', 
'MICROHEMATOCRIT', 'HEMATOCRIT (QUEST)', '.HEMATOCRIT (lc)', 'HCT-ACL', '_HCT (UW)', 'MH HCT', 'LEG HCT', 'HEMATOCRIT2', 'SPUN HCT', 
'HCT (CDH)', 'HCT(LABCORP)', 'REF-HCT', 'HEMATOCRIT-iSTAT','HEMATOCRIT (LC)', 'TAMC HCT', 'HCT (NMMC)', 'spl hematocrit' )


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Hematocrit2014_2017
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
into dflt.Hematocrit2014_2017
from #Hematocrit2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management