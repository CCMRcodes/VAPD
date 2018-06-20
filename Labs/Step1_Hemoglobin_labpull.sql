/*Below SQL code will pull Hemoglobin labs from CDW*/
/*Step 2 will download the saved Hemoglobin lab pull table into SAS dataset for further cleaning*/

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('20509-6', '24360-0', '30313-1', '30350-3', '30351-1', '30352-9', '35183-3', '59260-0', '718-7', '14775-1', '30353-7', 
'30354-5', '33025-8', '33026-6', '48725-6', '54289-4', '55782-7', '61180-6', '75928-2', '76768-1', '76769-9')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('HGB', 'HGB (V2)', 'Hemoglobin', 'HGB3', 'HEMOGLOBIN-----------', 'HGB (AA)', 'HGB (HB)', 'NEW HGB', 'HGB,Blood', 
'HGB (FV)', 'HB (HGB)', 'POC HGB', 'HGB*', 'HGB(5/25/17)', 'HEMOGLOBIN~disc 10/14', 'HGB (XN2000)', 'TOTAL HEMOGLOBIN (BGL4)', 'I-STAT HGB', 'GEM-THb (calculated)', 'HB (HGB)(GAS)', 
'iHEMOGLOBIN (new)', 'HEMOGLOBIN (POC)', 'AT- Hgb CALCU', 'POC-HEMOGLOBIN (HB)', 'HGB, ANC', 'iHEMOGLOBIN', 'HEMOGLOBIN, TOTAL (B-GAS)', 'HEMOGLOBIN (ABG)', 'ABG,ctHb', 
'ABG HGB', 'ANCILLARY HEMOGLOBIN', 'tHb (R)', 'HGB - (BLOOD GAS)', 'tHB..', 'POC HEMOGLOBIN (CALC.)', 'tHb', 'tHb (Arterial)', 'HGB(BMT)', 
'POC HEMOGLOBIN', 'TOTAL HGB-----------O', 'CTHB', 'TOTAL HGB', 'ISTAT HGB', 'BG HGB', 'HGB(LUFKIN)', 'HEMOGLOBIN (RT)', 'HGB (TOPC)', 
'HGB(s)', 'TOTAL HEMOGLOBIN', 'iSTAT HEMOGLOBIN', 'HGB (FS)', 'Hgb (BLOOD)', 'Hgb', 'HEMOGLOBIN-HEPARINIZED SYRINGE', 'FHHb', 'POC-HEMOGLOBIN', 'POC HGB ISTAT', 'ZZZtHb.', 
'HGB ABG (STL)', 'ISTAT-HGB #', 'ctHb (Hb)', 'GEM-THb (measured)','Hgb-iSTAT', 'HGB(KTY)', 'TOTAL HGB (SY)', 'tHb (RAPIDLAB)', 'WB HGB', 
'HGB(TMB)', 'tHb (Venous)', 'VBG,ctHb', 'THb   (BLOOD GAS)*ic', 'THB-POCART', 'HGB (MV)*INACT(1-1-15)', 'tHB', 'HB (tHb)', 'P-HGB', 'POC-HHB', 
'POC TOTAL HGB', 'CVICU-Hgb', 'Hgb calculated', 'HEMOGLOBIN (ISTAT)', 'I-STAT, HGB (STL-MA)','Hbcalc-i (hemoglobin)', 'HGB (iSTAT)', 'ISTAT HEMOGLOBIN', 
'HB (HGB)(AL)', 'I-STAT HEMOGLOBIN', 'ATS CTHB', 'HGB POC (BH/BU)', 'D-HgB', 'MN HGB', 'HEMOGLOBIN {i-STAT}', 'HGB(POC)', 'POC-tHb', 'tHb(GEM)', 'MVBG,ctHb', 
'THb', 'HB (RESP CARE)', 'I-HEMOGLOBIN', 'PB HGB', '.HGB (istat)', 'W-HEMOGLOBIN', 'tHb (PULM)', '_THB (OF ABG PANEL)', 'POC HgB', 'i-Hemoglobin', 
'iHGB', 'THB-POCVEN', 'HGB (HR)','ATS tHb', '_POC HGB', 'HGB-PIERRE','~TOTAL HEMOGLOBIN(THb)', 'BR-HGB', 'HGB ABG', 'HEMOGLOBIN (GAS)', 'tHb(dc"d 12/15/15)', 
'AT-HGB','HGB-COOX-Sea', 'Hgb (ATS)', 'RETICULOCYTE HEMOGLOBIN', '_HGB (I-STAT)', 'QUEST-HGB', 'REF-Hemoglobin', 'HGB {Reference Lab}', 'POC HEMOGLOBIN(CALCULATED)', 
'HGB_ANC', 'HGB-MMC', 'QUEST HEMOGLOBIN', 'LRL HEMOGLOBIN', '.HEMOGLOBIN(SL)', 'HEMOGLOBIN-QUEST(ELECTROPH)', 'HGB-SPL', 'tHGB-Tac', 'ELD HGB', 
'Anc Total Hgb', 'HEMOGLOBIN (096925)','HGB (LC) -', 'TOTAL HGB (POC)', 'HGB (LABCORP)', 'SALEM HGB-PB','HEMOGLOBIN (QUEST)', 'THB', 'TOT-Hb', 
'HATT-HGB', '(FFTH) HGB', 'HGB (HGB ELECTRO)', 'HGB---o', 'Hgb (Hgb Dis Panel)', 'HEMOGLOBIN, CALC (ISTAT)', 'HEMOGLOBIN2', 'HEMOGLOBIN POC', '(STRONG) HGB', 
'HEMOGLOBIN (ABG)*NE', 'Hb (ISTAT)', 'HGB (POCT)', '*POC TOTAL HEMOGLOBIN (WI)', 'HGB-ACL', 'tHb (HEMOGLOBIN-ABG)', 'MH HGB', 'LEG HGB', 
'HGB (CDH)','tHb(PA)', 'tHb(AO)', 'tHb(RA)', 'HEMOGLOBIN-iSTAT', 'HEMOGLOBIN(35489)', 'HGB(LABCORP)', 'TAMC HGB', 'HGB (NMMC)', 'spl hemoglobin', 'HEMOGLOBIN' )


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Hemoglobin2014_2017
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
into dflt.Hemoglobin2014_2017
from #Hemoglobin2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management