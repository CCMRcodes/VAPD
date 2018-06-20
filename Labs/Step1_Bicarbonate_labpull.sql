/*Below SQL code will pull Bicarbonate labs from CDW*/
/*Step 2 will download the saved Bicarbonate lab pull table into SAS dataset for further cleaning*/

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('14627-4', '19229-4', '19230-2', '19231-0', '19232-8', '19233-6', '1959-6', '1960-4', '1961-2', '1962-0', 
'1963-8', '28640-1', '28641-9', '39459-3', '39460-1', '48631-6', '54359-5', '69964-5' )

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('HCO3', 'BICARBONATE', 'BICARBONATE (SBC)', 'POC-HCO3', 'BICARBONATE (HCO3)', 'HCO3(SICU)', 'GEM-HCO3-(c)', 'BICARBONATE, CALCULATED (HCO3)', 
'HCO3 (CALC) (POC)', 'POC HCO3 (CALC.)', 'HCO3, ANC', 'HCO3-(c)', 'AT- HCO3 CALCU', 'HCO3 (calculated)', 'ABG,cHCO3', 'POC HCO3', 'HCO3-', 
'HCO3c (R)', 'HCO3..', 'BICARBONATE, CALC.', 'BICARBONATE* (BU)', 'HCO3a (Arterial)', 'ANCILLARY HCO3', 'BICARBONATE (ACT)', 'BICARBONATE-Sea', 
'HC03', 'BICARB. (HCO3)', 'HCO3----------------O', 'I-STAT HCO3', 'ISTAT HCO3', 'aHCO3', 'BG HCO3', 'HCO3 (RESP CARE)', 
'POC HCO3 (CALCULATED)', 'HCO3-ARTERIAL', 'iHCO3', 'POC BICARB', 'POC HCO3', '.MIXED VENOUS BICARB', 'CALC BICARBONATE', 'HCO3 (cHO3(P)c)', 
'Bicarbonate (SBC)', 'ZZZHCO3.', 'POC-cHCO3 BLOOD GAS', 'ISTAT-HCO3 #', 'HCO3--POC', 'BICARBONATE*NE', 'BICARBONATE (SY)', 'ACTUAL BICARB', 'HCO3a (RAPIDLAB)', 
'HCO3-BGL', 'HCO3a (Venous)', 'HCO3-ACT-POCART', '.BICARBONATE', 'BICARBONATE (BH/CN/AL)', 'POC BICARBONATE', 'HCO3 (VEN)', 'POC-BICARBONATE', 
'HCO3  (BLOOD GAS)*ic', 'HCO3c', 'HCO3(DCed 2.26.18', 'HCO3 (ART)', 'HCO3 (POC)', 'HCO3*ci', 'I-STAT, HCO3 (STL-MA)', 'HCO3calc-i', 'CVICU-BICARBONATE', 
'HCO3- (O.R.)', 'HCO3(P)', 'HCO3 i-STAT', 'BICARBONATE (SBC)*', 'ATS HCO3', 'BICARBONATE POC (BU)', 'HCO3 (ACTUAL BICARB.)', 'HCO3(POC)', 
'HCO3 - ARTERIAL {i-STAT}', 'HCO3 std(GEM)', 'BICARBONATE (POC)', 'POC-BICARBONATE OR', '.HCO3 (istat)', 'BICARB (SBC) OXYRECT', 'BICARBONATE (std)', 
'HCO3-MIXED VENOUS', 'HCO3 (PULM)', '_HCO3 (OF ABG PANEL)', 'i-HCO3', 'HCO3a (POC)', 'HCO3-ACT-POCVEN', 'HCO3 - EPOC', 'HCO3c (ISTAT)', 'Hco3', 
'HCO3-act', 'GEM-BICARBONATE (CALC)', 'Bicarbonate', 'I-BICARBONATE', 'CO2', 'BICARBONATE (cHCO3)', 'BICARBONATE (ARTERIAL)', 'I-STAT MVBG HCO3', 
'HCO3 std', 'HCO3-(act)', 'HCO3 (ATS)', 'POC-ISTAT ABG HCO3', 'HCO3-iSTAT', 'vHCO3', 'BICARBONATE-Tac', 'HCO3v', 'BICARBONATE (iSTAT)', 'HCO3 ISTAT', 
'SALEM BICARBONATE (PB)', 'CALC HCO3_ANC', 'HCO3 (ISTAT)', 'HCO3C', 'HCO3ACT', 'BICARBONATE (BGL4)', 'iSTAT HCO3', 'MIXED VENOUS BG HCO3', '_POC ABG HCO3')


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Bicarbonate2014_2017
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
into dflt.Bicarbonate2014_2017
from #Bicarbonate2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management