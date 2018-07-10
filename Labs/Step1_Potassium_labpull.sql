/*Below SQL code will pull Potassium labs from CDW*/
/*Step 2 will download the saved Potassium lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 6/16/2018
   Author: Shirley Wang */

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('22760-3', '2823-3', '2824-1', '29349-8', '39789-3', '39790-1', '41656-0', '51618-7', '75940-7', 
'77142-8', '82722-0', '12812-4', '12813-2', '32713-0', '42569-4', '6298-4', '86919-8')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('POTASSIUM (Dialysis)', 'POTASSIUM', 'POTASSIUM (V2)', 'POTASSIUM*', 'POTASSIUM-----------O', 'Potassium', 
'POTASSIUM*IA', 'POTASSIUM (Serum/Plasma)', 'POTASSIUM*NE', 'POTASSIUM (FV)', 'POTASSIUM,Blood', 'POTASSIUM (DCed 2.1.15', 'POTASSIUM (CX)', 
'POTASS.', 'ZPOTASSIUM', 'POC POTASSIUM', 'POTASSIUM(D/C 6/7/17)', 'POTASSIUM -SERUM THRU 3/31/18', 'POTASSIUM, SERUM', 'POTASSIUM(serum/plasma)', 
'GAS-K+(SICU)', 'POTASSIUM(AX)', 'GEM-potassium', 'I-STAT K', 'POTASSIUM, PLASMA', 'iPOTASSIUM (new)', 'AT- POTASSIUM', 'POTASSIUM (POC)', 
'POC-POTASSIUM (K)', 'POTASSIUM, ANC', 'iPOTASSIUM', 'POTASSIUM (BLOOD)', 'K+(WB)(CH)', 'Potassium..', 'POTASSIUM (Arterial)', 'POTASSIUM(BMT)', 
'ABG,POTASSIUM','ISTAT POTASSIUM', 'POTASSIUM(LUF)', 'POTASSIUM-GAS', 'POTASSIUM-WHOLE BLOOD', 'POTASSIUM (B-GAS)', 'POTASSIUM (FS)', 'POC-K+', 
'BG POTASSIUM', 'iSTAT POTASSIUM', 'ANCILLARY POTASSIUM', 'K+ (BLOOD)', 'POC-POTASSIUM', 'POC K ISTAT', 'ZZZPotassium.', 'POTASSIUM_KTY', 'ISTAT-POTASSIUM', 
'K+---POC', 'POC-K BLOOD GAS', 'K+-POC', 'K-iSTAT', 'P-POTASSIUM', 'K ABG (STL)', 'POTASSIUM(TMB)', 'GAS-K+', 'WB POTASSIUM', 'POTASSIUM (RAPIDLAB)', 'POTASSIUM (Venous)', 
'POTASSIUM (MV)*INACT(1-1-15)', 'P-POTASSIUM(I)', 'K (POC)', 'POTASSIUM (ISTAT)', 'CVICU-POTASSIUM', 'I-STAT, K (STL-MA)', 'K-i (Potassium)', 'D-POTASSIUM', 'POTASSIUM-POC*ic', 'K (iSTAT)', 
'POTASSIUM (O.R.)', 'K', 'I-STAT POTASSIUM', 'ATS K+','K(POC)', 'POTASSIUM POC (BU/BH)', 'MN POTASSIUM', 'POTASSIUM (PLASMA)', 'POTASSIUM {i-STAT} - Arterial', 
'POTASSIUM (Dialysis)(D/C 6/7/17)', 'PB POTASSIUM', 'POTASSIUM-ARTERIAL BLOOD', 'K+(GEM)', 'W-POTASSIUM', 'POC K', 'K+r', 'POC-POTASSIUM OR', 
'I-POTASSIUM', '.K (istat)', '_K (OF ABG PANEL)', 'POTASSIUM (DIALYSIS ONLY)', 'WHOLE BLOOD K+', '_POC K', 'i-Potassium', 'BR-POTASSIUM', 
'K+ (Blood Gas)', 'POTASSIUM,ARTERIAL', 'K+ (Gas)', 'POTASSIUM, (WORC)', 'Potassium (ATS)', '_POTASSIUM (I-STAT)', 'ELD POTASSIUM', 'MMC POTASSIUM', 
'Potassium(BG)', 'POTASSIUM-POC', 'POTASSIUM,SERUM(LABCORP)', 'LRL POTASSIUM', 'POTASSIUM,ISTAT', 'SALEM POTASSIUM  (PB)', 'HATT-POTASSIUM', 
'POTASSIUM (Plasma)', 'POTASSIUM POC', 'POTASSIUM (Ref.Lab)', 'POTASSIUM, WHOLE BLOOD', 'AT-POTASSIUM', 'SICU POTASSIUM', 'POTASSIUM,PLASMA', 'MH POTASSIUM', 
'LEG POTASSIUM', 'K+ (BLOOD GAS)', 'istatK', 'POTASSIUM (BLD GAS)* (BU/SY)', 'POTASSIUM-iSTAT', 'POC-ISTAT 6+ K', 'POTASSIUM,SENDOUT CDH' )


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Potassium2014_2017
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
into dflt.Potassium2014_2017
from #Potassium2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management