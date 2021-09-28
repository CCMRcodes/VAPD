/*Below SQL code will pull Sodium labs from CDW*/
/*Step 2 will download the saved Sodium lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 10/11/2018
   Author: Shirley Wang */

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('2947-0', '2951-2', '32717-1', '39791-9', '39792-7', '41657-8', '42570-2', '74688-3', '77139-4', '82812-9', '44783-9')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('SODIUM', 'SODIUM (V2)','SODIUM*', 'Sodium', 'SODIUM--------------O', 'SODIUM*IA', 'SODIUM (SERUM/PLASMA)', 
'SODIUM (Serum/Plasma)', 'SODIUM*NE','SODIUM (FV)', 'SODIUM,Blood', 'POC SODIUM', 'SODIUM (DCed 2.1.15', 
'SODIUM (CX)', 'SODIUM (LAB USE ONLY)', 'ZSODIUM', 'SODIUM(D/C 6/7/17)', 'SODIUM, SERUM', 'SODIUM -SERUM THRU 3/31/18', 'SODIUM,(plasma/serum)', 
'GAS-NA+(SICU)', 'GEM-Sodium', 'I-STAT NA', 'SODIUM, PLASMA', 'iSODIUM (new)', 'AT- SODIUM','SODIUM (POC)', 'POC-SODIUM (NA)', 
'SODIUM, ANC', 'iSODIUM', 'SODIUM (BLOOD)', 'Na+(WB) (R)', 'Sodium..', 'SODIUM (Arterial)', 'SODIUM(BMT)', 'ISTAT SODIUM', 'SODIUM-GAS', 'SODIUM-WHOLE BLOOD---', 
'SODIUM(LUF)', 'SODIUM (B-GAS)', 'POC-Na+','SODIUM (FS)', 'BG SODIUM', 'iSTAT SODIUM', 'ANCILLARY SODIUM', 'Na+ (BLOOD)', 'POC-SODIUM', 'POC SODIUM ISTAT', 'ZZZSodium.', 
'SODIUM_KTY', 'ISTAT-SODIUM', 'Na+---POC', 'POC-NA BLOOD GAS', 'Na-iSTAT', 'NA ABG (STL)', 'SODIUM(TMB)', 'GAS-NA+', 'WB SODIUM', 
'SODIUM (RAPIDLAB)', 'NA+-POC', 'SODIUM (Venous)', 'SODIUM (MV)*INACT(1-1-15)', 'ABG,SODIUM', 'P-SODIUM(I)', 'NA (POC)', 'SODIUM (ISTAT)', 
'CVICU-SODIUM', 'I-STAT, NA (STL-MA)', 'Na-i (Sodium)', 'D-SODIUM', 'SODIUM-POC*ic', 'NA (iSTAT)', 'SODIUM (O.R.)', 
'Na', 'I-STAT SODIUM', 'ATS NA+', 'MN SODIUM','SODIUM POC (BU/BH)', 'Na(POC)', 'SODIUM {i-STAT} - Arterial', 'PB SODIUM', 'SODIUM-ARTERIAL BLOOD', 
'Na+(GEM)','W-SODIUM', 'POC NA', 'NA+r', 'POC-SODIUM OR', 'I-SODIUM', '.NA (istat)', '_NA (OF ABG PANEL)', 'NA+-POCVEN', 
'WHOLE BLOOD SODIUM', '_POC NA', 'i-Sodium', 'BR-SODIUM', 'SODIUM (DIALYSIS ONLY)', 'Na (Blood Gas)', 'SODIUM,ARTERIAL', 
'Na+ (Gas)', 'Sodium (ATS)', '_SODIUM (I-STAT)', 'NA(POC)', 'SODIUM, (WORC)', 'ELD SODIUM', 
'MMC SODIUM', 'SODIUM-POC', 'Sodium(BG)', 'SODIUM,SERUM(LABCORP)', 'LRL SODIUM', 'SODIUM,ISTAT', 'SALEM SODIUM  (PB)', 'SODIUM POC', 'HATT-SODIUM', 
'AT-SODIUM', 'SICU SODIUM', 'MH SODIUM', 'Na+ (BLOOD GAS)', 'LEG SODIUM', 'SODIUM,PLASMA', 'iNA', 'SODIUM (BLD GAS)* (BU/SY)', 'SODIUM-iSTAT', 'SODIUM,SENDOUT CDH' )



/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT distinct a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Sodium2014_2017
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
     WHERE /* loincsid=-1 and  */   
      a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'  and a.CohortName='Cohort20210503' 


/*get unique PatientICN*/
select distinct a.*, b.PatientICN
into dflt.Sodium2014_2017
from #Sodium2014_2017 a
left join Src.SPatient_SPatient b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management

drop table dflt.Sodium2014_2017
