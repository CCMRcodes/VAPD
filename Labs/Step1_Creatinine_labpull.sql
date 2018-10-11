/*Below SQL code will pull Creatinine labs from CDW*/
/*Step 2 will download the saved Creatinine lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 10/11/2018
   Author: Shirley Wang */

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('2160-0','44784-7','35203-9','14682-9','38483-4','21232-4','59826-8','77140-2')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('CREATININE', 'CREATININE,SERUM', 'CREATININE, SERUM', '*CREATININE', 'CREATININE (V2)', 'Creatinine', 'CREATININE*', 
'CREATININE idms', 'CREATININE----------O', 'CREATININE-e', 'CREATININE2', 'CREATININE*IA', 'CREATININE(DOES NOT INCLUDE EGFR)', 'CREATININE, SERUM OR PLASMA', 
'CREATININE (DOES NOT CONTAIN eGFR)', 'Creatinine Serum Result', 'CREATININE,SERUM/PLASMA', 'CREATININE*NE', 'CREATININE (SERUM/PLASMA)', 'CREATININE (FV)', 'CREATININE,blood', 
'CREATININE,PLASMA (in mg/dL)', 'CREATININE (DCed 2.1.15', 'CREATININE (CX)', 'CREAT.', 'CREATININE,DC 1/14/16', 'ZCREATININE', '_CREATININE (OF eGFR PANEL)', 
'POC CREATININE', 'EGFR-CREATININE', 'CREATININE(D/C 6/7/17)', 'CREATININE-SERUM', 'CREATININE SER -', 'CREATININE (BLOOD)', 'CREATININE (SERUM)', 
'CREATININE(serum/plasma)', 'I-STAT CREA', 'CREATININE, PLASMA', 'iCREATININE', 'CREATININE, Serum', 'POC-CREATININE', 'CREATININE, BLOOD', 'CREATININE (WB)(R)', 
'CREATININE (POC)', 'CREATININE(BMT)', 'iCreatinine', 'CREATININE(LUF)', 'CREATININE (FS)', 'iSTAT CREATININE', 'ISTAT CREATININE', 'POCT CREATININE', 
'CREATININE (BG)', 'I-CREATININE', 'POC CREATININE ISTAT', 'creatinine', 'CREATININE_KTY', 'AT- CREATININE', 'WB CREATININE', 'ATS CREATININE', 'Ancillary Creatinine', 'CREATININE (ISTAT)', 
'ANCILLARY I-STAT CREATININE', 'Creatinine-iSTAT', 'CREATININE(TMB)', 'I-STAT, CREAT (STL-MA)', 'CREATININE (MV)*INACT(1-1-15)', 'P-CREATININE(I)', 'ANCILLARY CREATININE', 
'CREATININE, ANC', 'CREATININE-POC', 'CREATININE, ANCILLARY', 'D-CREATININE', 'CREATININE-POC*IC', 'CREAT (iSTAT)', 'PB CREATININE', '*POC-CREATININE', 'MN CREATININE, SERUM', 
'I-STAT CREATININE', 'I-STAT CREAT', 'CREATININE(FOR CT STUDIES ONLY)', 'CREATININE (SEND OUT ONLY)', 'CREATININE ENZYMATIC', 'CREAT (dialysis)(D/C 6/7/17)', 
'W-CREATININE', 'POC CREAT', 'CREATININE (eGFR)', 'CREATININE {i-STAT}', 'POC - CREAT', 'iCREAT', 'SERUM CREATININE', '_POC CREAT', 'SERUM CREATININE VALUE', 
'CREAT-ISTAT', 'DELTA EGFR', 'CREATININE (DIALYSIS ONLY)', 'BR-CREATININE', 'CT CREATININE', 'Creatinine-i', 'POC CREATININE (ISTAT)', '_CREATININE (I-STAT)', 
'CREATININE, (WOPC)', 'CREATININE POC (BU/BH)', 'CREATININE (ATS)', 'SERUM CREATININE', 'CREATININE  (sendout)', 'ELD CREATININE', 'CREATININE(POC)', 'MMC CREATININE', 
'PLASMA CREATININE (CrCl)', 'eCREAT', 'CREATININE,SERUM(LABCORP)', 'CREATININE SERUM', 'CREAT (dialysis)', 'CT CONTRAST CREATININE (DC"D 2/15)', 'LRL CREATININE', 
'CREATININE GFR', 'STAT CREATININE', 'SALEM CREATININE  (PB)', 'I-STAT CREATININE (I-STAT)', 'SEND OUT CREATININE', 'PL CREAT (RAW)', 'HATT-CREATININE', 
'CREATININE POC', 'CREATININE (Ref.Lab)', 'POC CREATININE (POC)', 'SERUM CREAT.(CL.)', 'MH CREAT, SER, mg/dL', 'LEG CREATININE', '_CREAT (SER OF CLEAR PNL)', 'CREATININE(Q)', 
'SERUM CREAT (FOR CLEARANCE)', 'Creatinine', 'CREATININE, (S/O)', 'CREATININE-iSTAT', 'i-Creatinine', 'AT-CREATININE', 'CREATININE I-STAT')


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Creatinine2014_2017
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
     WHERE /*loincsid=-1 and    */ 
      a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'


/*get unique PatientICN*/
select a.*, b.PatientICN
into dflt.Creatinine2014_2017
from #Creatinine2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management