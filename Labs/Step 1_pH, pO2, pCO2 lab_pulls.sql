/*Below SQL code will pull pH, pO2 and pCO2 labs from CDW*/
/*Step 2 will download the saved pH, pO2 and pCO2 lab pull tables into SAS dataset for further cleaning*/

/* Date Modified: 8/20/2018
   Author: Shirley Wang */

use /*INSERT STUDY NAME*/
go


/********* pH Labs *********/
/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('11558-4', '19213-8', '2744-1', '2745-8', '2746-6', '2753-2', '33254-4', 
'39485-8', '39486-6', '49701-6', '50980-2', '34554-6')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('PH', 'PH AT PT. TEMP', 'pH', 'PH (BLD GAS)', 'POC-pH', 'pH (BGL4)', 'GEM-pH', 'POC PH', 'PH, BLOOD GASES', 'POC pH', 
'GEM-pH (Temp. corrected)', 'PH (BU/SY/AL)', 'PH AT PT. TEMP.', 'PH, BLOOD', 'PH (B-GAS)', 'PH*,ANC', 'PH (POC)', 'AT- pH', 'PH BLOOD GAS', 
'ABG,pH(T)', 'pH (T. corrected)', 'PH (BLOOD GAS)', 'pH (BG)', 'pH (GAS)', 'pH..', 'AT- pH(T)', 'PH--------------BLOOD', 'pH (Arterial)', 
'pH(T) (Arterial)', 'POC-PH', 'ANCILLARY PH', 'PH BLOOD', 'pH (IONIZED)', 'PH.', 'I-STAT PH', 'ISTAT PH', 'PH AT TEMP', 'ARTERIAL PH', 
'BG PH', 'PH (RESP CARE)', 'PH-ARTERIAL', 'PH(patient temp.)', 'PH (Blood Gases)', 'POC pH', 'Ph', '.MIXED VENOUS pH', 'pH(BG)', 'pH at patient temp', 
'POC PH AT PT TEMP', 'PH(ABG)', 'POC PH AT PAT TEMP', 'PH {ARTERIAL}', 'POC-pH BLOOD GAS', 'ZZZpH.', 'pH (T)[Pre 4/25/18]', 'I-STAT PH(VENOUS)', 
'ISTAT-PH', 'PH AT 37o', 'pH-----POC', 'iPH (new)', 'PH*ne', 'pH (RAPIDLAB)', 'PH (ARTERIAL BLOOD GAS pH)', 'pH(T) (RAPIDLAB)', 'pH (Venous)', 
'pH(T) (Venous)', 'VBG,pH(T)', 'PH-POCART', '.PH(ARTERIAL)','POC pH (ABG)', 'pH (VEN)', 'iPH', 'pH(T)', 'PH    (BLOOD GAS)*ic', 'PH (ARTERIAL)', 
'pH (TEMP. CORRECTED)', 'BLOOD GAS pH', 'PH (BLD GAS)(DCed 2.26.18', 'PH_', 'pH (ART)', 'PH at Pt. Temp.', 'PH*IA', 'pH-i', 'I-STAT, pH (STL-MA)', 
'I-STAT, PH AT PT TEMP (STL-MA)', 'POC-pHT', 'CVICU-pH', 'PH (O.R.)','pH i-STAT', 'pH AT PT. TEMP', 'ATS PH (GAS)', 'PH*', 'pH (POC)', 'PH-TEMP CORRECT', 
'BLOOD PH', 'PH POC (BU)', 'PH(T)', 'pH(POC)', 'PH ARTERIAL', 'pH-ARTERIAL {i-STAT}', '.VpH', 'pH ABG', 'PHT (RESP CARE)', 'pH-i (T. Correct)', 
'pH(GEM)','PH (CHEM)', 'MVBG,pH(T)', 'pH(ABG)', 'pH at Pt Temp(GEM)', 'PH(BLOOD GAS)', 'PH ABG', 'POC-PH OR', 'iSTAT pH', 'PH(IONIZED CALCIUM)', '.PH (istat)', 
'PH-ART', 'PH OXYRECT', 'PH-MIXED VENOUS', 'pH (PULM)', 'pH (B.GAS)','i-pH', 'PH - EPOC', 'PH-POCVEN', 'pH (ISTAT)', 'PH@TEMP (POC)', 'PH AT PT.TEMP POC (BU)', 
'ABG pH', 'pH (Temp Corrected)', 'GEM-PH BLOOD', 'I-STAT PH(ARTERIAL)', 'I-PH', 'PH(TEMP CORRECTED)', 'ATS PH AT PT. TEMP', 'pH-ARTERIAL', 
'pH BLOOD', 'I-STAT MVBG PH', 'PH Temp. Corr. I-STAT', 'PH I-STAT', 'pH (ATS)', 'UDS PH TEST', 'PH DETERMINATION', 'POC-ISTAT ABG PH', 'pH-iSTAT', 
'VENOUS PH','PH-Tac', 'pHv', 'PH (BH)','pH (iSTAT)', 'PH ISTAT', 'pH screening', 'PH(venous)I-STAT', 'ABG(PH)','pH_ANC', 'PH(RESP)', 'pH (TEMP CORRECTED) (POC)', 
'PH,VENOUS', 'MIXED VENOUS BG PH', '_POC ABG pH')


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #pH2014_2017
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
into dflt.pH2014_2017
from #pH2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management


/***************************************************************************************************************************************/
/********* pO2 Labs *********/
/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('19218-7', '19254-2', '19255-9','19256-7', '19980-2', '19981-0', '19982-8', '19983-6', '19997-6', '19998-4', 
'19999-2', '20000-6', '20001-4', '20002-2','20003-0', '20004-8', '20005-5', '20006-3', '20007-1', '20008-9', '20009-7', '20010-5', 
'20011-3', '20012-1', '20013-9', '20014-7', '20015-4', '20016-2', '20017-0', '20018-8', '20019-6', '20020-4', '20021-2', '20022-0', 
'20023-8', '20024-6', '20025-3', '20026-1', '20027-9', '20028-7', '20029-5', '20030-3', '59274-1', '64799-0', '11556-8', '10232-7', '10233-5', 
'10238-4', '19219-5', '20031-1', '2703-7','2704-5', '33437-5', '34163-6', '57800-5')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('PO2', 'PO2 AT PT. TEMP', 'pO2','POC-pO2', 'PO2 (POC)', 'PARTIAL PRESSURE OF OXYGEN (pO2)', 'GEM-PO2', 
'pO2 (BGL4)','GEM-PO2 (Temp. corrected)', 'POC PO2','PO2, ANC', 'AT- PO2', 'O2CT.', 'POC pO2', 'O2 CT', 'ABG,ctO2', 'ABG,pO2(T)', 'POC-PO2', 
'AT- PO2(T)', 'pO2..', 'pO2(T) (Arterial)', 'pO2 (Arterial)', 'PO2 (BU/SY)', 'PaO2', 'ANCILLARY PO2', 'tO2c', 'pO2 (T. corrected)', 
'pO2 (R)', 'I-STAT PO2', 'ISTAT PO2', 'PO2 @ 37', 'BG PO2', 'PO2-----------------O', 'PO2 (RESP CARE)', 'PO2-Sea', 'PO2-ARTERIAL', 'PO2(patient temp.)', 
'POC PO2', 'POC PO2 AT PT TEMP', 'PO2(arterial)', 'PO2(T)', 'POC PO2 AT PAT TEMP', 'ZZZpO2.', 'PO2 at patient temp', 'ISTAT-PO2', 
'ctO2 (O2Ct)', 'POC-PO2 BLOOD GAS', 'pO2--POC', 'iPO2 (new)', 'O2CT', 'PO2*ne', 'PO2 (Effective 021302)', 'p02 (RAPIDLAB)', 'pO2(T) (RAPIDLAB)', 
'PO2-POCART', '.PO2', 'O2CT (Pre-3/30/18)', 'PO2 (AL)', 'POC pO2 (ABG)', 'PO2 (Pre 4/25/18)', 'pO2 (TEMP. CORRECTED)', 'PO2(DCed 2.26.18', 
'pO2 (ART)', 'iPO2', 'POC PO2 @ 37', 'PO2*ci', 'I-STAT, pO2 (STL-MA)', 'POC-PO2T', 'CVICU-PaO2', 'PO2   (BLOOD GAS)*ic', 'PO2 i-STAT', 'ATS PO2', 
'ATS PO2 AT PT. TEMP', 'pO2-i', 'PO2*', 'PO2-TEMP CORRECT', 'PO2 (O.R.)', 'I-STAT, PO2 AT PT TEMP (STL-MA)', 'PO2 at Pt. Temp.', 'PO2 - ARTERIAL {i-STAT}', 
'pO2 AT PT. TEMP', 'PO2T (RESP CARE)', 'pO2(POC)', 'pO2(GEM)', 'pO2 at Pt Temp(GEM)', 'POC-PO2 OR', 'pO2-i (T. Correct)', 'pO2(T)', 
'.PO2 (istat)', 'PO2 OXYRECT', 'O2CT OXYRECT', 'pO2 (PULM)', '_PO2 (OF ABG PANEL)', 'i-PO2', 'PO2 POC (BU)', 'PO2@TEMP (POC)', 'PO2 - EPOC', 
'P O2', 'GEM-sO2c (calculated)', 'PO2 AT PT.TEMP POC (BU)', 'PO2 (Temp Corrected)', 'BO2(O2Cap)', 'I-PO2', 'Po2', 'PO2 Temp. Corr. I-STAT', 
'PO2(TEMP CORRECTED)', 'POC-ISTAT ABG PO2', 'PO2 (ATS)', 'PO2-Tac', 'PO2v', 'PO2 (BH)', 'PO2 (iSTAT)', 'PO2-iSTAT', 'PO2 ISTAT', 'PO2 (ISTAT)', 
'iSTAT pO2', 'pO2_ANC', 'PO2 (TEMP CORRECTED) (POC)', 'POC-PO2(T)', '_POC ABG pO2')


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #pO22014_2017
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
     WHERE  a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'


/*get unique PatientICN*/
select a.*, b.PatientICN
into dflt.pO22014_2017
from #pO22014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management

/***************************************************************************************************************************************/
/********* pCO2 Labs *********/
/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('19212-0', '19223-7', '19878-8', '19879-6', '19880-4','19881-2', '19892-9', '19893-7', '19894-5', '19895-2', '19896-0', 
'19897-8', '19898-6', '19899-4', '19900-0', '19901-8', '19902-6', '19903-4', '19904-2', '19905-9', '19906-7', '19907-5', '19908-3', '19909-1', 
'2019-8', '2020-6', '2021-4', '2024-8', '2026-3', '2027-1',  '32771-8', '33022-5', '34705-4', '34728-6', '40619-9', '40620-7', 
'41647-9', '50981-0', '51781-3', '57920-1', '57922-7', '76184-1', '77143-6', '11557-6', '48391-7', '49694-3', '49695-0', '16551-4', '74684-2')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('PCO2', 'PCO2 AT PT. TEMP', 'pCO2', 'POC-pCO2', 'pCO2 (BGL4)', 'PCO2 (POC)', 'GEM-PCO2', 'PARTIAL PRESSURE CARBON DIOXIDE (pCO2)', 
'POC PCO2', 'GEM-PCO2 (Temp. Corrected)', 'POC-PCO2', 'PCO2,ANC', 'POC pCO2', 'AT- PCO2', 'ABG,pCO2(T)', 'pCO2 (T. corrected)', 'PaCO2', 
'pCO2 (R)', 'pCO2..', 'PCO2 (SY)', 'AT- PCO2(T)', 'pCO2 (Arterial)', 'pCO2(T) (Arterial)', 'ANCILLARY PCO2', 'PCO2-Sea', 'PCO2----------------O', 
'I-STAT PCO2', 'PCO2 @ 37', 'ISTAT PCO2', 'BG PCO2', 'POC TCO2 (MEASURED)', 'PCO2 (RESP CARE)', 'PCO2-ARTERIAL', 'PCO2(patient temp.)', 
'POC PCO2', 'iSTAT TCO2', '.MIXED VENOUS pCO2', 'PCO2 at patient temp', 'POC PCO2 AT PT TEMP', 'PCO2(T)', 'POC-PCO2 BLOOD GAS', 'ZZZpCO2.', 
'CO2_KTY', 'PCO2 (Pre 4/25/18)', 'ISTAT-PCO2', 'I-STAT PCO2(VENOUS)', 'pCO2----POC', 'iPCO2 (new)', 'PCO2 (BU)', 'PCO2*ne', 'pCO2 (RAPIDLAB)', 
'pCO2(T) (RAPIDLAB)', 'PCO2-POCART', '.PCO2', 'POC pCO2 (ABG)', 'pCO2 (VEN)','P-CO2(I)', 'PCO2 (AL)', 'pCO2(T)', 'PCO2  (BLOOD GAS)*ic', 
'iPCO2', 'pCO2 (TEMP. CORRECTED)', 'PCO2(DCed 2.26.18', 'pCO2 (ART)', 'PCO2 at Pt. Temp.', 'PCO2*ci', 'POC PCO2 @ 37', 'I-STAT, pCO2 (STL-MA)', 
'pCO2-i', 'CVICU-PaCO2', 'PCO2 (O.R.)', 'POC-PCO2T', 'PCO2 i-STAT', 'pCO2 AT PT. TEMP', 'ATS PCO2 AT PT. TEMP', 'ATS PCO2', 'PCO2*', 
'PCO2-TEMP CORRECT', 'PCO2 POC (BU)', 'pCO2(POC)', 'PCO2 - ARTERIAL {i-STAT}', '.VpCO2', 'PCO2T (RESP CARE)', 'pCO2(GEM)', 'pCO2-i (T. Correct)', 
'MVBG,pCO2(T)', 'pCO2 at Pt Temp(GEM)', 'POC-PCO2 OR', 'iSTAT pCO2', '.PCO2 (istat)','PCO2 OXYRECT', 'PCO2-MIXED VENOUS', 'pCO2 (PULM)', 
'_PCO2 (OF ABG PANEL)', 'i-PCO2','PCO2-POCVEN', 'PCO2 - EPOC', 'PCO2@TEMP (POC)', 'PCO2 (ISTAT)', 'PCO2 AT PT.TEMP POC (BU)', 'P CO2', 
'Pco2', 'POC PCO2 AT PT. TEMP', 'PCO2(Temp Corrected)', 'I-PCO2','PCO2(TEMP CORRECTED)', 'I-STAT MVBG PCO2', 'PCO2 Temp. Corr. I-STAT', 
'PCO2 (ATS)', 'POC-ISTAT ABG PCO2', 'PCO2-iSTAT', 'PCO2-Tac','PCO2v', 'PvCO2', 'PCO2 (BH)', 'PCO2 (iSTAT)','PCO2 ISTAT', 'PCO2(venous)I-STAT', 
'pCO2_ANC','PCO2 (TEMP CORRECTED) (POC)', 'POC-PCO2(T)', 'MIXED VENOUS BG PCO2', '_POC ABG pCO2', 'POC PCO2 AT PAT TEMP', 
'pCO2 (Venous)', 'pCO2(T) (Venous)', 'VBG,pCO2(T)', 'I-STAT, PCO2 AT PT TEMP (STL-MA)' )


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #pCO22014_2017
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
     WHERE  a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'


/*get unique PatientICN*/
select a.*, b.PatientICN
into dflt.pCO22014_2017
from #pCO22014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management