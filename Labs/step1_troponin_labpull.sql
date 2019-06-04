/*Below SQL code will pull Troponin labs from CDW*/
/*Step 2 will download the saved Troponin lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 2/13/19
   Author: Sarah Seelye */

use ORD_Iwashyna_201108021D
go

/***** Troponin ******/

--pull in LOINCSIDs
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWORK].[Dim].[loinc]
where loinc in(	'10839-9', '16255-2', '42757-5', '49563-0', '76399-5', '89577-1', '89578-9', '89579-7',
				'33204-9', '48425-3', '48426-1', '6597-9', '6598-7', '67151-1', '89575-5', '89576-3')

select * from  #loinc
--(2080 rows affected)

--pull in labchemtestnames to send to PIs for review
SELECT a.LabChemTestSID, a.PatientSID, a.sta3n, a.LabChemSpecimenDateTime, a.LOINCSID,  c.labchemtestname 
into #labtestnames
FROM  [ORD_Iwashyna_201108021D].[Src].[Chem_PatientLabChem] AS A
INNER JOIN #loinc b on  a.Loincsid=b.Loincsid 
LEFT JOIN [CDWWork].[Dim].[labchemtest]  as c on a.Labchemtestsid=c.Labchemtestsid
WHERE  (a.LabChemSpecimenDateTime >= '20140101'  AND  a.LabChemSpecimenDateTime < '20180101') /*can change dates*/
       and (a.LabChemResultNumericValue is NOT NULL)

select * from #labtestnames
--(1,899,233 row(s) affected)

--save table into dflt table to import to SAS 
--(this will be used to construct frequency table of labchemtestnames to send to PIs for review,
--see SAS program "mkg_Troponin_labtestnames_freqTable")
select * 
into dflt.Troponin
from #labtestnames

/*	after importing table to SAS, deleting duplicate labchemtest names, 
	creating a frequency table of labchemtest names, and sending to PIs
	for review, then, pull all labchemtest names for given lab
	that were not excluded by PIs */

--pull in labchemtestnames following review from PIs	
select LabChemTestSID, LabChemTestName, LabChemPrintTestName, Sta3n
into #PIselectlabtestnames
from [CDWWork].[Dim].[LabChemTest]
where labchemtestname in (
'(IDM) TROPONIN I', 
'(OBSOLETE)TROPONIN', 
'***ERTROPONIN I ULTRA', 
'ACS TROPONIN',
'ACS-TROPONIN I',
'ANC TROPONIN I POC',
'ANCILLARY I-STAT TROPONIN I',
'ANCILLARY TROPONIN I',
'ANCILLARY TROPONIN-I TRIAGE (PB)',
'AT- TROPONIN-I',
'AT-CTNI',
'ATS cTnI',
'BR-TROPONIN I',
'CARDIAC TROPONIN I*NE',
'CARDIAC TROPONIN I',
'CARDIAC TROPONIN-I',
'CODE TROPONIN-T',
'CTNI',
'CTNI-ISTAT',
'ED TROPONIN',
'ER TROPONIN',
'ISTAT TROP I',
'iSTAT TROPONIN I',
'i-STAT TROPONIN I',
'I-STAT TROPONIN',
'I-STAT TROPONIN-I(FM)',
'I-STAT TROPONIN-I(HS)',
'ISTAT TROPONIN-I',
'I-STAT, TROP I (STL-MA)',
'ISTAT-TROPONIN',
'iTROPONIN I (ISTAT)',
'iTROPONIN I',
'iTROPONIN',
'MMC TROPONIN I',
'MV-TROPONIN',
'POC - TNI',
'POC CTNI',
'POC TROPONIN (ISTAT)',
'POC TROPONIN I (I-STAT)',
'POC TROPONIN I',
'POC TROPONIN',
'POC TROPONIN-I',
'POCT TROPONIN-I',
'POC-TROPONIN T',
'SALEM TROPONIN I (PB)',
'TMCB-TROPONIN',
'TNIULTRA(PLAS)',
'TNIULTRA(PLASMA) (DC"D JUNE 2015)',
'TP TROPONIN I',
'TPI (INITIAL),ANC',
'TRIAGE TROPONIN (DC"D 3/17)',
'TROP I POC (BH)',
'TROP I(D/C 6/7/17)',
'TROP-I iSTAT',
'TROPN-I (BU)',
'TROPONIN #2',
'TROPONIN #3',
'TROPONIN (POC)',
'TROPONIN (PRE 5/1/17)',
'TROPONIN (TRIAGE) -CP',
'TROPONIN (TRIAGE) -MO',
'TROPONIN ED POC',
'TROPONIN I (9/25/07)', 
'TROPONIN I (AUSTIN/WACO)',
'TROPONIN I (BEFORE 2-4-2014)',
'TROPONIN I (CARDIAC)',
'TROPONIN I (d/c"d 02/02/2014)',
'TROPONIN I (dc"d 3/10/14)',
'TROPONIN I (INITIAL)',
'TROPONIN I (ISTAT-NEWT)',
'TROPONIN I (old)',
'TROPONIN I (POST 7-22-10)',
'TROPONIN I (QUANT)',
'TROPONIN I (QUEST)',
'TROPONIN I (ROUTINE)',
'TROPONIN I (SERIAL)',
'TROPONIN I (Since 9/17/01)',
'TROPONIN I (STAT LAB)',
'TROPONIN I (STAT)',
'TROPONIN I (TEM/BC/BR/CP/P)',
'TROPONIN I (TnI)',
'TROPONIN I (VITROS 5600)',
'TROPONIN I {i-STAT} (ECU ONLY)',
'TROPONIN I CARDIAC TROPONIN',
'TROPONIN I ED/UC',
'TROPONIN I NEW',
'TROPONIN I POC',
'TROPONIN I- ST.LUKES',
'TROPONIN I thru 2015',
'TROPONIN I THRU 3/31/18',
'TROPONIN I ULTRA*',
'TROPONIN I ULTRA',
'TROPONIN I VITROS5600',
'TROPONIN I(ACL)',
'TROPONIN I(CO/KC/WI)(pre2/16/16)',
'TROPONIN I(DCd 2.17.15)',
'TROPONIN I(EK)(pre2/16/16)',
'TROPONIN I(i-STAT)',
'TROPONIN I(POC)',
'TROPONIN I',
'Troponin I',
'TROPONIN I, i-STAT1',
'TROPONIN I, PLASMA',
'TROPONIN I, SEND OUT TEST',
'TROPONIN II',
'TROPONIN I-QUEST',
'TROPONIN ISTAT',
'TROPONIN I-STAT',
'Troponin I-STAT',
'TROPONIN ISTAT-EK(pre 8/1/16)',
'TROPONIN POC*O,GI',
'TROPONIN T (QUEST)',
'TROPONIN T HS',
'TROPONIN T(34483)',
'TROPONIN T',
'Troponin T',
'TROPONIN T,SENDOUT CDH',
'TROPONIN T/SEND OUT',
'TROPONIN ULTRA (DXI)-CP',
'TROPONIN(I)',
'TROPONIN(PRE 5/1/17)',
'TROPONIN',
'TROPONIN-ACS',
'TROPONIN-I (Abbott)',
'TROPONIN-I (Access)',
'TROPONIN-I (AL/SY/CN/BH)',
'TROPONIN-I (CENTAUR)',
'TROPONIN-I (D"CD 7/26/17)',
'TROPONIN-I (dc"d 8/2018)',
'TROPONIN-I (FV)',
'TROPONIN-I (i-STAT)',
'TROPONIN-I (LAB)',
'TROPONIN-I (LABCORP)',
'TROPONIN-I (LCHC only)',
'TROPONIN-I (PB-D/C 2/16)',
'TROPONIN-I (POC)',
'TROPONIN-I (POST 2/29/00)',
'TROPONIN-I ANCILLARY',
'TROPONIN-I Beckman+3',
'TROPONIN-I POC',
'TROPONIN-I Routine (D/C 7/17/17)',
'TROPONIN-I STAT (D/C 7/17/17)',
'TROPONIN-I ULTRA [WLA only]',
'TROPONIN-I ULTRA',
'TROPONIN-I(POC) ISTAT',
'TROPONIN-I(ULTRA)',
'TROPONINI',
'TROPONIN-I',
'TROPONIN-I,BLOOD',
'TROPONIN-I,ISTAT',
'TROPONIN-I-INACT01/31/19',
'TROPONIN-ISTAT',
'Troponin-iSTAT',
'TROPONIN-I-ULTRA',
'TROPONIN-POC(ER only)',
'TROPONIN-POC',
'TROPONIN-T (QU)',
'TROPONIN-T (SEND OUT)',
'TROPONIN-T*CI',
'TROPONIN-T*IC',
'TROPONIN-T',
'TROPONIN-U',
'WB TROPONIN I',
'ZZTROPONIN I (667)',
'ZZTROPONIN I ER/VITROS5600',
'ZZTROPONIN I POC (BU)(',
'ZZTROPONIN-I (OLD 04-11-18)',
'ZZTROPONIN-I (STL-MA)D/C 02/16/16',
'zzzTROPONIN-I(NEW)',
'zzzTROPONIN-T')
--(289 rows affected)

select * 
into dflt.troponin_labname_ss20190507
from #PIselectlabtestnames

--pull loincsids and labchemtestsids from CDW for 2014-2017
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Troponin2014_2017
FROM  src.Chem_PatientLabChem AS A
INNER JOIN #loinc  b on  a.Loincsid=b.Loincsid 
LEFT JOIN [CDWWork].[Dim].[topography] AS d ON A.TopographySID =D.TopographySID
	WHERE a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'

UNION

SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
FROM src.Chem_PatientLabChem a         
INNER JOIN #PIselectlabtestnames b ON a.labchemtestsid=b.labchemtestsid 
LEFT JOIN [CDWWork].[Dim].[topography] AS d ON A.TopographySID =D.TopographySID
     WHERE /*loincsid=-1 and */  
      a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'

--get unique PatientICN & save into dflt table
select a.*, b.PatientICN
into dflt.Troponin_20142017_sms20190322
from #Troponin2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID
-- (3,447,896 rows affected)	

--compress table
alter table dflt.Troponin_20142017_sms20190322 /*copy and paste table name here: dflt.TableName or temp.tablename*/ 
rebuild partition=ALL
with
(data_compression=page)

--download dflt table into a SAS table to do further data management in SAS program:
--"step2_Troponin_I_T_2014_2017"
