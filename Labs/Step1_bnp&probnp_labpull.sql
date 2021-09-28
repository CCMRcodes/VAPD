/*Below SQL code will pull BNP & pro-BNP labs from CDW*/
/*Step 2 will download the saved BNP & pro-BNP lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 3/21/19
   Author: Sarah Seelye */

use ...
go

/***** BNP ******/

--pull in LOINCSIDs
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWORK].[Dim].[loinc]
where loinc in(	'30934-4', '35257-5', '42637-9', '47092-2',
				'33762-6', '33763-4', '71425-3', '83107-3', '83108-1')

select * from  #loinc
--(1170 rows affected)

--pull in labchemtestnames to send to PIs for review
SELECT a.LabChemTestSID, a.PatientSID, a.sta3n, a.LabChemSpecimenDateTime, a.LOINCSID,  c.labchemtestname 
into #labtestnames
FROM  [ ].[ ].[ ] AS A
INNER JOIN #loinc b on  a.Loincsid=b.Loincsid 
LEFT JOIN [CDWWork].[Dim].[labchemtest]  as c on a.Labchemtestsid=c.Labchemtestsid
WHERE  (a.LabChemSpecimenDateTime >= '20140101'  AND  a.LabChemSpecimenDateTime < '20180101') /*can change dates*/
       and (a.LabChemResultNumericValue is NOT NULL)

select * from #labtestnames
--(1,566,216 rows affected)

--save table into dflt table to import to SAS 
--(this will be used to construct frequency table of labchemtestnames to send to PIs for review,
--see SAS program "mkg_BNP_labtestnames_freqTable")
select * 
into dflt.BNP_20190321
from #labtestnames

/*	after importing table to SAS, deleting duplicate labchemtest names, 
	creating a frequency table of labchemtest names, and sending to PIs
	for review, then, pull all labchemtest names for given lab
	that were not excluded by PIs */

--pull in labchemtestnames following review from PIs	
select LabChemTestSID, LabChemTestName, LabChemPrintTestName, Sta3n
into #PIselectlabtestnames
from [CDWWork].[Dim].[LabChemTest]
where labchemtestname in	(
'(B-TYPE) NATRIURETIC PEPTIDE (SENDOUT)'  ,
 '(IDM) BNP'  ,
 'ANCILLARY BNP'  ,
 'B NATR.PEPTIDE(BNP)(dc"d 8/28/14)'  ,
 'B NATRIURETIC PEPTDE'  ,
 'B NATRIURETIC PEPTIDE'  ,
 'B NATRIURETIC PEPTIDE (BNP)'  ,
 'B TYPE NATRIURETIC PEPTIDE'  ,
 'BIOSITE BNP'  ,
 'BN Peptide'  ,
 'BN PEPTIDE'  ,
 'B-NATRIURETIC PEPTIDE'  ,
 'B-NATRIURETIC PEPTIDE (DCT 2017)'  ,
 'BNP'  ,
 'BNP (140889)'  ,
 'BNP (37386)'  ,
 'BNP (BRAIN NATRIURETIC PEPTIDE) ABBOTT'  ,
 'BNP (BU)'  ,
 'BNP (Dc"d 08/11/17)'  ,
 'BNP (FV)'  ,
 'BNP (IA)'  ,
 'BNP (IN-HOUSE)'  ,
 'BNP (Natriuretic Peptide Brain)'  ,
 'BNP (NEW)(Dcd 11-19-15)'  ,
 'BNP (REF.)'  ,
 'BNP (SPL)'  ,
 'BNP (SY)'  ,
 'BNP (TO 3/20/17)'  ,
 'BNP (TRIAGE)'  ,
 'BNP (wx)'  ,
 'BNP EVALUATR'  ,
 'BNP Ft. Myers only'  ,
 'BNP I-STAT'  ,
 'BNP NEW'  ,
 'BNP NT-PRO'  ,
 'BNP TAMC STAT'  ,
 'BNP THRU 5/9/18'  ,
 'BNP(DCd 2.17.15)'  ,
 'BNP(fee basis)'  ,
 'BNP(N-TERMINAL)'  ,
 'BNP(P)'  ,
 'BNP(POC)'  ,
 'BNP(POST 7/28/2010)'  ,
 'BNP(PRE 6/7/17)'  ,
 'BNP(proBNP)'  ,
 'BNP, NT-PRO'  ,
 'BNP, SENDOUT BMC'  ,
 'BNP,BLOOD'  ,
 'BNP-LUBBOCK STAT ONLY !'  ,
 'BNP-NT-pro'  ,
 'BNPS'  ,
 'BRAIN NATRIURETIC PEPTIDE'  ,
 'BRAIN NATRIURETIC PEPTIDE (PRE 8-20-18)'  ,
 'BRAIN NATRIURETIC PEPTIDE(dc"d 10/06/15)'  ,
 'BRAIN NATRIURETIC PEPTIDES'  ,
 'BR-BNP'  ,
 'B-TYPE NATRIURETIC'  ,
 'BType NATRIURETIC PEPTIDE'  ,
 'B-TYPE NATRIURETIC PEPTIDE'  ,
 'B-Type Natriuretic Peptide'  ,
 'B-TYPE NATRIURETIC PEPTIDE (BP)'  ,
 'B-TYPE NATRIURETIC PEPTIDE(DC,11/18/14)'  ,
 'B-TYPE NATRIURETIC PEPTIDE(DCed 9.28.15'  ,
 'B-TYPE NATRIURETIC PEPTIDE(LABCORP)'  ,
 'CARDIO IQ NT-ProBNP'  ,
 'CARDIOLOGY BNP SENDOUT (REF LAB)'  ,
 'DC"D 01/9/19-probrain BNP(0050083)'  ,
 'ER B-TYPE NATRIURETIC PEPTIDE'  ,
 'HATT-BNP'  ,
 'ISTAT BNP'  ,
 'LEG BNP'  ,
 'LRL BNP'  ,
 'MCC-BNP'  ,
 'MMC BNP (EDTA)'  ,
 'MN BNP'  ,
 'MV-BNP'  ,
 'NATRIURETIC PEP(BNP)'  ,
 'NATRIURETIC PEPTIDE (B TYPE)'  ,
 'NATRIURETIC PEPTIDE, BETA-TYPE'  ,
 'NT pro BNP'  ,
 'NT PRO-BNP'  ,
 'NT proB-TYPE NATRIURETIC PEPTIDE*'  ,
 'NT PROHORMONE BRAIN NATRIURETIC PEPTIDE'  ,
 'NT-pBNP (BH/CN/AL)'  ,
 'NT-PRO BNP'  ,
 'NT-PROBNP'  ,
 'NT-ProBNP'  ,
 'NT-proBNP'  ,
 'NT-proBNP (DCED 013119)'  ,
 'NT-proBNP (O)'  ,
 'NT-proBNP (proBNP N-TERMINAL)'  ,
 'NT-proBNP (Roche)'  ,
 'NT-proBNP (VITROS 5600)'  ,
 'NT-proBNP*IA'  ,
 'NT-proBNP*NE'  ,
 'PB BNP'  ,
 'PBNP'  ,
 'PBNP (BRAIN NATRIURETIC PEPTIDE) SIEMENS'  ,
 'POC BNP'  ,
 'POC TRIAGE: BNP (ED ONLY)'  ,
 'POC-BNP'  ,
 'PRO BNP'  ,
 'PRO BNP(ACL-STAT)'  ,
 'PROBNP'  ,
 'ProBNP'  ,
 'proBNP'  ,
 'PRO-BNP'  ,
 'pro-BNP'  ,
 'PRO-BNP (FOR OFF-TOUR ONLY) CDH'  ,
 'PROBNP (LABCORP)'  ,
 'PRO-BNP (LabCorp)'  ,
 'proBNP (SEND OUT)11188'  ,
 'PRO-BNP CARDIOASSESSR DC"D 10/02/2016'  ,
 'PROBNP(in-house)'  ,
 'ProBNP/SEND OUT'  ,
 'PROBRAIN NATRIURETIC PEPTIDE, NT (50083)'  ,
 'RML BNP'  ,
 'ZB TYPE NATRIURETIC PEPTIDE'  ,
 'zBNP'  ,
 'ZZ BNP (DCT 3/2014)'  ,
 'ZZ-B NATRIURETIC PEPTIDE (D/C 5/2015)'  ,
 'ZZBNP (BU'  ,
 'ZZBNP ANCILLARY'  ,
 'ZZZ BNP'  ,
 'ZZZBNP-RRRH'  ,
 'ZZZNT-proBNP')
--(217 rows affected)

select * from #PIselectlabtestnames

--pull loincsids and labchemtestsids from CDW for 2014-2017
SELECT distinct a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #BNP2014_2017
FROM  src.Chem_PatientLabChem AS A
INNER JOIN #loinc  b on  a.Loincsid=b.Loincsid 
LEFT JOIN [CDWWork].[Dim].[topography] AS d ON A.TopographySID =D.TopographySID
	WHERE a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'   and a.CohortName='Cohort20210503' 

UNION

SELECT distinct a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
FROM src.Chem_PatientLabChem a         
INNER JOIN #PIselectlabtestnames b ON a.labchemtestsid=b.labchemtestsid 
LEFT JOIN [CDWWork].[Dim].[topography] AS d ON A.TopographySID =D.TopographySID
     WHERE /*loincsid=-1 and */  
      a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'  and a.CohortName='Cohort20210503' 

--get unique PatientICN & save into dflt table
select distinct a.*, b.PatientICN
into dflt.BNP_proBNP_20142017_sms20190321
from #BNP2014_2017 a
left join Src.SPatient_SPatient b on a.patientsid=b.PatientSID
--(1,629,547 rows affected)	

--compress table
alter table dflt.BNP_proBNP_20142017_sms20190321 /*copy and paste table name here: dflt.TableName or temp.tablename*/ 
rebuild partition=ALL
with
(data_compression=page)

--download dflt table into a SAS table to do further data management in SAS program:
--"step2a_BNP&proBNP_2014_2017"
--then.
drop table dflt.BNP_proBNP_20142017_sms20190321
