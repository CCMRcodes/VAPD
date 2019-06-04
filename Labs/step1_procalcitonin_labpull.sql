/*Below SQL code will pull Procalcitonin labs from CDW*/
/*Step 2 will download the saved Procalcitonin lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 1/25/19
   Author: Sarah Seelye */

use  
go

/***** procalcitonin ******/

--pull in LOINCSIDs
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWORK].[Dim].[loinc]
where loinc in('33959-8', '75241-0')

select * from  #loinc
--(260 rows affected)


--pull in labchemtestnames to send to PIs for review
SELECT a.LabChemTestSID, a.PatientSID, a.sta3n, a.LabChemSpecimenDateTime, a.LOINCSID,  c.labchemtestname 
into #labtestnames
FROM  [ ].[ ].[ ] AS A
INNER JOIN #loinc b on  a.Loincsid=b.Loincsid 
LEFT JOIN [CDWWork].[Dim].[labchemtest]  as c on a.Labchemtestsid=c.Labchemtestsid
WHERE  (a.LabChemSpecimenDateTime >= '20140101'  AND  a.LabChemSpecimenDateTime < '20180101') /*can change dates*/
       and (a.LabChemResultNumericValue is NOT NULL)

select * from #labtestnames
--(96752 row(s) affected)

--save table into dflt table to import to SAS 
--(this will be used to construct frequency table of labchemtestnames to send to PIs for review,
--see SAS program "mkg_procalcitonin_labtestnames_freqTable")
select * 
into dflt.procalcitonin_20190125
from #labtestnames


/*	after importing table to SAS, deleting duplicate labchemtest names, 
	creating a frequency table of labchemtest names, and sending to PIs
	for review, then, pull all labchemtest names for given lab
	that were not excluded by PIs */

--pull in labchemtestnames following review from PIs	
select LabChemTestSID, LabChemTestName, LabChemPrintTestName, Sta3n
into #PIselectlabtestnames
from [CDWWork].[Dim].[LabChemTest]
where labchemtestname in	('PROCALCITONIN', 'PROCALCITONIN (AML)', 'PROCALCITONIN (BEFORE APRIL 2018)', 'PROCALCITONIN (DC"D 1/1/17)',
							'PROCALCITONIN (DC"D 1/4/2018)', 'PROCALCITONIN (LABCORP)dc"d 12/10/18', 'PROCALCITONIN (LC164750) DC"D 8/21/17',
							'PROCALCITONIN (LabCorp)', 'PROCALCITONIN (MA)', 'PROCALCITONIN (NEW)', 'PROCALCITONIN (PCT)', 
							'PROCALCITONIN (PCT)-NTX', 'PROCALCITONIN (Q)(d/c2/18)', 'PROCALCITONIN (QU)(dc"d 7/17/18)', 'PROCALCITONIN (REF LAB)',
							'PROCALCITONIN (SENDOUT)DC"D', 'PROCALCITONIN (SMH)', 'PROCALCITONIN (STAT)', 'PROCALCITONIN (SY/AL/Q)', 
							'PROCALCITONIN (pre3/13/18)', 'PROCALCITONIN LC', 'PROCALCITONIN SERUM', 'PROCALCITONIN [SPL] (OUTPUT)',
							'PROCALCITONIN(...10/15)*NE', 'PROCALCITONIN(16265)SENDOUT', 'PROCALCITONIN(Dcd 4-25-17)', 'PROCALCITONIN(LC)DCed 5.11.17',
							'PROCALCITONIN(O)d/c', 'PROCALCITONIN(PRE 4/2/2018)', 'PROCALCITONIN(Q)D"CD10/2018', 'PROCALCITONIN*', 
							'PROCALCITONIN*CI', 'PROCALCITONIN*NE', 'PROCALCITONIN, COBAS', 'PROCALCITONIN, QUEST(DC"D 10/31/17)', 
							'PROCALCITONIN, SERUM', 'PROCALCITONIN,BLOOD', 'PROCALCITONIN,Bld', 'PROCALCITONIN,SERUM', 'PROCALCITONIN,SERUM (DC"D 1/26/15)',
							'PROCALCITONIN,SERUM/d"cd 1/19/15', 'PROCALCITONIN-Q(Dcd 6-18-15)', 'PROCALCITONIN-QUEST', 'PROCALCITONIN-STAT (CC(Dced 11.16.17',
							'PROCALCITONIN-UKMC (to 11-18-13)', 'PROCALCITONIN-VAMC', 'PROCALCITONIN1', 'PROCALCITONIN_QUEST', 'STAT PROCALCITONIN', 
							'ZZ PROCALCITONIN', 'ZZPROCALCITIONIN(D"ced1.2.18)', 'ZZPROCALCITONIN (QUEST)', 'ZZPROCALCITONIN (SY/AL', 
							'ZZPROCALCITONIN, SERUM', 'ZZZPROCALCITONIN (NTX)')
							--(121 rows affected)

select * from #PIselectlabtestnames

--pull loincsids and labchemtestsids from CDW for 2014-2017
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Procalcitonin2014_2017
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
into dflt.Procalcitonin2014_2017
from #Procalcitonin2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID
-- (139710 rows affected)	

--download dflt table into a SAS table to do further data management in SAS program:
--"step2_procalcitonin_2014_2017"
