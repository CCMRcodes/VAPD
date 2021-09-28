/*Below SQL code will pull Urea labs from CDW*/
/*Step 2 will download the saved Urea lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 10/11/2018
   Author: Shirley Wang */

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('12961-9', '12962-7', '12963-5', '12964-3', '14937-7', '22664-7', '3091-6', '3094-0', '39776-0', 
'39777-8', '59570-2', '6299-2', '72903-8', '72926-9', '72927-7', '77010-7', '77136-0', '11064-3', '11065-0', '12966-8', 
'12981-7', '17759-2', '17760-0', '35234-4', '42571-0', '48629-0', '48639-9', '49071-4', '72270-2', '20977-5' )

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('UREA NITROGEN', 'BUN', 'UREA NITROGEN,BLOOD', 'UREA NITROGEN (BUN)', 'UREA NITROGEN mg/dL', 'UREA NITROGEN (V2)', 
'UREA NITROGEN*', 'UREA NITROGEN-------O','Blood Urea Nitrogen', 'UREA NITROGEN*IA', 'XUREA NITROGEN', 'UREA NITROGEN, BLOOD', 'UREA NITROGEN*NE', 
'UREA NITROGEN (FV)', 'UREA NITROGEN,Blood', 'UREA NITROGEN (DCed 2.1.15','UREA NITROGEN (CX)', 'ZUREA NITROGEN', 'BUN(D/C 6/7/17)', 'BUN -SERUM THRU 3/31/18', 
'UREA NITROGEN, SERUM', 'POC BUN', 'UREA NITROGEN,plasma', 'I-STAT BUN','BUN, PLASMA', 'iBUN (new)', 'UREA NITROGEN(blood)', 'BUN/UREA (BLOOD UREA NITROGEN)', 
'UREA NITROGEN(BMT)', 'POC-UREA NITROGEN', 'UREA NITROGEN(LUF)', 'BUN (POC)', 'UREA NITROGEN (FS)', 'iBLOOD UREA NITROGEN', 'iBUN', 
'iSTAT BUN', 'ISTAT BUN','POST DIALYSIS BUN', 'POC BUN ISTAT', 'BUN_KTY', 'AT- BUN', 'BUN-iSTAT', 'BUN (TMB)', 'WB BUN', 'UREA NITROGEN (MV)*INACT(1-1-15)', 
'P-BUN(I)', 'ANCILLARY BUN', 'BUN,POST DIALYSIS', 'BUN, ANC', 'UREA NITROGEN (ISTAT)', 'D-BUN', 'UREA NITROGEN-POC*ic', 'BUN,POST', 'BUN (iSTAT)', 
'BUN-POST DIALYSIS', 'MN BUN', 'preBUN', 'postBUN','POST UREA NITROGEN, BLOOD', 'PRE-BUN', 'POST BUN', 'POST-DIALYSIS BUN', 'PB BUN', 'W-UREA NITROGEN', 
'BUN (POST DIAL)', 'POST-BUN', 'UREA NITROGEN (POST)', '.PRE-DIALYSIS BUN', '.POST-DIALYSIS BUN', 'BUN,PRE DIALYSIS', 'UREA NITROGEN (DIALYSIS ONLY)', 
'_POC BUN', 'BUN-PRE DIALYSIS', 'I-BUN', 'BR-BUN', 'UREA NITROGEN (POST DIALYSIS ONLY)', 'ISTAT UREA NITROGEN', 'BUN (ATS)', '_BUN (I-STAT)', 'BUN (WORC)', 
'ELD BUN', 'POC UREA NITROGEN', 'BUN(POC)', 'MMC BUN', 'PRE-DIALYSIS BUN', 'BUN-POC', 'UREA NITROGEN(LABCORP)', 'LRL UREA NITROGEN', 'UREA NITROGEN (POC)', 
'UREA NITROGEN POC (BU/BH)', 'SALEM BUN  (PB)', 'BUN (ISTAT)', 'BUN POC', 'HATT-BUN', 'UREA NITROGEN (Ref.Lab)', 'AT-BUN', 'MH BUN', 
'LEG BUN', 'Bun','UREA NITROGEN (ARUP)', 'i-Bun', 'BUN (POST DIALYSIS)', 'BUN (PRE-DIALYSIS)')


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT distinct a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Urea2014_2017
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
     WHERE  /* loincsid=-1 and  */   
      a.LabChemSpecimenDateTime >= '2014-01-01' and a.LabChemSpecimenDateTime < '2018-01-01'  and a.CohortName='Cohort20210503' 


/*get unique PatientICN*/
select distinct a.*, b.PatientICN
into dflt.Urea2014_2017
from #Urea2014_2017 a
left join Src.SPatient_SPatient b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management

drop table dflt.Urea2014_2017
