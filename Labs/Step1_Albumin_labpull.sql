/*Below SQL code will pull Albumin labs from CDW*/
/*Step 2 will download the saved Albumin lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 6/18/2018
   Author: Shirley Wang */

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('1751-7', '2862-1', '54347-0', '61151-7', '61152-5', '62234-0', '62235-7', '76631-1', '77148-5', '21058-3', 
'40046-5', '40047-3', '40048-1', '40049-9', '40050-7', '40051-5', '40052-3', '40053-1', '40054-9', '40055-6', '40056-4', '40057-2', 
'40058-0', '40059-8', '40060-6', '40061-4', '40062-2', '40063-0')

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('ALBUMIN', 'ALBUMIN (V2)', 'ALBUMIN*','Albumin', 'ALBUMIN*IA', 'ALBUMIN (PLASMA)', 'ALBUMIN (LX)', 
'ALBUMIN*NE', 'ALBUMIN (Blood/Fluids V.A.)','ALBUMIN (DCed 2.1.15', 'ALBUMIN (FV)', 'ALBUMIN (CX)', 'ALBUMIN ,Blood', 'ZALBUMIN', 'ALB(D/C 6/7/17)', 
'ALBUMIN, SERUM', 'ALBUMIN -SERUM THRU 3/31/18', 'ALBUMIN,(serum/plasma)', 'ALBUMIN(SERA/PLASMA)', 'ALBUMIN, PLASMA', 'ALBUMIN(BMT)', 
'ALBUMIN(LUF)', 'ALBUMIN (FS)', 'ALBUMIN(SPE)', 'ALBUMIN(KTY)', 'SPECIALTY ALBUMIN', 'ALBUMIN (PE)','ALBUMIN(TMB)', 'SPE ALBUMIN', 'ALBUMIN-SPE-FRACTION', 
'ALBUMIN (Q)', 'P-ALBUMIN(I)','ALBUMIN EL', 'ALBUMIN (MV)*INACT(1-1-15)', '*ALBUMIN (QUEST)', 'ALBUMIN (g/dL)', 'Albumin fraction', 'ALBUMIN  (PB-sendout)', 
'ALB (elp)', 'ALBUMIN FRACTION (g/dL)', 'ALBUMIN (S-HEM)', 'D-ALBUMIN', 'ALB gm/dl', 'ALBUMIN SERUM-QUEST', 'ALBUMIN, SERUM-QD', 'ALBUMIN (QD)', 
'ALBUMIN(ELECTRO)', 'MN ALBUMIN', 'ALBUMIN (gm)', 'ALBUMIN(747)', 'SERUM ALBUMIN* (SY)', 'ALBUMIN (PARAGON)', 'PB ALBUMIN', 'Albumin, Serum (LC)', 
'ALBUMIN(14966)', 'W-ALBUMIN','*ALBUMIN (MA-PB)', 'REF-ALBUMIN', 'ALBUMIN (CALCULATED)', 'ALBUMIN (ARUP)', 'ALBUMIN.SERUM.QUEST', 'ALBUMIN (gr/dL)', 
'ALBUMIN g/dl', 'ALBUMIN (QUEST)', 'ALBUMIN(PVA)', 'ALBUMIN (SEAT)', 'ALBUMIN SEATTLE  -', 'ALBUMIN (LABCORP)', 'BR-ALBUMIN', 'Albumin (LC)', 'ALBUMIN,SERUM', 
'SO-ALBUMIN', 'ALBUMIN SERUM REF','Albumin (AML)', 'ZZZALBUMIN,SERUM(SL1510)','ELD ALBUMIN', 'ALBUMIN (001487)', 'ALBUMIN, ARUP', 'ALBUMIN-------(PSHCS)', 
'MMC ALBUMIN', 'ALBUMIN(s)','ALBUMIN (SEATTLE)', 'ALBUMIN,SERUM,(LABCORP)', 'LRL ALBUMIN', 'Albumin (g/dL)', 'Albumin (Serum LABCORP)', 
'ALBUMIN-QD', 'ALBUMIN (CALC)', 'serum albumin', 'ALBUMIN-FL', 'ALBUMIN,Serum(qu)', 'ALBUMIN el-SO', 'ALBUMIN-SEA', 'ALBUMIN, SERUM (PE)', 'albumin fraction', 
'ALBUMIN (Ref.Lab)', 'SALEM ALBUMIN   (PB)', 'ALBUMIN,SERUM(o)', '.ALBUMIN,SERUM (SL)', 'aLBUMIN (Q)', 'ZMH ALBUMIN BEFORE 1/28/14', 'ALBUMIN,SERUM (MS)', 
'Albumin, Serum', 'albumin, serum, ref lab', 'ALBUMIN (REF LAB)', 'ALBUMIN IN SERUM', 'HATT-ALBUMIN', 'QUEST ALBUMIN', 'LEG ALBUMIN', 
'ALBUMIN,QUANT,SERUM* (V2/Q)', 'ALBUMIN FRACTION','ALBUMIN(EP)', 'ALBUMIN (SPEP)', '.ALBUMIN FRACTION', 'ALBUMIN, ELECTROPHORESIS', 'Albumin serum electph', 
'ALBUMIN, ELECTROPHOR','ALBUMIN (EP)', 'ALBUMIN (SPEP) [SD]', 'DIALYSIS ALBUMIN (BCG METHOD)', 'ALBUMIN FRACTION (SPEP)', 'ALBUMIN (ELECTROPHORESIS)', 
'ALBUMIN (ELECTRO)','ALBUMIN FRACTION (SPEP)*ia', 'ALBUMIN,PEP', 'ALB FRACT (AML)', 'Albumin (SPEP)', 'ALBUMIN SPE/UPE', '.SPEP ALBUMIN', 
'ALBUMIN,ELECTROPHORESIS', 'DIALYSIS ALBUMIN', 'ALBUMIN FRACTION (SPIFE)', 'ALBUMIN FRACTION (PEP)', 'ALBUMIN-FRACTION (dc"d 7/2015)', 'Albumin EP', 
'.ALBUMIN ELEC', 'ALBUMIN FRAC.', 'ALBUMIN(EP)(b)','ALBUMIN (EP) (wr)', 'Albumin Fraction', 'ALBUMIN (PEP)', 'ALBUMIN [for SPEP](LC)', 'ALBUMIN (SPE)', 
'ALBUMIN FRACTION SERUM*ne', 'ALBUMIN(EP),blood', 'ALBUMIN-FRACTION', 'SPEP ALBUMIN', 'ALBUMIN FRACTIO (EP)','ALBUMIN (protein elec)', 
'(SPEP) ALBUMIN','SPEP ALBUMIN* (SY)', '.ALBUMIN ELECTRO', 'ALBUMIN-EP', '.Albumin fraction', 'ALBUMIN FRACTION*NE', 'ALBUMIN(ELECTRO-PHILA)', 
'ALBUMIN, SPEP/UPEP WR', 'ALBUMIN el', 'ALBUMIN, SPEP', '_ALBUMIN-SPEP', 'SPEP-ALBUMIN* (AL)', 'ALBUMIN PE', 'Albumin-Fraction', 'ALBUMIN(EP)wx', 
'Albumin (Electrophoresis)', 'ALBUMIN-EP (PORT)', 'P.E. ALBUMIN', 'ALBUMIN (IEP,SERUM)', 'LC-ALBUMIN, ELE', 'ALBUMIN SPE')


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #Albumin2014_2017
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
into dflt.Albumin2014_2017
from #Albumin2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management