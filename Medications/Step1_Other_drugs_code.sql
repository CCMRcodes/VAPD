/*This is the example sql code to pull Other drugs (Sedatives, Analgesics & Paralytics) from CDW*/
/* Date Modified: 6/11/2018
   Author: Shirley Wang */

/*START BY PULLING DATA IN SQL SERVER, THEN SAVE THE TABLES TO DOWNLOAD INTO SAS TABLES TO DO DATA MANAGEMENT*/

/**** SQL CODES  ****/
use /*INSERT STUDY NAME*/
go

/*step 1: get all the LocalDrugSIDs associated with list of drugs*/
/*first pull all LocalDrugSIDs for drugs of interest*/
SELECT a.DrugNameWithoutDose, a.LocalDrugNameWithDose,  a.NationalDrugNameWithDose,a.NationalDrug, a.Sta3n, a.LocalDrugSID, a.VAClassification,
a.UnitDoseMedicationRoute
into #localdrugsid
FROM  [CDWWork].[Dim].[LocalDrug] AS A 
WHERE a.LocalDrugNameWithDose like '%LORAZEPAM%' or a.LocalDrugNameWithDose like '%MIDAZOLAM%' or 
 a.LocalDrugNameWithDose like '%PROPOFOL%' or a.LocalDrugNameWithDose like '%VECURONIUM%'
 or a.LocalDrugNameWithDose like '%CISATRACURIUM%' or a.LocalDrugNameWithDose like '%DEXMEDETOMIDINE%' or a.LocalDrugNameWithDose like '%FENTANYL%'   
 or a.LocalDrugNameWithDose like '%ATRACURIUM%' or a.LocalDrugNameWithDose like '%ETOMIDATE%'   
 or a.LocalDrugNameWithDose like '%HYDROMORPHONE%' or a.LocalDrugNameWithDose like '%KETAMINE%' 
 or a.LocalDrugNameWithDose like '%LACTULOSE%' or a.LocalDrugNameWithDose like '%PANCURONIUM BROMIDE%'
 or a.LocalDrugNameWithDose like '%RIFAXIMIN%' or a.LocalDrugNameWithDose like '%MORPHINE%' 
OR
 a.DrugNameWithoutDose like '%LORAZEPAM%' or a.DrugNameWithoutDose like '%MIDAZOLAM%' or a.DrugNameWithoutDose like '%PROPOFOL%' or a.DrugNameWithoutDose like '%VECURONIUM%'
 or a.DrugNameWithoutDose like '%CISATRACURIUM%' or a.DrugNameWithoutDose like '%DEXMEDETOMIDINE%' or a.DrugNameWithoutDose like '%FENTANYL%'   
 or a.DrugNameWithoutDose like '%ATRACURIUM%' or a.DrugNameWithoutDose like '%ETOMIDATE%'   
 or a.DrugNameWithoutDose like '%HYDROMORPHONE%' or a.DrugNameWithoutDose like '%KETAMINE%' 
 or a.DrugNameWithoutDose like '%LACTULOSE%' or a.DrugNameWithoutDose like '%PANCURONIUM BROMIDE%'
 or a.DrugNameWithoutDose like '%RIFAXIMIN%' or a.DrugNameWithoutDose like '%MORPHINE%' 
OR
 a.NationalDrug like '%LORAZEPAM%' or NationalDrug like '%MIDAZOLAM%' or a.NationalDrug like '%PROPOFOL%' or a.NationalDrug like '%VECURONIUM%'
 or a.NationalDrug like '%CISATRACURIUM%' or a.NationalDrug like '%DEXMEDETOMIDINE%' or a.NationalDrug like '%FENTANYL%'   
 or a.NationalDrug like '%ATRACURIUM%' or a.NationalDrug like '%ETOMIDATE%'   or a.NationalDrug like '%HYDROMORPHONE%' or a.NationalDrug like '%KETAMINE%' 
 or a.NationalDrug like '%LACTULOSE%' or a.NationalDrug like '%PANCURONIUM BROMIDE%' or a.NationalDrug like '%RIFAXIMIN%' or a.NationalDrug like '%MORPHINE%'
OR
 a.NationalDrugNameWithDose like '%LORAZEPAM%' or a.NationalDrugNameWithDose like '%MIDAZOLAM%' or a.NationalDrugNameWithDose like '%PROPOFOL%' 
 or a.NationalDrugNameWithDose like '%VECURONIUM%' or a.NationalDrugNameWithDose like '%CISATRACURIUM%'
 or a.NationalDrugNameWithDose like '%DEXMEDETOMIDINE%' or a.NationalDrugNameWithDose like '%FENTANYL%'   
 or a.NationalDrugNameWithDose like '%ATRACURIUM%' or a.NationalDrugNameWithDose like '%ETOMIDATE%'   
 or a.NationalDrugNameWithDose like '%HYDROMORPHONE%' or a.NationalDrugNameWithDose like '%KETAMINE%' 
 or a.NationalDrugNameWithDose like '%LACTULOSE%' or a.NationalDrugNameWithDose like '%PANCURONIUM BROMIDE%'
 or a.NationalDrugNameWithDose like '%RIFAXIMIN%' or a.NationalDrugNameWithDose like '%MORPHINE%';
 

/* get IVSolutionIngredientSID & LocalDrugSID from [CDWWORK].[Dim].[IVSolutionIngredient]*/
SELECT a.IVSolutionIngredientSID, a.LocalDrugNameWithDose, a.Sta3n, a.LocalDrugSID, a.Volume, a.IVSolutionFirstIngredientPrintName
into #IVSolutionIngredient
FROM  [CDWWORK].[Dim].[IVSolutionIngredient] AS A 
WHERE  a.LocalDrugNameWithDose like '%LORAZEPAM%' or a.LocalDrugNameWithDose like '%MIDAZOLAM%' or 
    a.LocalDrugNameWithDose like '%PROPOFOL%' or a.LocalDrugNameWithDose like '%VECURONIUM%'
 	or a.LocalDrugNameWithDose like '%CISATRACURIUM%' or a.LocalDrugNameWithDose like '%DEXMEDETOMIDINE%' or a.LocalDrugNameWithDose like '%FENTANYL%'   
 	or a.LocalDrugNameWithDose like '%ATRACURIUM%' or a.LocalDrugNameWithDose like '%ETOMIDATE%'   
 	or a.LocalDrugNameWithDose like '%HYDROMORPHONE%' or a.LocalDrugNameWithDose like '%KETAMINE%' 
 	or a.LocalDrugNameWithDose like '%LACTULOSE%' or a.LocalDrugNameWithDose like '%PANCURONIUM BROMIDE%'
 	or a.LocalDrugNameWithDose like '%RIFAXIMIN%' or a.LocalDrugNameWithDose like '%MORPHINE%' 
OR
	a.IVSolutionFirstIngredientPrintName like '%LORAZEPAM%' or a.IVSolutionFirstIngredientPrintName like '%MIDAZOLAM%' or 
	a.IVSolutionFirstIngredientPrintName like '%PROPOFOL%' or a.IVSolutionFirstIngredientPrintName like '%VECURONIUM%'
	or a.IVSolutionFirstIngredientPrintName like '%CISATRACURIUM%' or a.IVSolutionFirstIngredientPrintName like '%DEXMEDETOMIDINE%' 
	or a.IVSolutionFirstIngredientPrintName like '%FENTANYL%'  or a.IVSolutionFirstIngredientPrintName like '%ATRACURIUM%' 
	or a.IVSolutionFirstIngredientPrintName like '%ETOMIDATE%'  or a.IVSolutionFirstIngredientPrintName like '%HYDROMORPHONE%' 
	or a.IVSolutionFirstIngredientPrintName like '%KETAMINE%'  or a.IVSolutionFirstIngredientPrintName like '%LACTULOSE%'
	or a.IVSolutionFirstIngredientPrintName like '%PANCURONIUM BROMIDE%' or a.IVSolutionFirstIngredientPrintName like '%RIFAXIMIN%' 
	or a.IVSolutionFirstIngredientPrintName like '%MORPHINE%';



/*get IVAdditiveIngredientSID & LocalDrugSID from [CDWWORK].[Dim].[IVAdditiveIngredient]*/ 
SELECT a.IVAdditiveIngredientSID, a.LocalDrugNameWithDose, a.Sta3n,  a.LocalDrugSID, a.DrugUnit, a.IVAdditiveIngredientPrintName
into #IVAdditiveIngredient
FROM  [CDWWORK].[Dim].[IVAdditiveIngredient] AS A 
WHERE  a.LocalDrugNameWithDose like '%LORAZEPAM%' or a.LocalDrugNameWithDose like '%MIDAZOLAM%' or 
 	a.LocalDrugNameWithDose like '%PROPOFOL%' or a.LocalDrugNameWithDose like '%VECURONIUM%'
 	or a.LocalDrugNameWithDose like '%CISATRACURIUM%' or a.LocalDrugNameWithDose like '%DEXMEDETOMIDINE%' or a.LocalDrugNameWithDose like '%FENTANYL%'   
 	or a.LocalDrugNameWithDose like '%ATRACURIUM%' or a.LocalDrugNameWithDose like '%ETOMIDATE%'   
 	or a.LocalDrugNameWithDose like '%HYDROMORPHONE%' or a.LocalDrugNameWithDose like '%KETAMINE%' 
 	or a.LocalDrugNameWithDose like '%LACTULOSE%' or a.LocalDrugNameWithDose like '%PANCURONIUM BROMIDE%'
 	or a.LocalDrugNameWithDose like '%RIFAXIMIN%' or a.LocalDrugNameWithDose like '%MORPHINE%' or a.IVAdditiveIngredientPrintName like '%LORAZEPAM%' 
OR 
	a.IVAdditiveIngredientPrintName like '%MIDAZOLAM%' or  a.IVAdditiveIngredientPrintName like '%PROPOFOL%' 
	or a.IVAdditiveIngredientPrintName like '%VECURONIUM%' or a.IVAdditiveIngredientPrintName like '%CISATRACURIUM%' 
	or a.IVAdditiveIngredientPrintName like '%DEXMEDETOMIDINE%' or a.IVAdditiveIngredientPrintName like '%FENTANYL%'   
 	or a.IVAdditiveIngredientPrintName like '%ATRACURIUM%' or a.IVAdditiveIngredientPrintName like '%ETOMIDATE%'   
 	or a.IVAdditiveIngredientPrintName like '%HYDROMORPHONE%' or a.IVAdditiveIngredientPrintName like '%KETAMINE%' 
 	or a.IVAdditiveIngredientPrintName like '%LACTULOSE%' or a.IVAdditiveIngredientPrintName like '%PANCURONIUM BROMIDE%'
 	or a.IVAdditiveIngredientPrintName like '%RIFAXIMIN%' or a.IVAdditiveIngredientPrintName like '%MORPHINE%';



-------------------------------------------------------------------------------------------------------------------
/*pull from [BCMA_BCMADispensedDrug] for study years*/
SELECT a.DrugNameWithoutDose, a.LocalDrugNameWithDose,  a.NationalDrugNameWithDose,a.NationalDrug, a.VAClassification,
a.UnitDoseMedicationRoute, b.*
into #BCMA_BCMADispensedDrug_v1
FROM #localdrugsid  as A
left join [Src].[BCMA_BCMADispensedDrug] as B on a.LocalDrugSID=b.LocalDrugSID
where b.ActionDateTime>= '2014-01-01' and b.ActionDateTime <'2018-01-01'

select patientsid , BCMAMedicationLogSID
into #medlogA 
from [Src].[BCMA_BCMAMedicationLog]
where BCMAMedicationLogSID in (select BCMAMedicationLogSID from #BCMA_BCMADispensedDrug_v1 )


--get patientsid from the parent BCMA_BCMAMedicationLog table instead then get patienticn
select a.*, b.PatientSID as medlog_PatientSID
into #medlog
from #BCMA_BCMADispensedDrug_v1 a
left join #medlogA  b on a.BCMAMedicationLogSID=b.BCMAMedicationLogSID


--get patienticn 
select a.*, b.patienticn
into  dflt.BCMA_BCMADispensedDrug_v2
from   #medlog a
left join [Src].[CohortCrosswalk] b on a.medlog_PatientSID=b.patientsid 
order by patienticn, ActionDateTime;



-------------------------------------------------------------------------------------------------------------------
/*pull from IV Additive for study years */
SELECT  a.LocalDrugNameWithDose, a.DrugUnit, a.IVAdditiveIngredientPrintName, a.LocalDrugSID, b.*
into #BCMA_Additive_v1
FROM  #IVAdditiveIngredient  as A
left join [Src].[BCMA_BCMAAdditive] as B on a.IVAdditiveIngredientSID=b.IVAdditiveIngredientSID
where b.ActionDateTime>= '2014-01-01' and b.ActionDateTime <'2018-01-01'

/*get patientsid for #BCMA_Additive_v1 from BCMAMedicationLog, need patientsid to get unique patienticn later*/
SELECT A.*, B.PatientSID, c.patienticn
into dflt.BCMA_Additive_v1
FROM   #BCMA_Additive_v1  A
LEFT JOIN  [Src].[BCMA_BCMAMedicationLog]  B ON A.BCMAMedicationLogSID =B.BCMAMedicationLogSID 
left join [Src].[CohortCrosswalk] c on b.patientsid=c.patientsid;/*Add patientICN*/


-------------------------------------------------------------------------------------------------------------------
/*/*pull from  IV Solutions for study years */
SELECT  a.LocalDrugNameWithDose, a.Volume, a.IVSolutionFirstIngredientPrintName,a.LocalDrugSID, b.*
into #BCMA_Solution_v1
FROM  #IVSolutionIngredient  as A
left join [Src].[BCMA_BCMASolution] as B on a.IVSolutionIngredientSID=b.IVSolutionIngredientSID
where b.ActionDateTime>= '2014-01-01' and b.ActionDateTime <'2018-01-01'


/*get patientsid for #BCMA_Additive_v1 from BCMAMedicationLog, need patientsid to get unique patienticn later*/
SELECT A.*, B.PatientSID, c.patienticn
into dflt.BCMA_Solution_v1
FROM   #BCMA_Solution_v1  A
LEFT JOIN  [Src].[BCMA_BCMAMedicationLog]  B ON A.BCMAMedicationLogSID =B.BCMAMedicationLogSID 
left join [Src].[CohortCrosswalk] c on b.patientsid=c.patientsid;/*Add patientICN*/



-------------------------------------------------------------------------------------------------------------------

-- move on to step 2, download dflt tables into SAS tables for data management