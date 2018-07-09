/*This is the example sql code to pull vasopressor drugs from CDW*/
/* Date Modified: 6/11/2018
   Author: Shirley Wang */

/*START BY PULLING DATA IN SQL SERVER, THEN SAVE THE TABLES TO DOWNLOAD INTO SAS TABLES TO DO DATA MANAGEMENT*/

/**** SQL CODES  ****/
use /*INSERT STUDY NAME*/
go

/*step 1: get all the LocalDrugSIDs associated with list of drugs*/
/*first pull all LocalDrugSIDs*/
SELECT a.DrugNameWithoutDose, a.LocalDrugNameWithDose,  a.NationalDrugNameWithDose,a.NationalDrug, a.Sta3n, a.LocalDrugSID, a.VAClassification,
a.UnitDoseMedicationRoute
into #localdrugsid
FROM  [CDWWork].[Dim].[LocalDrug] AS A 
WHERE  a.LocalDrugNameWithDose like '%DOPAMINE%' or a.LocalDrugNameWithDose like '%NOREPINEPHRINE%' or a.LocalDrugNameWithDose like '%EPINEPHRINE%'
or a.LocalDrugNameWithDose like '%PHENYLEPHRINE%' or a.LocalDrugNameWithDose like '%VASOPRESSIN%' 
OR
 a.DrugNameWithoutDose like '%DOPAMINE%' or a.DrugNameWithoutDose like '%NOREPINEPHRINE%' or a.DrugNameWithoutDose like '%EPINEPHRINE%'
or a.DrugNameWithoutDose like '%PHENYLEPHRINE%' or a.DrugNameWithoutDose like '%VASOPRESSIN%' 
OR
 a.NationalDrug like '%DOPAMINE%' or a.NationalDrug like '%NOREPINEPHRINE%' or a.NationalDrug like '%EPINEPHRINE%'
or a.NationalDrug like '%PHENYLEPHRINE%' or a.NationalDrug like '%VASOPRESSIN%' 
OR
a.NationalDrugNameWithDose like '%DOPAMINE%' or a.NationalDrugNameWithDose like '%NOREPINEPHRINE%' or a.NationalDrugNameWithDose like '%EPINEPHRINE%'
or a.NationalDrugNameWithDose like '%PHENYLEPHRINE%' or a.NationalDrugNameWithDose like '%VASOPRESSIN%' ;


/* get IVSolutionIngredientSID & LocalDrugSID from [CDWWORK].[Dim].[IVSolutionIngredient]*/
SELECT a.IVSolutionIngredientSID, a.LocalDrugNameWithDose, a.Sta3n, a.LocalDrugSID, a.Volume, a.IVSolutionFirstIngredientPrintName
into #IVSolutionIngredient
FROM  [CDWWORK].[Dim].[IVSolutionIngredient] AS A 
WHERE a.LocalDrugNameWithDose like '%DOPAMINE%' or a.LocalDrugNameWithDose like '%NOREPINEPHRINE%' or a.LocalDrugNameWithDose like '%EPINEPHRINE%'
or a.LocalDrugNameWithDose like '%PHENYLEPHRINE%' or a.LocalDrugNameWithDose like '%VASOPRESSIN%' 
OR
a.IVSolutionFirstIngredientPrintName like '%DOPAMINE%' or a.IVSolutionFirstIngredientPrintName like '%NOREPINEPHRINE%' or a.IVSolutionFirstIngredientPrintName like '%EPINEPHRINE%'
or a.IVSolutionFirstIngredientPrintName like '%PHENYLEPHRINE%' or a.IVSolutionFirstIngredientPrintName like '%VASOPRESSIN%';



/*get IVAdditiveIngredientSID & LocalDrugSID from [CDWWORK].[Dim].[IVAdditiveIngredient]*/ 
SELECT a.IVAdditiveIngredientSID, a.LocalDrugNameWithDose, a.Sta3n,  a.LocalDrugSID, a.DrugUnit, a.IVAdditiveIngredientPrintName
into #IVAdditiveIngredient
FROM  [CDWWORK].[Dim].[IVAdditiveIngredient] AS A 
WHERE  a.LocalDrugNameWithDose like '%DOPAMINE%' or a.LocalDrugNameWithDose like '%NOREPINEPHRINE%' or a.LocalDrugNameWithDose like '%EPINEPHRINE%'
or a.LocalDrugNameWithDose like '%PHENYLEPHRINE%' or a.LocalDrugNameWithDose like '%VASOPRESSIN%' 
or 
a.IVAdditiveIngredientPrintName like '%DOPAMINE%' or a.IVAdditiveIngredientPrintName like '%NOREPINEPHRINE%' or a.IVAdditiveIngredientPrintName like '%EPINEPHRINE%'
or a.IVAdditiveIngredientPrintName like '%PHENYLEPHRINE%' or a.IVAdditiveIngredientPrintName like '%VASOPRESSIN%';


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


--try patienticn again
select a.*, b.patienticn
into  dflt.BCMADispensedDrug_Pressor
from   #medlog a
left join [Src].[CohortCrosswalk] b on a.medlog_PatientSID=b.patientsid 
order by patienticn, ActionDateTime;





------------------------------------------------------------------------------------------------------------------------
/*pull from IV Additive for study years */
SELECT  a.LocalDrugNameWithDose, a.DrugUnit, a.IVAdditiveIngredientPrintName, a.LocalDrugSID, b.*
into #BCMA_Additive_v1
FROM  #IVAdditiveIngredient  as A
left join [Src].[BCMA_BCMAAdditive] as B on a.IVAdditiveIngredientSID=b.IVAdditiveIngredientSID
where b.ActionDateTime>= '2014-01-01' and b.ActionDateTime <'2018-01-01'



/*get patientsid for BCMAAdditive from BCMAMedicationLog*/
SELECT A.*, B.PatientSID, c.patienticn
into dflt.BCMA_Additive_pressor
FROM   #BCMA_Additive_v1  A
LEFT JOIN  [Src].[BCMA_BCMAMedicationLog]  B ON A.BCMAMedicationLogSID =B.BCMAMedicationLogSID 
left join [Src].[CohortCrosswalk] c on b.patientsid=c.patientsid;/*Add patientICN*/


-------------------------------------------------------------------------------------------------------------------
/*pull from  IV Solutions for study years */
SELECT  a.LocalDrugNameWithDose, a.Volume, a.IVSolutionFirstIngredientPrintName,a.LocalDrugSID, b.*
into #BCMA_Solution_v1
FROM  #IVSolutionIngredient  as A
left join [Src].[BCMA_BCMASolution] as B on a.IVSolutionIngredientSID=b.IVSolutionIngredientSID
where b.ActionDateTime>= '2014-01-01' and b.ActionDateTime <'2018-01-01'



/*get patientsid for BCMASolution from BCMAMedicationLog*/
SELECT A.*, B.PatientSID, c.patienticn
into dflt.BCMA_Solution_pressor
FROM   #BCMA_Solution_v1  A
LEFT JOIN  [Src].[BCMA_BCMAMedicationLog]  B ON A.BCMAMedicationLogSID =B.BCMAMedicationLogSID 
left join [Src].[CohortCrosswalk] c on b.patientsid=c.patientsid;/*Add patientICN*/


-------------------------------------------------------------------------------------------------------------------

-- move on to step 2, download dflt tables into SAS tables for data management