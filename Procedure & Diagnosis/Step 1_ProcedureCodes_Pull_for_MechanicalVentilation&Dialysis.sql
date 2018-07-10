/*Step 1, pull Procedure Codes*/

/*Below SQL code will pull in procedure codes for ICD-9 and ICD-10 for 2014-2017*/

/* Date Modified: 7/2/2018
   Author: Shirley Wang */

use /*insert study name*/
go

SELECT a.InpatientICDProcedureSID, a.Sta3n, a.PatientSID, a.InpatientSID,
a.AdmitDateTime, a.DischargeDateTime, a.ICD9ProcedureSID, a.ICD10ProcedureSID, a.ICDProcedureDateTime,
c.ICD10ProcedureCode, d.ICD9ProcedureCode
into #proc2014_2017
from Src.Inpat_InpatientICDProcedure a 
left join [CDWWORK].[Dim].[ICD10Procedure] c on a.ICD10ProcedureSID=c.ICD10ProcedureSID
left join [CDWWORK].[Dim].[ICD9Procedure] d on a.ICD9ProcedureSID=d.ICD9ProcedureSID
where a.AdmitDateTime >='2014-01-01' and a.AdmitDateTime < '2018-01-01'


--get patienticn
select a.*, b.PatientICN
into dflt.proc2014_2017
from #proc2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

/*download dflt.proc2014_2017 into a SAS dataset*/