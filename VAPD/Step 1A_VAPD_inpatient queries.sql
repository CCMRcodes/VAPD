/*Step 1 of building a VA inpatient database*/
/*Below SQL code pulls VA acute inpatient records including diagnosis codes from specialty stays.*/

/* Date Modified: 9/21/2018
   Author: Brenda Vincent */

use /*INSERT STUDY NAME*/
go

--indicator for acute specialty
select distinct treatingspecialtysid, specialtysid, treatingspecialtyname, specialty, 
case when specialty in ('ANESTHESIOLOGY', 
					'CARDIAC INTENSIVE CARE UNIT', 'CARDIAC SURGERY','CARDIAC-STEP DOWN UNIT',
					'CARDIOLOGY','DERMATOLOGY','EAR, NOSE, THROAT (ENT)','ENDOCRINOLOGY',
					'EPILEPSY CENTER','GASTROENTEROLOGY','GEM ACUTE MEDICINE','GEN MEDICINE (ACUTE)',
					'GENERAL SURGERY','GENERAL(ACUTE MEDICINE)','HEMATOLOGY/ONCOLOGY',
					'INTERMEDIATE MEDICINE','MEDICAL ICU','MEDICAL STEP DOWN','METABOLIC',
					'NEUROLOGY','NEUROSURGERY','OB/GYN','OPHTHALMOLOGY','ORAL SURGERY',
					'ORTHOPEDIC','PERIPHERAL VASCULAR','PLASTIC SURGERY','PODIATRY',
					'PROCTOLOGY','PULMONARY, NON-TB','PULMONARY, TUBERCULOSIS','STROKE UNIT',
					'SURGICAL ICU','SURGICAL STEPDOWN','TELEMETRY','THORACIC SURGERY','TRANSPLANTATION',
					'UROLOGY','VASCULAR','z GEM ACUTE MEDICINE','zCARDIAC-STEP DOWN UNIT','zCARDIOLOGY',
					'zDERMATOLOGY','zENDOCRINOLOGY','zEPILEPSY CENTER','zGASTROENTEROLOGY','zGENERAL(ACUTE MEDICINE',
					'zHEMATOLOGY/ONCOLOGY','zMETABOLIC','zNEUROLOGY','zNEUROSURGERY','zOPHTHALMOLOGY',
					'zORTHOPEDIC','zPERIPHERAL VASCULAR', 'zPODIATRY', 'zPROCTOLOGY', 'zPULMONARY, NON-TB',
					'zPULMONARY, TUBERCULOSI', 'zSTROKE UNIT','zSURGICAL ICU','zTELEMETRY','zUROLOGY',
					'ZZPULMONARY DISEASE','MEDICAL OBSERVATION','SURGICAL OBSERVATION','ED OBSERVATION','NEUROLOGY OBSERVATION') 
					     then 1 else 0 end as acute
into dflt.acutespecialty
from cdwwork.dim.treatingspecialty

/*Inpat.SpecialtyTransfer Records*/
declare @STARTDATE2 datetime2 ='20140101', @ENDDATE2 datetime2 ='20180201'
SELECT distinct 
		st.InpatientSID,
		st.PatientSID, 
		st.sta3n,
		st.SpecialtyTransferDateTime,
		st.specialtytransfersid, /*to join to dx table*/
		st.LOSInService, 
		a.specialty,
		a.acute, 
		flag='S'
into #vapd_inpatspec
FROM Src.Inpat_SpecialtyTransfer st
inner join dflt.acutespecialty a--get acute indicator and specialty name
on st.TreatingSpecialtySID=a.TreatingSpecialtySID
where st.admitdatetime >= @STARTDATE2 and st.admitdatetime< @ENDDATE2 and st.InpatientSID>0;

--get diagnosis codes
select a.*, b.POAIndicator, b.OrdinalNumber, c.icd9code, d.icd10code
into #vapd_inpatspec_dx
from #vapd_inpatspec a
left join SRC.Inpat_SpecialtyTransferDiagnosis b
on a.specialtytransfersid=b.specialtytransfersid
left join cdwwork.dim.icd10 d
on b.icd10sid=d.icd10sid
left join cdwwork.dim.icd9 c 
on b.icd9sid=c.icd9sid;

select top 100 * from #vapd_inpatspec_dx order by inpatientsid, ordinalnumber

select count(distinct inpatientsid) from #vapd_inpatspec_dx
select count(distinct patientsid) from #vapd_inpatspec_dx

DROP TABLE #vapd_inpatspec; 

alter table #vapd_inpatspec_dx 
drop column specialtytransfersid

--INPAT.INPATIENT RECORDS (to get DischargeDateTime and sta6a)
declare @STARTDATE datetime2 ='20140101', @ENDDATE datetime2 ='20180201'
SELECT distinct 
		i.InpatientSID,
		i.PatientSID,
		i.sta3n,
		i.AdmitDateTime, 
		i.DischargeDateTime,
		w.sta6a,
		flag='I'
into #VAPD_inpat
FROM SRC.Inpat_Inpatient AS i
left join cdwwork.dim.wardlocation  w--get sta6a
on i.admitwardlocationsid=w.wardlocationsid
where i.admitdatetime >= @STARTDATE and i.admitdatetime< @ENDDATE and i.inpatientsid>0;


--Join diagnosis tables
select a.*, b.OrdinalNumber as inpat_ordinalnumber, c.icd10code as inpat_icd10code, d.icd9code  as inpat_icd9code
into  #VAPD_inpat_dx
from #VAPD_inpat a
left join SRC.Inpat_Inpatientdiagnosis b
on a.inpatientsid=b.inpatientsid
left join cdwwork.dim.icd10 c
on b.icd10sid=c.icd10sid
left join cdwwork.dim.icd9 d 
on b.icd9sid=d.icd9sid


--Join Inpatient and Inpatient Specialty Transfer tables (with dx codes), 
--get PatientICN and ScrSSN for DOD 

SELECT distinct s.PatientICN, s.scrssn,  a.*,  b.admitdatetime, b.dischargedatetime, b.sta6a
into #merged_vapd_inpat
FROM #vapd_inpatspec_dx a
left join #vapd_inpat_dx b
on a.inpatientsid=b.inpatientsid and a.patientsid=b.patientsid 
inner JOIN src.CohortCrosswalk s
ON (a.PatientSID=s.PatientSID)

--remove null dx codes and get from inpat.inpatientdiagnosis table
select *
into  #merged_vapd_inpat2
from #merged_vapd_inpat
where icd9code is not null or icd10code is not null

select top 100 * from #merged_vapd_inpat2 order by InpatientSID, OrdinalNumber

select * into #nulldx 
from #merged_vapd_inpat 
where icd9code is null and icd10code is null

SELECT distinct a.patienticn, a.scrssn, a.InpatientSID, a.PatientSID, a.Sta3n, 
a.SpecialtyTransferDateTime, a.LOSInService, a.specialty,
a.acute, a.flag, a.admitdatetime, a.dischargedatetime, a.sta6a, 
b.inpat_ordinalnumber as ordinalnumber, b.inpat_icd9code as icd9code, 
b.inpat_icd10code as icd10code
into #nulldx2
FROM #nulldx a
left join #vapd_inpat_dx b
on a.inpatientsid=b.inpatientsid and a.patientsid=b.patientsid


select top 100 * from #nulldx2 order by inpatientsid, ordinalnumber
select top 100 * from #merged_vapd_inpat2 order by InpatientSID, OrdinalNumber

drop table #merged_vapd_inpat

--union the two tables. 
--This table has all specialty stays with the dx codes from either the spec trans dx table or inpat dx table
select patienticn, scrssn, inpatientsid, patientsid, sta3n, specialtytransferdatetime, 
losinservice, specialty, acute, flag, admitdatetime, dischargedatetime, sta6a, 
ordinalnumber, icd9code, icd10code
into #merged_vapd_inpat
from  #merged_vapd_inpat2
union 
select patienticn, scrssn, inpatientsid, patientsid, sta3n, specialtytransferdatetime, 
losinservice, specialty, acute, flag, admitdatetime, dischargedatetime, sta6a, 
ordinalnumber, icd9code, icd10code
from #nulldx2 

select count(distinct inpatientsid) from #vapd_inpat_dx
select count(distinct patientsid) from #vapd_inpat_dx

select count(distinct inpatientsid) from #vapd_inpatspec_dx
select count(distinct patientsid) from #vapd_inpatspec_dx

select count(distinct inpatientsid) from #merged_vapd_inpat
select count(distinct patienticn) from #merged_vapd_inpat

--date of death for all patients in cohort
select distinct a.patienticn, b.dod
into #dod
from src.CohortCrosswalk a
left join src.VitalStatus_Mini b
on a.scrssn=b.scrssn
where dod is not null

--add dod
select a.*, b.dod
into dflt.merged_vapd_inpat
from #merged_vapd_inpat a
left join #dod b
on a.patienticn=b.patienticn

select top 100 * from dflt.merged_vapd_inpat 
order by inpatientsid, specialtytransferdatetime, ordinalnumber

select top 100 *
 from dflt.merged_vapd_inpat order by inpatientsid, ordinalnumber

 select * into #check from dflt.merged_vapd_inpat where DischargeDateTime is null

 drop table dflt.vapd_inpat, #vapd_inpat

select  *
into #vapd_inpat
from dflt.merged_vapd_inpat
where dischargedatetime is not null

drop table dflt.merged_vapd_inpat

select *
into dflt.vapd_inpat
from #vapd_inpat--56,033,770

select count(distinct patienticn) from dflt.vapd_inpat  
select count(distinct inpatientsid) from dflt.vapd_inpat 



/*Download dflt.vapd_inpat into a SAS dataset for further data management*/
