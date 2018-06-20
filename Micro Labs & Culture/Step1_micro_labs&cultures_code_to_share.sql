/*Below sql code is to extract micro biology labs and cultures for the VAPD. Principle Investigators (PI) came up with a list of test names of interest.
These CSV files need to be uploaded into the Research database in CDW: 
1)lab chem test names_round2
2)lab chem topography
3)ordered tests_tji
4)specimen_types_tji
5)test name vs ordered test
6)workload_codes*/


/*********** Other lab chem tests of interest **************/
/*SQL codes for CDW*/
use /*INSERT Studyname*/
go

--Look for Flu, Legionella, C Diff lab tests
--Added RSV, and General Viral Panel

select labchemtestname, labchemtestsid
into #TestNames
from cdwwork.dim.labchemtest
where labchemtestname like '%flu%' or
labchemtestname like '%legion%' or
labchemtestname like '%diff%' or
labchemtestname like '%rsv%' or
labchemtestname like '%syncytial%' or
labchemtestname like '%viral%' 


--Version 2, add in strep, nocardia, malaria, antigens, Pneumocystis 
drop table #TestNames2
select labchemtestname, labchemtestsid
into #TestNames2
from cdwwork.dim.labchemtest
where labchemtestname like '%strep%' or
labchemtestname like '%nocardia%' or
labchemtestname like '%malaria%' or
labchemtestname like '%antigens%' or
labchemtestname like '%pneumocystis%' 


---found some tests we missed
select labchemtestname, labchemtestsid
into #TestNames3
from cdwwork.dim.labchemtest
where labchemtestname like '%cryptococc%' or
labchemtestname like '%histoplasm%' 



--select distinct labchemtestname from #TestNAmes3
--order by LabChemTestName

--put the three tables together
select * 
into #TestNamesFinal
from #TestNAmes

union select * from #TestNames2
union select * from #TestNAmes3



--Import CSV file, (lab chem test names_round2.csv) where he marked which tests to keep, then merge in labchemtestsid.
--5/27/16--new file imported with additional changes and tests
--then merge with labchem data

select a.*,b.[Final Keep]
into #TestsKeep
from #TestNamesFinal a
left join dflt.[lab_chem_tests_round2] b
on a.labchemtestname=b.labchemtestname
where [final keep]=1 


select a.labchemtestname,b.*
into dflt.lab_chem_keep
from #TestsKeep a
left join 
src.Chem_PatientLabChem b
on a.labchemtestsid=b.labchemtestsid
where b.LabChemspecimendatetime>='20140101' and b.LabChemspecimendatetime<'20180101'

--These descriptives were outputed for clinical review, clinicians determine which test and topography to keep or exclude
--Looks at topography data and decide which to keep
--This is done by type of test, because it varies depending on the test

select distinct b.topography,count(*) as count
from dflt.lab_chem_keep a 
left join cdwwork.dim.topography b
on a.topographysid=b.topographysid
where labchemtestname like '%flu%' or 
labchemtestname like '%rsv%' or
labchemtestname like '%syncytial%' or
labchemtestname like '%viral%' 
group by topography
order by topography

select distinct b.topography,count(*) as count
from dflt.lab_chem_keep a 
left join cdwwork.dim.topography b
on a.topographysid=b.topographysid
where labchemtestname like '%diff%'
group by topography
order by topography

select distinct labchemtestname,b.topography,count(*) as count
from dflt.lab_chem_keep a 
left join cdwwork.dim.topography b
on a.topographysid=b.topographysid
where labchemtestname like '%legion%' 
group by labchemtestname,topography
order by labchemtestname,topography

select distinct b.topography,count(*) as count
from dflt.lab_chem_keep a 
left join cdwwork.dim.topography b
on a.topographysid=b.topographysid
where labchemtestname like '%strep%'
or labchemtestname like '%histoplasm%'
group by topography
order by topography

select distinct b.topography,count(*) as count
from dflt.lab_chem_keep a 
left join cdwwork.dim.topography b
on a.topographysid=b.topographysid
where labchemtestname like '%malaria%' 
group by topography
order by topography

select distinct b.topography,count(*) as count
from dflt.lab_chem_keep a 
left join cdwwork.dim.topography b
on a.topographysid=b.topographysid
where labchemtestname like '%pneumocystis%' 
group by topography
order by topography

select distinct b.topography,count(*) as count
from dflt.lab_chem_keep a 
left join cdwwork.dim.topography b
on a.topographysid=b.topographysid
where labchemtestname like '%cryptococc%' 
group by topography
order by topography

select distinct labchemtestname,b.topography,count(*) as count
from dflt.lab_chem_keep a 
left join cdwwork.dim.topography b
on a.topographysid=b.topographysid
where labchemtestname like '%nocardia%' 
group by labchemtestname,topography
order by labchemtestname,topography

--these descriptives were reviewed by PIs for decisions to keep or exclude in later step

--Import file lab chem topography.csv to keep based on test types and topography
--I created a nickname for the each type of tests to use to merge on
--This data is in the table [lab_chem_topography]

--Add Nickname field to Lab data, so I can merge with the topography table above

select a.*,b.topography,
case when labchemtestname like '%cryptococc%' then 'cryptococc'
when labchemtestname like '%pneumocystis%' then 'pneumocystis'
when labchemtestname like '%malaria%' then 'malaria'
when labchemtestname like '%strep%' or labchemtestname like '%histoplasm%' then 'strep or histoplasm'
when labchemtestname like '%legion%' and (labchemtestname like '%antigen%' or labchemtestname like '%ag%')
 then 'Legion ag'
when labchemtestname like '%legion%' and labchemtestname not like '%antigen%' and labchemtestname not like '%ag%'
 then 'Legion other'
when labchemtestname like '%diff%' then 'diff'
when labchemtestname  like '%flu%' or labchemtestname like '%rsv%' or labchemtestname like '%syncytial%' or
labchemtestname like '%viral%' then 'Viral' else null end as nickname
into #LabChem2 
from dflt.lab_chem_keep a
left join cdwwork.dim.topography b
on a.topographysid=b.topographysid


select a.*,b.keep
into dflt.LabChemFinal3_14_18
from dflt.[lab_chem_topography] b
left join #LabChem2 a
on a.nickname=b.test and a.topography=b.topography
where b.keep=1


--check first to be sure this works right, then go back and add where keep=1 to above statement

--select distinct LabChemTestName,Topography 
--from #LabChem3 
--where keep is null or keep=0
--order by LabChemTestName,Topography 

--download dflt.LabChemFinal3_14_18 into SAS table in step 2.


-------------------------------------------------------------------------------------------------------------

/********** ORDERED CULTURES **************/
--This csv file (Ordered test_TJI.csv) was uploaded into our Research database in CDW into the dflt.ordered test_TJI table.

--[INSERT STUDY NAME].dflt.Ordered_tests_Tji  is a list of labchem test names to include from Principle Investigator (PI).
select a.*,b.labchemtestsid
into #KeepList
from [INSERT STUDY NAME].dflt.Ordered_tests_Tji a
left join cdwwork.dim.labchemtest b
on a.labchemtestname=b.labchemtestname

select * from #Keeplist

--Another PI looked at the list of Workload codes and determined which to keep
--This csv file (workload_codes.csv) was uploaded into the table dflt.workload_types

--dflt.workload_types is the list from PI indicating which workload type/lab procedure to keep
--merge in workload code as well as lab chem testname

drop table #Keep2
select a.keep,a.flu,a.labchemtestname,b.*, c.topography, d.collectionsample, e.labprocedure, e.interest_cx
into #Keep2
from #KeepList a 
join [INSERT STUDY NAME].Src.Micro_MicroOrderedTest b
on a.labchemtestsid=b.OrderedLabChemTestSID
left join cdwwork.dim.topography c
on b.topographysid=c.topographysid
left join cdwwork.dim.collectionsample d
on b.collectionsamplesid=d.collectionsamplesid
left join [INSERT STUDY NAME].dflt.workload_types e
on e.workloadcode=b.OrderedTest


--The two lists above had some contradictions so we have to recitify those
--query below is where two PI's lists don't match
--give this to PI to determine which combinations to exclude

select  labchemtestname,labprocedure,keep,interest_cx,count(*) as count
from #keep2
group by labchemtestname,labprocedure,keep,interest_cx

--The arbitrated list (name vs ordered test_tji.csv) was uploaded to the table dflt.name_vs_ordered_test

--upload  list where PI arbitrated the above list and merge in to drop unwanted combos
drop table #Keep3
select a.*,b.[arbitrated keep]
into #Keep3 
from #Keep2 a
left join dflt.name_vs_ordered_test b
on a.labchemtestname=b.labchemtestname and a.labprocedure=b.labprocedure
where b.[arbitrated keep] =1 or b.[arbitrated keep] is null



--keep only the original lab test names of interest from PI's list
--those where keep=1 and in time frame.
select * 
into #Keep4
from #Keep3
where (keep=1 or [arbitrated keep]=1) and SpecimentakenDateTime>='20140101' and SpecimenTakenDateTime<'20180101' 


--look at records with missing topography or collection sample
select labchemtestname,labprocedure,topography,collectionsample, count(*) as count
from #keep4
where topography='*Missing*' or topography is null or collectionsample ='*Missing*' or collectionsample is null
group by labchemtestname,labprocedure,topography,collectionsample
order by labchemtestname,labprocedure,topography,collectionsample

--Create list of topography and collection samples for these labs and send to PI
select topography, collectionsample,count(*)  as count
from #Keep4
group by topography, collectionsample
order by topography, collectionsample

--List of specimen types to keep or drop (specimen_Types_tji.csv) was uploaded from into dflt.specimen_types_tji
--Import list that PI marked as keep or not keep and merge with keep4 table

select a.*,b.[Keep Non Flu], b.[Keep Flu]
into #keep5 
from #Keep4 a
left join dflt.specimen_types_tji b
on a.topography=b.topography and a.collectionsample=b.collectionsample
where (Flu=0 and [Keep Non FLu]=1) or (Flu=1 and [Keep Flu]=1)


select a.sta3n,a.cohortname,a.patientsid,a.labchemtestname,a.labprocedure,a.topography,a.collectionsample,
a.specimentakendatetime
into [INSERT STUDY NAME].Dflt.OrderedCultures
from #keep5 a 


select year(SpecimenTakenDateTime),count(*) as count
from [INSERT STUDY NAME].Dflt.OrderedCultures
group by year(SpecimenTakenDateTime)

select max(SpecimenTakenDateTime), min(SpecimenTakenDateTime) 
from [INSERT STUDY NAME].Dflt.OrderedCultures

select distinct labchemtestname,topography,collectionsample, count(*) as count
from [INSERT STUDY NAME].Dflt.OrderedCulturesFinal
group by labchemtestname,topography,collectionsample
order by labchemtestname,topography,collectionsample


-- when we added in nocardia, we ended up with topographies that we didn't want
--keep only blood and bronchial stuff, drop all collection samples below.
select * 
into dflt.OrderedCulturesFinal3_14_18
from dflt.OrderedCultures
where not (labchemtestname like '%nocardia%' and collectionsample in ('ABSCESS','ASPIRATE','BIOPSY','Choose:','CSF',
'FLUID','FLUID, PLEURAL','FLUID, SYNOVIAL','INDUCED SPUTUM','LESION','LUNG','LUNG ASPIRATE','LUNG TISSUE',
'PERITONEAL FLUID','PLEURAL FLUID','POST BRONCH SPUTUM','SPUTUM','SPUTUM, NOT SPECIFIED','SPUTUM,EXPECTOR',
'SPUTUM,RESP.THERAPY ASSISTED','SPUTUM,TRACH/ET','STOOL','SWAB','SYNOVIAL FLUID','TISSUE','TRACHEAL ASPIRATE','WOUND'))


--download dflt.OrderedCulturesFinal3_14_18 into SAS table in step 2.