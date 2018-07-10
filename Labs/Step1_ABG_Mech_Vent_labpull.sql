/*Below SQL code will pull ABG labs from CDW*/
/*Step 2 will download the saved ABG lab pull table into SAS dataset for further cleaning*/

/* Date Modified: 6/16/2018
   Author: Shirley Wang */

use /*INSERT STUDY NAME*/
go

/*pull in all loincsids*/
select LOINC, Component, Sta3n, LOINCSID
into #loinc
from [CDWWork].[Dim].[loinc]
where loinc in ('11556-8', '11557-6', '18767-4', '19214-6','19216-1', '19218-7','19220-3', '19254-2', '19255-9', '19258-3', 
'2019-8', '2021-4', '2026-3', '2027-1', '20564-1', '20565-8', '24336-0', '24338-6', '24339-4', '24341-0', 
'24343-6', '24344-4', '2703-7', '2705-2', '2708-6', '2711-0', '2713-6','32771-8', '34163-6', '34705-4', '40619-9', 
'41647-9', '48391-7', '50188-2', '50982-8', '50984-4', '50986-9', '50987-7', '50989-3', '50991-9', '51731-8', '51733-4', 
'59274-1', '59275-8','59404-4', '60841-4', '61007-1', '62613-5', '65343-6', '71841-1', '71843-7', 
'71844-5', '71845-2', '71852-8', '71853-6' )

/*pull in Labchemtest*/
SELECT Labchemtestsid, LabChemTestName, LabChemPrintTestName, Sta3n
into #labtestnames
FROM  [CDWWork].[Dim].[LabChemTest]
WHERE labchemtestname in ('*VBG(VENOUS BLOOD GAS)', 
'0ABG RESULT', 
'0VENT (ABG)', 
'ABG', 
'ABG (12/01-11/08)', 
'ABG (12/08-9/09)', 
'ABG (ATS)', 
'ABG (BGL11)', 
'ABG (CORRECTED FOR PT.TEMP.)', 
'ABG (JV HOSP)', 
'ABG (LAB) 10/2/08', 
'ABG (MGMC)', 
'ABG (PRE 12/19/2011)', 
'ABG (PRE 5/22/98)', 
'ABG (RA For Recertification)', 
'ABG (gas only)', 
'ABG (gas only) GEM4k', 
'ABG 1 Liter', 
'ABG 100%', 
'ABG 2 Liters', 
'ABG 21%', 
'ABG 24%', 
'ABG 28%', 
'ABG 3 Liters', 
'ABG 30%', 
'ABG 35%', 
'ABG 4 Liters', 
'ABG 40%', 
'ABG 5 Liters', 
'ABG 50%', 
'ABG 6 Liters', 
'ABG 60%', 
'ABG 70%', 
'ABG 80%', 
'ABG 90%', 
'ABG AT PT TEMP', 
'ABG BREVARD (7/99 - 10/00)', 
'ABG BREVARD (D/C 2/12)', 
'ABG CO-OX (PULMONARY)', 
'ABG CO-OX ONLY', 
'ABG COOX (4/96 - 12/01)', 
'ABG CT PANEL 1 (dc"d)', 
'ABG DISPLAY (NO LYTES)', 
'ABG DRAW SITE', 
'ABG ELECTRO', 
'ABG EO', 
'ABG NO LYTES', 
'ABG PANEL', 
'ABG PANEL (RESP CARE)', 
'ABG PANEL - ARTERIAL LINE', 
'ABG PANEL-CARD/OR-G', 
'ABG PANEL-ICU-G', 
'ABG PANEL-J', 
'ABG PANEL-LC', 
'ABG PANEL-POC..', 
'ABG PANEL-WARD/ER-G', 
'ABG PANEL/CO-OX (RESP CARE)', 
'ABG PASCO (D/C 5/12)', 
'ABG PH-RRRH', 
'ABG PLUS (BGL4)', 
'ABG PROFILE', 
'ABG PROFILE FOR PULMONARY EMBOLISM', 
'ABG RESULTS', 
'ABG SEQUENCE', 
'ABG SICU (4/96 - 6/97)', 
'ABG SICU (6/97 - 12/01)', 
'ABG SITE', 
'ABG SPECIMEN TYPE', 
'ABG VIERA (INACTIVE 10/04/13)', 
'ABG W/LYTES', 
'ABG WITH CO-OX', 
'ABG pH', 
'ABG"S  (PB-MA-EV)', 
'ABG(2/28/01-3/30/06)', 
'ABG(BLOOD GASES)  (STL)', 
'ABG(Before 2/28/01)', 
'ABG(HAMOT)', 
'ABG(OR) (to 2/23/07)', 
'ABG(PH)', 
'ABG(dc"d)', 
'ABG+NA/K/OXIM/GLU (BGL11)', 
'ABG,FCOHb', 
'ABG,FIO2', 
'ABG,FO2Hb', 
'ABG,Hb,lytes,iCa', 
'ABG,Hb,lytes,iCa(GEM 4K)', 
'ABG,PT TEMP', 
'ABG,ctO2', 
'ABG,pCO2(T)', 
'ABG,pH(T)', 
'ABG,pO2(A-a,T)e', 
'ABG,pO2(T)', 
'ABG,pO2(a/A,T)e', 
'ABG,sO2', 
'ABG-FM', 
'ABG-LAB STD PANEL     AT:         FI', 
'ABG-LYTES', 
'ABG-MILES CITY', 
'ABG-POC', 
'ABG-RRRH', 
'ABG/CHEM5/IONIZED CALCIUM', 
'ABG/CO-OXIMETRY', 
'ABGCONT', 
'ABGCONT-HS', 
'ABL BLOOD GAS(CONTROL)', 
'ANCILLARY BLOOD GAS PANEL', 
'ARTERIAL BLOOD GAS', 
'ARTERIAL BLOOD GAS (CN)', 
'ARTERIAL BLOOD GAS (ON O2)', 
'ARTERIAL BLOOD GAS (ROOM AIR)', 
'ARTERIAL BLOOD GAS PANEL', 
'ARTERIAL BLOOD GASES', 
'ARTERIAL BLOOD GASES (BGL4)', 
'ARTERIAL BLOOD GASES (DCT 4/17)', 
'ARTERIAL BLOOD GASES (ON O2)', 
'ARTERIAL BLOOD GASES ON OXYGEN', 
'ARTERIAL BLOOD GASES ONLY', 
'ARTERIAL BLOOD GASES W/ CBXY (DCT 4/', 
'ARTERIAL BLOOD GASES*NE', 
'ARTERIAL BLOOD GASES-POC', 
'ARTERIAL BLOOD GASES-Pulm(DC"d 4/13/', 
'ARTERIAL BLOOD GASES-Tac', 
'ARTERIALBLOOD GASES', 
'AT-ABG', 
'ATS BLOOD GAS PANEL', 
'Ancillary BLOOD GASES', 
'BG OXYGEN CONTENT', 
'BG,OXYGEN CONTENT(THRU 5/8/2000)', 
'BLOOD GAS', 
'BLOOD GAS (10/93-10/98)*IC', 
'BLOOD GAS (AL)', 
'BLOOD GAS (ALL RESULTS)', 
'BLOOD GAS (Arterial)', 
'BLOOD GAS (BU)', 
'BLOOD GAS (FV)', 
'BLOOD GAS (ISTAT) Dc"d 1/2016', 
'BLOOD GAS (MU)', 
'BLOOD GAS (MV)*INACT(1-1-15)', 
'BLOOD GAS (SIEMENS)', 
'BLOOD GAS (SY)', 
'BLOOD GAS (TALLAH.)(THRU 7/30/01)', 
'BLOOD GAS (Venous)', 
'BLOOD GAS (iSTAT RESULT SEARCH)+_', 
'BLOOD GAS (not for use in SAC!)', 
'BLOOD GAS ANALYSIS', 
'BLOOD GAS ANALYSIS (ARTERIAL BLOOD)', 
'BLOOD GAS ANALYSIS (ARTERIAL BLOOD)', 
'BLOOD GAS ANALYSIS (OLD)', 
'BLOOD GAS ANALYSIS -ER 9/1/2006', 
'BLOOD GAS AND CO-OX PANEL', 
'BLOOD GAS ARTERIAL', 
'BLOOD GAS III', 
'BLOOD GAS JP/BR ONLY', 
'BLOOD GAS ONLY(PRE 2/28/01)', 
'BLOOD GAS PANEL', 
'BLOOD GAS PANEL FOR ICU', 
'BLOOD GAS PANEL [SEPULVEDA ONLY]', 
'BLOOD GAS PANEL(PRE 9/21/09)', 
'BLOOD GAS PANEL, ANC', 
'BLOOD GAS PANEL,VENOUS', 
'BLOOD GAS PANEL_', 
'BLOOD GAS PROFILE', 
'BLOOD GAS PROFILE (CALL RT)', 
'BLOOD GAS PROFILE (NOVA M)', 
'BLOOD GAS PROFILE - HCT/O2 SAT (NOVA', 
'BLOOD GAS SEQUENCER', 
'BLOOD GAS W/ COOX', 
'BLOOD GAS [Resp. Therapy only]', 
'BLOOD GAS i-STAT(ARTERIAL)', 
'BLOOD GAS i-STAT(VENOUS)', 
'BLOOD GAS pH', 
'BLOOD GAS(POC)', 
'BLOOD GAS(REFERRAL, DC"D)', 
'BLOOD GAS(RESPIRATORY CARE ONLY)', 
'BLOOD GAS(TALLAHASSEE)', 
'BLOOD GAS*IC', 
'BLOOD GAS, ANCILLARY', 
'BLOOD GAS, ARTERIAL', 
'BLOOD GAS, ARTERIAL (ISTAT)', 
'BLOOD GAS, ARTERIAL(WX)', 
'BLOOD GAS, GLUCOSE and LACTATE', 
'BLOOD GAS, VENOUS', 
'BLOOD GAS, VENOUS (PULMONARY)', 
'BLOOD GAS,ARTER(WX)', 
'BLOOD GAS,ARTERIAL', 
'BLOOD GAS,ARTERIAL(GEM)', 
'BLOOD GAS,THRU 2/01', 
'BLOOD GAS,VENOUS', 
'BLOOD GAS,VENOUS (GEM 4K)', 
'BLOOD GAS,VENOUS(GEM)', 
'BLOOD GAS,VENOUS(IS)JP/BR', 
'BLOOD GAS,VENOUS(WX)', 
'BLOOD GAS-ARTERIAL', 
'BLOOD GAS-ARTERIAL (SICU)', 
'BLOOD GAS-LYTES-HCT-GLUCOSE(PRE 2/28', 
'BLOOD GAS-MONT (dc"d 10/18/01)_', 
'BLOOD GAS-TUSK (dc"d 10/18/01)_', 
'BLOOD GAS-VENOUS', 
'BLOOD GAS-VENOUS (SICU)', 
'BLOOD GAS2-SEA', 
'BLOOD GASES', 
'BLOOD GASES  *VENOUS*', 
'BLOOD GASES  Consult order needed', 
'BLOOD GASES  INPATIENT', 
'BLOOD GASES  OUTPATIENT', 
'BLOOD GASES & CO-OXIMETRY', 
'BLOOD GASES & O2 SAT', 
'BLOOD GASES ( TO 6.22.95)', 
'BLOOD GASES (ARTERIAL BLOOD)', 
'BLOOD GASES (ARTERIAL)(ENDED 11-2013', 
'BLOOD GASES (BEFORE 10/2/13)', 
'BLOOD GASES (BH)', 
'BLOOD GASES (Inpatient)', 
'BLOOD GASES (NEWT)', 
'BLOOD GASES (NEWT)pre 2-12-07', 
'BLOOD GASES (PRIOR TO 1/2007)', 
'BLOOD GASES (RADIOMETER)DC"D 2008', 
'BLOOD GASES (VENOUS BLOOD)', 
'BLOOD GASES (VENOUS)', 
'BLOOD GASES (WHAV)', 
'BLOOD GASES **', 
'BLOOD GASES +LYTES', 
'BLOOD GASES - (DC"D 11/15)', 
'BLOOD GASES - BILOXI ONLY', 
'BLOOD GASES - EPOC', 
'BLOOD GASES - JACC ONLY', 
'BLOOD GASES - OLD', 
'BLOOD GASES II(prior to 5/2/2007)', 
'BLOOD GASES w/CHEM', 
'BLOOD GASES(PRIOR TO 05/03/04)', 
'BLOOD GASES(on O2)', 
'BLOOD GASES(room air)', 
'BLOOD GASES*', 
'BLOOD GASES, ARTERIAL', 
'BLOOD GASES, VENOUS', 
'BLOOD GASES,SENDOUT CDH', 
'BLOOD GASES-ABL (dc"d)', 
'BLOOD GASES-ASP (dc"d)', 
'BLOOD GASES-ASP LAB (dc"d 3/1998)', 
'BLOOD GASES-ASP.ONLY (dc"d)', 
'BLOOD GASES-BK', 
'BLOOD GASES-CHIRON (dc"d)', 
'BLOOD GASES-HS', 
'BLOOD GASES-KER DC"D 05/20/14', 
'BLOOD GASES-PULMONARY', 
'BLOOD GASES-RADIOMETER', 
'BLOOD GASES/LACTATE(DC"D)', 
'BR-ABG', 
'BR-PH(BLOOD GAS)', 
'CARBON DIOXIDE', 
'CARBON DIOXIDE (CO2)', 
'CARBON DIOXIDE (MWMC)', 
'CARBON DIOXIDE (POC)', 
'CARBON DIOXIDE (Q)', 
'CARBON DIOXIDE (RVMC)', 
'CARBON DIOXIDE (STAT)', 
'CARBON DIOXIDE CO2', 
'CARBON DIOXIDE CONTENT', 
'CARBON DIOXIDE THRU 6/26/06', 
'CARBON DIOXIDE TOTAL', 
'CARBON DIOXIDE thru 3/2/2004', 
'CARBON DIOXIDE thru 7/18/01', 
'CARBON DIOXIDE(001578)DC"D 10/6/2010', 
'CARBON DIOXIDE(ACL)', 
'CARBON DIOXIDE(CX9 OUTPUT ONLY)', 
'CARBON DIOXIDE(PH)', 
'CARBON DIOXIDE(PRIOR TO 4/29/96)', 
'CARBON DIOXIDE(PRIOR TO 8/23/00)', 
'CARBON DIOXIDE*NE', 
'CARBON DIOXIDE, WHOLE BLOOD', 
'CARBON DIOXIDE,Blood', 
'CARBON DIOXIDE,TOTAL(LABCORP)', 
'CARBON DIOXIDE-TMCB', 
'CVICU-BLOOD GAS', 
'CVICU-BLOOD GAS (OLD)', 
'D-CARBON DIOXIDE', 
'DMH-CARBON DIOXIDE (DC"D 8/12)', 
'EG7 (ABG)', 
'EXPIRED GAS PANEL(Before 2/22/01)', 
'GAS PANEL POC', 
'GEM (ABG) PANEL', 
'GEM-BLOOD GAS PROFILE', 
'I-STAT ABG+', 
'I-STAT BLOOD GAS', 
'I-STAT BLOOD GAS PANEL', 
'I-STAT CHEM8 +ABG', 
'ICU-ABG w/cooximetry', 
'ISABGC', 
'ISABGC i-STAT', 
'ISTAT BLOOD GASES', 
'ISTAT G3+ (ABG)', 
'ISTAT OR ABG', 
'ISTAT VENOUS BLOOD GASES', 
'LAB ABG', 
'LAB BLOOD GAS PANEL', 
'LAB(ABG)', 
'LOC-BLOOD GASES', 
'LOPC-ARTERIAL BLOOD GASES', 
'LRL CARBON DIOXIDE', 
'MICU BLOOD GASES-COOX', 
'MISC. BLOOD GAS TESTS', 
'Na,K,iCA,Hct,Blood Gas(POC)', 
'O-BLOOD GASES', 
'O2 SAT (ABG)', 
'O2CT  (BLOOD GAS)(10/93-10/98)*IC', 
'O2HB  (BLOOD GAS)*IC', 
'O2SAT (BLOOD GAS)(10/93-10/98)*IC', 
'O2SAT (BLOOD GAS)*IC', 
'OML BLOOD GASES', 
'OREM BLOOD GAS', 
'OXYGEN CONTENT,blood', 
'P-CARBON DIOXIDE', 
'P-CARBON DIOXIDE (DC"D 3-2009)', 
'PCO2  (BLOOD GAS)(10/93-10/98)*IC', 
'PCO2  (BLOOD GAS)*IC', 
'PERIPHERAL VENOUS BLOOD GAS', 
'PH    (BLOOD GAS)(10/93-10/98)*IC', 
'PH    (BLOOD GAS)*IC', 
'PH (ARTERIAL BLOOD GAS pH)', 
'PH (BLOOD GAS)', 
'PH (Blood Gases)', 
'PH (FLUID-BLOOD GAS LAB)', 
'PH ABG', 
'PH BLOOD GAS', 
'PH(BLOOD GAS)', 
'PH, BLOOD GASES', 
'PH-ABG (ASP)(dc"d 3/1998)', 
'PHANTOM ABG FOR NORMAL', 
'PHANTOM ABG QC', 
'PHANTOM ABG QC (HCT/O2 SAT)', 
'PHANTOM ABG/HCT/SO2', 
'PO2   (BLOOD GAS)(10/93-10/98)*IC', 
'PO2   (BLOOD GAS)*IC', 
'POC ABG', 
'POC ABG/ELECTROLYTES', 
'POC ABG/LACTATE', 
'POC ARTERIAL BLOOD GAS', 
'POC BLOOD GAS', 
'POC BLOOD GAS G3+', 
'POC BLOOD GAS PANEL', 
'POC BLOOD GAS PANEL CG8+', 
'POC BLOOD GAS(iSTAT)', 
'POC Blood Gases', 
'POC OXYGEN SATURATATION', 
'POC pCO2 (ABG)', 
'POC pH (ABG)', 
'POC pO2 (ABG)', 
'POC- BLOOD GASES', 
'POC-% OXYGEN', 
'POC-BLOOD GAS (EPOCAL)', 
'POC-BLOOD GAS ANALYSIS', 
'POC-ISTAT ABG BASE EXCESS', 
'POC-ISTAT ABG FIO2', 
'POC-ISTAT ABG HCO3', 
'POC-ISTAT ABG PCO2', 
'POC-ISTAT ABG PH', 
'POC-ISTAT ABG PO2', 
'POC-ISTAT ABG SO2', 
'POC-ISTAT ABG TCO2', 
'POC-ISTAT ARTERIAL BLOOD GAS', 
'POC-PCO2 BLOOD GAS', 
'POC-PO2 BLOOD GAS', 
'POC-cTCO2 BLOOD GAS', 
'POC-pH BLOOD GAS', 
'POCT ABG PLUS', 
'POCT ABG PLUS (12/08-4/10)', 
'POCT ABG PLUS (6/05-11/08)', 
'PULM. BLOOD GASES', 
'PULMONARY BLOOD GAS', 
'Q-CARBON DIOXIDE DC"D', 
'QC ABG PANEL', 
'QC BLOOD GAS', 
'QC FOR ABG"S', 
'RESPIRATORY CARE ABG', 
'REVIEW BLOOD GAS', 
'RICU BLOOD GASES-COOX', 
'RT OXYGEN THER IP OXYGEN', 
'RTABG', 
'S-CARBON DIOXIDE', 
'S-CARBON DIOXIDE DC"D', 
'SABG', 
'SALEM ABG"S  (PB)', 
'SEQUENCE ABG', 
'SEQUENCE ABG (BH)', 
'SEQUENCE ABG (CN)', 
'SEQUENCE ABG(NPT-7)', 
'SEQUENCE ABL BLOOD GAS CONTROLS*IC', 
'SEQUENCE BLOOD GAS', 
'SEQUENCE BLOOD GASES (RT)', 
'SEQUENCE BLOOD GASES (SY)', 
'SEQUENCE MU ABG', 
'SEQUENCE, ABG', 
'SEQUENCE, BLOOD GAS-288', 
'SEQUENCE, BLOOD GAS-845', 
'SEQUENCING ABG PANEL*CI', 
'SI BLOOD GASES', 
'SICU BLOOD GASES', 
'SPR-ABG (DC)', 
'SR CARBON DIOXIDE', 
'STAT BLOOD GAS-ARTERIAL', 
'STAT LAB ABG(Discontinued)', 
'TEMPERATURE CORRECTED ARTERIAL BLOOD', 
'TEMPERATURE CORRECTED BLOOD GAS', 
'TEMPERATURE CORRECTED VENOUS BLOOD G', 
'TEST ABG', 
'TOTAL CARBON DIOXIDE', 
'TOTAL CARBON DIOXIDE (ISTAT)', 
'TOTAL CARBON DIOXIDE (POC)', 
'TVL-BLOOD GAS (ARTERIAL)', 
'TVL-BLOOD GAS (VENOUS)', 
'U-CARBON DIOXIDE(CO2)', 
'ULTRA OR BLOOD GAS', 
'V-BLOOD GASES', 
'VENOUS BLOOD GAS', 
'VENOUS BLOOD GAS (BGL11)', 
'VENOUS BLOOD GAS (BGL4)', 
'VENOUS BLOOD GAS PANEL', 
'VENOUS BLOOD GAS+NA+K+OXIM/GLU (11)', 
'VENOUS BLOOD GAS-POC', 
'VENOUS BLOOD GASES', 
'VENOUS BLOOD GASES & CO-OXIMETRY', 
'VENOUS BLOOD GASES*CI', 
'VENOUS BLOOD GASES*O', 
'VENOUS PULM-BLOOD GAS FUNC DC"D 10/1', 
'W-CARBON DIOXIDE', 
'WP QC-BLOOD GAS', 
'WS-ABG', 
'X-BLOOD GAS (inact. 5/22/97)', 
'XXBLOOD GAS PANEL (DC"D 4/15/15)', 
'XXBLOOD GASES', 
'XXXABG', 
'ZABG PLUS (BGL 13)', 
'ZABG w/CO-OX', 
'ZABG-PLEASE USE CPRS ADD ORDER MENU', 
'ZABG-SICU', 
'ZARTERIAL BLOOD GASES (ON O2)-L', 
'ZBLOOD GAS FOR OR BEFORE 1/1/10', 
'ZBLOOD GAS FOR OR-TEMPCOR-1/1/10', 
'ZBLOOD GASES', 
'ZBLOOD GASES (*DO NOT USE 7/99*)', 
'ZBLOOD GASES (PULMONARY PANEL)', 
'ZCARBON DIOXIDE-EMERGENCY', 
'ZMC CARBON DIOXIDE', 
'ZNEW BLOOD GAS', 
'ZST.JAMES ABG', 
'ZSTAT CARBON DIOXIDE', 
'ZVENOUS BLOOD GAS (BGL13)', 
'ZVENOUS BLOOD GAS WITH CO-OX', 
'ZZ BLOOD GAS PANEL - 12/06', 
'ZZ BLOOD GASES', 
'ZZ BLOOD GASES MCAL', 
'ZZ*BLOOD GASSES', 
'ZZ-BLOOD GASES', 
'ZZ-CABG PANEL (OLD RESULTS)', 
'ZZABG (MA-EV)', 
'ZZABG MINUS', 
'ZZABG PANEL-CARD/OR(THRU 5/8/2000)', 
'ZZABG PANEL-ICU(THRU 5/8/2000)', 
'ZZABG PANEL-LC(THRU 1/1/99)', 
'ZZABG PANEL-WARD/ER(THRU 5/8/2000)', 
'ZZABG POC(...9/2010)*O', 
'ZZABG(EM)', 
'ZZABG,pCO2', 
'ZZABG-NOT USED', 
'ZZBLOOD GAS', 
'ZZBLOOD GAS (63)', 
'ZZBLOOD GAS (ARTERIAL)', 
'ZZBLOOD GAS (LAB) TO', 
'ZZBLOOD GAS (NICU LAB)', 
'ZZBLOOD GAS (PUL LAB)', 
'ZZBLOOD GAS (RAPIDLAB)', 
'ZZBLOOD GAS CAP', 
'ZZBLOOD GAS CHEM PANEL', 
'ZZBLOOD GAS PROFILE', 
'ZZBLOOD GAS PROFILE-BR', 
'ZZBLOOD GAS W/MEAS. SAT', 
'ZZBLOOD GAS-KC(PRE 9/23/94)', 
'ZZBLOOD GAS/COOX PANEL 1 (NO LYTES)', 
'ZZBLOOD GAS/COOX PANEL 2 (WITH LYTES', 
'ZZBLOOD GASES', 
'ZZBLOOD GASES & O2 SAT & BASE XS', 
'ZZBLOOD GASES (NOT IN USE)', 
'ZZBLOOD GASES (POC)', 
'ZZBLOOD GASES (VENOUS)', 
'ZZBLOOD GASES Consult order needed', 
'ZZBLOOD GASES DC"D 10-26-2007', 
'ZZBLOOD GASES(NEITHER)', 
'ZZBLOOD GASES-CO', 
'ZZBR-QC BLOOD GAS ACIDOSIS', 
'ZZBR-QC BLOOD GAS ALKALOSIS', 
'ZZBR-QC BLOOD GAS NORMAL', 
'ZZCAP AQUEOUS BLOOD GAS', 
'ZZCARBON DIOXIDE', 
'ZZCARBON DIOXIDE (SK)', 
'ZZCARBON DIOXIDE [Old 3/01]', 
'ZZCO2 ABG-TOT. (TCO2)', 
'ZZCONTROL-BLOOD GAS', 
'ZZDADE (178) ABG PANEL', 
'ZZDADE (288) ABG PANEL', 
'ZZFD BLOOD GAS', 
'ZZFD PH ABG', 
'ZZICU ABG PANEL', 
'ZZINFO-ANC-ISTAT-CHEM/ABG/HEMATOCRIT', 
'ZZIRMA BLOODGAS', 
'ZZLAB BLOOD GAS PROFILE DC"S 10-26-0', 
'ZZMMH ABG', 
'ZZNS CARBON DIOXIDE [Old 6/06]', 
'ZZO.R. BLOOD GAS', 
'ZZO2(CT-BLOOD GAS)', 
'ZZOXYGEN', 
'ZZOak BLOOD GAS [Old 2/12]', 
'ZZPH (ABG)', 
'ZZPH (BLOOD GAS)', 
'ZZPH(ABG)-Tac', 
'ZZPOC BLOOD GAS (GEM)', 
'ZZPOC BLOOD GAS VENOUS', 
'ZZPULMONARY BLOOD GASES__B/4__2/00', 
'ZZQ.C. FOR BLOOD GAS', 
'ZZREDD BLOOD GAS [Old 6/06]', 
'ZZSAC BLOOD GAS [Old 4/05]', 
'ZZSEH-CARBON DIOXIDE', 
'ZZSEQUENCE,ABG-LC(NEITHER)', 
'ZZTEST ABG', 
'ZZTEST LIST FOR ABG INSTR__B/4__7/10', 
'ZZXTESTABG', 
'ZZZ BLOOD GASES', 
'ZZZ BLOOD GASES PANEL', 
'ZZZABG (RESULTS ONLY)', 
'ZZZABG ELECT (STL ONLY)', 
'ZZZABG PANEL-POC', 
'ZZZABG PH-BONHAM', 
'ZZZABG pH', 
'ZZZABG pHc', 
'ZZZABG, (METRO)', 
'ZZZARTERIAL BLOOD GASES', 
'ZZZBLOOD GAS ANALYSIS-LYONS', 
'ZZZBLOOD GAS-VENOUS', 
'ZZZBLOOD GASES', 
'ZZZBLOOD GASES-BONHAM (Before 4/12/2', 
'ZZZBLOOD GASSES', 
'ZZZCARBON DIOXIDE (MANUAL)', 
'ZZZCARBON DIOXIDE-KER', 
'ZZZCH CARBON DIOXIDE CO2', 
'ZZZOR ABG PANEL', 
'ZZZOxygen.', 
'ZZZPOC Blood GAS', 
'ZZZS-CARBON DIOXIDE', 
'ZZZULTRA OR BLOOD GAS', 
'ZZpreOCT94(PH BLOOD GAS)', 
'_BLOOD GASES (ARTERIAL BLD)_B/4_1/17', 
'_O2HB (OF ABG PANEL)', 
'_OXYHGB (OF ABG PANEL)', 
'_PCO2 (OF ABG PANEL)', 
'_PO2 (OF ABG PANEL)', 
'_POC ABG TCO2', 
'_POC ABG pCO2', 
'_POC ABG pH', 
'_POC ABG pO2', 
'_POC ABG sO2', 
'_SO2 (OF ABG PANEL)', 
'aABG', 
'carbon dioxide (do not use or else)', 
'g3+ i-STAT(ABG)', 
'i-STAT BLOOD GAS (INC. LACTATE)', 
'i-STAT BLOOD GAS (INC. LYTES/HCT/HGB', 
'oBLOOD GASES-L', 
'pH (ABG)', 
'pH (ABG)(PRE 2/28/01)', 
'pH ABG-LAB', 
'pH(ABG)', 
'ph abg (dc"d 10/2000)', 
'xVENOUS BLOOD GAS (LAB) O2 SAT', 
'z BLOOD GASES', 
'zABG COMMENT', 
'zBLOOD GASES(DC9/27/11)', 
'zRC BLOOD GAS', 
'zTIME ABG DRAWN', 
'zZARTERIAL BG (LAKESIDE ABG)', 
'zzABG', 
'zzARTERIAL BLOOD GAS', 
'zzARTERIAL BLOOD GAS INFORMATION', 
'zzBLOOD GAS BP', 
'zzBLOOD GASES (BEFORE 1/30/03)', 
'zzBLOOD GASES-WHITESBURG', 
'zzBlood gases arterial', 
'zzPOCT ABG PLUS, ARTERIAL', 
'zzQC ABG THROUGH 11/18/11', 
'zzo PH ABG-L', 
'zzz CARBON DIOXIDE(BRAD)')


/*pull loincsids and labchemtestsids from CDW for 2014-2017*/
SELECT a.LabChemSID, a.LabSubjectSID,  a.Sta3n, a.LabPanelIEN, a.LabPanelSID, a.LongAccessionNumberUID, a.ShortAccessionNumber,
       a.LabChemTestSID, a.PatientSID, a.LabChemSpecimenDateTime, a.LabChemSpecimenDateSID, a.LabChemCompleteDateTime, a.LabChemCompleteDateSID,
       a.LabChemResultValue, a.LabChemResultNumericValue, a.TopographySID, a.LOINCSID, a.Units, a.RefHigh, a.RefLow, d.Topography
into #ABG2014_2017
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
into dflt.ABG2014_2017
from #ABG2014_2017 a
left join src.CohortCrosswalk b on a.patientsid=b.PatientSID

--download dflt talbe into a SAS table to do further data management