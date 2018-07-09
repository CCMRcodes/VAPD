/*This is the example sql code to pull antibiotic drugs from CDW*/
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
WHERE a.LocalDrugNameWithDose like ('%Acyclovir%') or a.LocalDrugNameWithDose like ('%Amikacin%') or a.LocalDrugNameWithDose like ('%Amoxicillin%') or a.LocalDrugNameWithDose like ('%Clavulanate%') or 
a.LocalDrugNameWithDose like ('%Amphotericin B%') or a.LocalDrugNameWithDose like ('%Ampicillin%') or a.LocalDrugNameWithDose like ('%Sulbactam%') or 
a.LocalDrugNameWithDose like ('%Anidulafungin%') or a.LocalDrugNameWithDose like ('%Azithromycin%') or a.LocalDrugNameWithDose like ('%Aztreonam%') or a.LocalDrugNameWithDose like ('%Caspofungin%') or 
a.LocalDrugNameWithDose like ('%Cefaclor%') or a.LocalDrugNameWithDose like ('%Cefadroxil%') or a.LocalDrugNameWithDose like ('%Cefamandole%') or a.LocalDrugNameWithDose like ('%Cefazolin%') or 
a.LocalDrugNameWithDose like ('%Cefdinir%') or a.LocalDrugNameWithDose like ('%Cefditoren%') or a.LocalDrugNameWithDose like ('%Cefepime%') or a.LocalDrugNameWithDose like ('%Cefixime%') or 
a.LocalDrugNameWithDose like ('%Cefmetazole%') or a.LocalDrugNameWithDose like ('%Cefonicid%') or a.LocalDrugNameWithDose like ('%Cefoperazone%') or a.LocalDrugNameWithDose like ('%Cefotaxime%') or 
a.LocalDrugNameWithDose like ('%Cefotetan%') or a.LocalDrugNameWithDose like ('%Cefoxitin%') or a.LocalDrugNameWithDose like ('%Cefpodoxime%') or 
a.LocalDrugNameWithDose like ('%Cefprozil%') or a.LocalDrugNameWithDose like ('%Ceftaroline%') or a.LocalDrugNameWithDose like ('%Ceftazidime%') or 
a.LocalDrugNameWithDose like ('%Avibactam%') or a.LocalDrugNameWithDose like ('%Ceftibuten%') or a.LocalDrugNameWithDose like ('%Ceftizoxime%') or a.LocalDrugNameWithDose like ('%Tazobactam%') or 
a.LocalDrugNameWithDose like ('%Ceftriaxone%') or a.LocalDrugNameWithDose like ('%Cefuroxime%') or a.LocalDrugNameWithDose like ('%Cephalexin%') or a.LocalDrugNameWithDose like ('%Cephalothin%') or a.LocalDrugNameWithDose like ('%Cephapirin%') or 
a.LocalDrugNameWithDose like ('%Cephradine%') or a.LocalDrugNameWithDose like ('%Chloramphenicol%') or a.LocalDrugNameWithDose like ('%Cidofovir%') or a.LocalDrugNameWithDose like ('%Cinoxacin%') or 
a.LocalDrugNameWithDose like ('%Ciprofloxacin%') or a.LocalDrugNameWithDose like ('%Clindamycin%') or a.LocalDrugNameWithDose like ('%Cloxacillin%') or a.LocalDrugNameWithDose like ('%Colistin%') or 
a.LocalDrugNameWithDose like ('%Colistimethate%') or a.LocalDrugNameWithDose like ('%Dalbavancin%') or a.LocalDrugNameWithDose like ('%Daptomycin%') or a.LocalDrugNameWithDose like ('%Dicloxacillin%') or 
a.LocalDrugNameWithDose like ('%Doripenem%') or a.LocalDrugNameWithDose like ('%Doxycycline%') or a.LocalDrugNameWithDose like ('%Ertapenem%') or 
a.LocalDrugNameWithDose like ('%Fidaxomicin%') or a.LocalDrugNameWithDose like ('%Fluconazole%') or a.LocalDrugNameWithDose like ('%Foscarnet%') or a.LocalDrugNameWithDose like ('%Fosfomycin%') or a.LocalDrugNameWithDose like ('%Ganciclovir%') or 
a.LocalDrugNameWithDose like ('%Gatifloxacin%') or a.LocalDrugNameWithDose like ('%Gentamicin%') or a.LocalDrugNameWithDose like ('%Imipenem%') or a.LocalDrugNameWithDose like ('%Itraconazole%') or 
a.LocalDrugNameWithDose like ('%Kanamycin%') or a.LocalDrugNameWithDose like ('%Levofloxacin%') or a.LocalDrugNameWithDose like ('%Lincomycin%') or a.LocalDrugNameWithDose like ('%Linezolid%') or 
a.LocalDrugNameWithDose like ('%Meropenem%') or a.LocalDrugNameWithDose like ('%Methicillin%') or a.LocalDrugNameWithDose like ('%Metronidazole%') or a.LocalDrugNameWithDose like ('%Mezlocillin%') or 
a.LocalDrugNameWithDose like ('%Micafungin%') or a.LocalDrugNameWithDose like ('%Minocycline%') or a.LocalDrugNameWithDose like ('%Moxifloxacin%') or a.LocalDrugNameWithDose like ('%Nafcillin%') or 
a.LocalDrugNameWithDose like ('%Nitrofurantoin%') or a.LocalDrugNameWithDose like ('%Norfloxacin%') or a.LocalDrugNameWithDose like ('%Ofloxacin%') or a.LocalDrugNameWithDose like ('%Oritavancin%') or 
a.LocalDrugNameWithDose like ('%Oxacillin%') or a.LocalDrugNameWithDose like ('%Penicillin%') or a.LocalDrugNameWithDose like ('%Peramivir%') or a.LocalDrugNameWithDose like ('%Piperacillin%') or 
a.LocalDrugNameWithDose like ('%Tazobactam%') or a.LocalDrugNameWithDose like ('%Pivampicillin%') or a.LocalDrugNameWithDose like ('%Polymyxin B%') or a.LocalDrugNameWithDose like ('%Posaconazole%') or 
a.LocalDrugNameWithDose like ('%Quinupristin%') or a.LocalDrugNameWithDose like ('%Dalfopristin%') or a.LocalDrugNameWithDose like ('%Streptomycin%') or a.LocalDrugNameWithDose like ('%Sulfadiazine%') or 
a.LocalDrugNameWithDose like ('%trimethoprim%') or a.LocalDrugNameWithDose like ('%Sulfamethoxazole%') or a.LocalDrugNameWithDose like ('%Sulfisoxazole%') or 
a.LocalDrugNameWithDose like ('%Tedizolid%') or a.LocalDrugNameWithDose like ('%Telavancin%') or a.LocalDrugNameWithDose like ('%Telithromycin%') or a.LocalDrugNameWithDose like ('%Tetracycline%') or 
a.LocalDrugNameWithDose like ('%Ticarcillin%') or a.LocalDrugNameWithDose like ('%Clavulanate%') or a.LocalDrugNameWithDose like ('%Tigecycline%') or a.LocalDrugNameWithDose like ('%Tobramycin%') or 
a.LocalDrugNameWithDose like ('%Trimethoprim%') or a.LocalDrugNameWithDose like ('%Sulfamethoxazole%') or a.LocalDrugNameWithDose like ('%Vancomycin%') or a.LocalDrugNameWithDose like ('%Voriconazole%')
or
a.drugnamewithoutdose like ('%Acyclovir%') or a.drugnamewithoutdose like ('%Amikacin%') or a.drugnamewithoutdose like ('%Amoxicillin%') or a.drugnamewithoutdose like ('%Clavulanate%') or 
a.drugnamewithoutdose like ('%Amphotericin B%') or a.drugnamewithoutdose like ('%Ampicillin%') or a.drugnamewithoutdose like ('%Sulbactam%') or 
a.drugnamewithoutdose like ('%Anidulafungin%') or a.drugnamewithoutdose like ('%Azithromycin%') or a.drugnamewithoutdose like ('%Aztreonam%') or a.drugnamewithoutdose like ('%Caspofungin%') or 
a.drugnamewithoutdose like ('%Cefaclor%') or a.drugnamewithoutdose like ('%Cefadroxil%') or a.drugnamewithoutdose like ('%Cefamandole%') or a.drugnamewithoutdose like ('%Cefazolin%') or 
a.drugnamewithoutdose like ('%Cefdinir%') or a.drugnamewithoutdose like ('%Cefditoren%') or a.drugnamewithoutdose like ('%Cefepime%') or a.drugnamewithoutdose like ('%Cefixime%') or 
a.drugnamewithoutdose like ('%Cefmetazole%') or a.drugnamewithoutdose like ('%Cefonicid%') or a.drugnamewithoutdose like ('%Cefoperazone%') or a.drugnamewithoutdose like ('%Cefotaxime%') or 
a.drugnamewithoutdose like ('%Cefotetan%') or a.drugnamewithoutdose like ('%Cefoxitin%') or a.drugnamewithoutdose like ('%Cefpodoxime%') or 
a.drugnamewithoutdose like ('%Cefprozil%') or a.drugnamewithoutdose like ('%Ceftaroline%') or a.drugnamewithoutdose like ('%Ceftazidime%') or 
a.drugnamewithoutdose like ('%Avibactam%') or a.drugnamewithoutdose like ('%Ceftibuten%') or a.drugnamewithoutdose like ('%Ceftizoxime%') or a.drugnamewithoutdose like ('%Tazobactam%') or 
a.drugnamewithoutdose like ('%Ceftriaxone%') or a.drugnamewithoutdose like ('%Cefuroxime%') or a.drugnamewithoutdose like ('%Cephalexin%') or a.drugnamewithoutdose like ('%Cephalothin%') or a.drugnamewithoutdose like ('%Cephapirin%') or 
a.drugnamewithoutdose like ('%Cephradine%') or a.drugnamewithoutdose like ('%Chloramphenicol%') or a.drugnamewithoutdose like ('%Cidofovir%') or a.drugnamewithoutdose like ('%Cinoxacin%') or 
a.drugnamewithoutdose like ('%Ciprofloxacin%') or a.drugnamewithoutdose like ('%Clindamycin%') or a.drugnamewithoutdose like ('%Cloxacillin%') or a.drugnamewithoutdose like ('%Colistin%') or 
a.drugnamewithoutdose like ('%Colistimethate%') or a.drugnamewithoutdose like ('%Dalbavancin%') or a.drugnamewithoutdose like ('%Daptomycin%') or a.drugnamewithoutdose like ('%Dicloxacillin%') or 
a.drugnamewithoutdose like ('%Doripenem%') or a.drugnamewithoutdose like ('%Doxycycline%') or a.drugnamewithoutdose like ('%Ertapenem%') or 
a.drugnamewithoutdose like ('%Fidaxomicin%') or a.drugnamewithoutdose like ('%Fluconazole%') or a.drugnamewithoutdose like ('%Foscarnet%') or a.drugnamewithoutdose like ('%Fosfomycin%') or a.drugnamewithoutdose like ('%Ganciclovir%') or 
a.drugnamewithoutdose like ('%Gatifloxacin%') or a.drugnamewithoutdose like ('%Gentamicin%') or a.drugnamewithoutdose like ('%Imipenem%') or a.drugnamewithoutdose like ('%Itraconazole%') or 
a.drugnamewithoutdose like ('%Kanamycin%') or a.drugnamewithoutdose like ('%Levofloxacin%') or a.drugnamewithoutdose like ('%Lincomycin%') or a.drugnamewithoutdose like ('%Linezolid%') or 
a.drugnamewithoutdose like ('%Meropenem%') or a.drugnamewithoutdose like ('%Methicillin%') or a.drugnamewithoutdose like ('%Metronidazole%') or a.drugnamewithoutdose like ('%Mezlocillin%') or 
a.drugnamewithoutdose like ('%Micafungin%') or a.drugnamewithoutdose like ('%Minocycline%') or a.drugnamewithoutdose like ('%Moxifloxacin%') or a.drugnamewithoutdose like ('%Nafcillin%') or 
a.drugnamewithoutdose like ('%Nitrofurantoin%') or a.drugnamewithoutdose like ('%Norfloxacin%') or a.drugnamewithoutdose like ('%Ofloxacin%') or a.drugnamewithoutdose like ('%Oritavancin%') or 
a.drugnamewithoutdose like ('%Oxacillin%') or a.drugnamewithoutdose like ('%Penicillin%') or a.drugnamewithoutdose like ('%Peramivir%') or a.drugnamewithoutdose like ('%Piperacillin%') or 
a.drugnamewithoutdose like ('%Tazobactam%') or a.drugnamewithoutdose like ('%Pivampicillin%') or a.drugnamewithoutdose like ('%Polymyxin B%') or a.drugnamewithoutdose like ('%Posaconazole%') or 
a.drugnamewithoutdose like ('%Quinupristin%') or a.drugnamewithoutdose like ('%Dalfopristin%') or a.drugnamewithoutdose like ('%Streptomycin%') or a.drugnamewithoutdose like ('%Sulfadiazine%') or 
a.drugnamewithoutdose like ('%trimethoprim%') or a.drugnamewithoutdose like ('%Sulfamethoxazole%') or a.drugnamewithoutdose like ('%Sulfisoxazole%') or 
a.drugnamewithoutdose like ('%Tedizolid%') or a.drugnamewithoutdose like ('%Telavancin%') or a.drugnamewithoutdose like ('%Telithromycin%') or a.drugnamewithoutdose like ('%Tetracycline%') or 
a.drugnamewithoutdose like ('%Ticarcillin%') or a.drugnamewithoutdose like ('%Clavulanate%') or a.drugnamewithoutdose like ('%Tigecycline%') or a.drugnamewithoutdose like ('%Tobramycin%') or 
a.drugnamewithoutdose like ('%Trimethoprim%') or a.drugnamewithoutdose like ('%Sulfamethoxazole%') or a.drugnamewithoutdose like ('%Vancomycin%') or a.drugnamewithoutdose like ('%Voriconazole%')
or
a.NationalDrug like ('%Acyclovir%') or a.NationalDrug like ('%Amikacin%') or a.NationalDrug like ('%Amoxicillin%') or a.NationalDrug like ('%Clavulanate%') or 
a.NationalDrug like ('%Amphotericin B%') or a.NationalDrug like ('%Ampicillin%') or a.NationalDrug like ('%Sulbactam%') or 
a.NationalDrug like ('%Anidulafungin%') or a.NationalDrug like ('%Azithromycin%') or a.NationalDrug like ('%Aztreonam%') or a.NationalDrug like ('%Caspofungin%') or 
a.NationalDrug like ('%Cefaclor%') or a.NationalDrug like ('%Cefadroxil%') or a.NationalDrug like ('%Cefamandole%') or a.NationalDrug like ('%Cefazolin%') or 
a.NationalDrug like ('%Cefdinir%') or a.NationalDrug like ('%Cefditoren%') or a.NationalDrug like ('%Cefepime%') or a.NationalDrug like ('%Cefixime%') or 
a.NationalDrug like ('%Cefmetazole%') or a.NationalDrug like ('%Cefonicid%') or a.NationalDrug like ('%Cefoperazone%') or a.NationalDrug like ('%Cefotaxime%') or 
a.NationalDrug like ('%Cefotetan%') or a.NationalDrug like ('%Cefoxitin%') or a.NationalDrug like ('%Cefpodoxime%') or 
a.NationalDrug like ('%Cefprozil%') or a.NationalDrug like ('%Ceftaroline%') or a.NationalDrug like ('%Ceftazidime%') or 
a.NationalDrug like ('%Avibactam%') or a.NationalDrug like ('%Ceftibuten%') or a.NationalDrug like ('%Ceftizoxime%') or a.NationalDrug like ('%Tazobactam%') or 
a.NationalDrug like ('%Ceftriaxone%') or a.NationalDrug like ('%Cefuroxime%') or a.NationalDrug like ('%Cephalexin%') or a.NationalDrug like ('%Cephalothin%') or a.NationalDrug like ('%Cephapirin%') or 
a.NationalDrug like ('%Cephradine%') or a.NationalDrug like ('%Chloramphenicol%') or a.NationalDrug like ('%Cidofovir%') or a.NationalDrug like ('%Cinoxacin%') or 
a.NationalDrug like ('%Ciprofloxacin%') or a.NationalDrug like ('%Clindamycin%') or a.NationalDrug like ('%Cloxacillin%') or a.NationalDrug like ('%Colistin%') or 
a.NationalDrug like ('%Colistimethate%') or a.NationalDrug like ('%Dalbavancin%') or a.NationalDrug like ('%Daptomycin%') or a.NationalDrug like ('%Dicloxacillin%') or 
a.NationalDrug like ('%Doripenem%') or a.NationalDrug like ('%Doxycycline%') or a.NationalDrug like ('%Ertapenem%') or 
a.NationalDrug like ('%Fidaxomicin%') or a.NationalDrug like ('%Fluconazole%') or a.NationalDrug like ('%Foscarnet%') or a.NationalDrug like ('%Fosfomycin%') or a.NationalDrug like ('%Ganciclovir%') or 
a.NationalDrug like ('%Gatifloxacin%') or a.NationalDrug like ('%Gentamicin%') or a.NationalDrug like ('%Imipenem%') or a.NationalDrug like ('%Itraconazole%') or 
a.NationalDrug like ('%Kanamycin%') or a.NationalDrug like ('%Levofloxacin%') or a.NationalDrug like ('%Lincomycin%') or a.NationalDrug like ('%Linezolid%') or 
a.NationalDrug like ('%Meropenem%') or a.NationalDrug like ('%Methicillin%') or a.NationalDrug like ('%Metronidazole%') or a.NationalDrug like ('%Mezlocillin%') or 
a.NationalDrug like ('%Micafungin%') or a.NationalDrug like ('%Minocycline%') or a.NationalDrug like ('%Moxifloxacin%') or a.NationalDrug like ('%Nafcillin%') or 
a.NationalDrug like ('%Nitrofurantoin%') or a.NationalDrug like ('%Norfloxacin%') or a.NationalDrug like ('%Ofloxacin%') or a.NationalDrug like ('%Oritavancin%') or 
a.NationalDrug like ('%Oxacillin%') or a.NationalDrug like ('%Penicillin%') or a.NationalDrug like ('%Peramivir%') or a.NationalDrug like ('%Piperacillin%') or 
a.NationalDrug like ('%Tazobactam%') or a.NationalDrug like ('%Pivampicillin%') or a.NationalDrug like ('%Polymyxin B%') or a.NationalDrug like ('%Posaconazole%') or 
a.NationalDrug like ('%Quinupristin%') or a.NationalDrug like ('%Dalfopristin%') or a.NationalDrug like ('%Streptomycin%') or a.NationalDrug like ('%Sulfadiazine%') or 
a.NationalDrug like ('%trimethoprim%') or a.NationalDrug like ('%Sulfamethoxazole%') or a.NationalDrug like ('%Sulfisoxazole%') or 
a.NationalDrug like ('%Tedizolid%') or a.NationalDrug like ('%Telavancin%') or a.NationalDrug like ('%Telithromycin%') or a.NationalDrug like ('%Tetracycline%') or 
a.NationalDrug like ('%Ticarcillin%') or a.NationalDrug like ('%Clavulanate%') or a.NationalDrug like ('%Tigecycline%') or a.NationalDrug like ('%Tobramycin%') or 
a.NationalDrug like ('%Trimethoprim%') or a.NationalDrug like ('%Sulfamethoxazole%') or a.NationalDrug like ('%Vancomycin%') or a.NationalDrug like ('%Voriconazole%')
or
a.NationalDrugNameWithDose like ('%Acyclovir%') or a.NationalDrugNameWithDose like ('%Amikacin%') or a.NationalDrugNameWithDose like ('%Amoxicillin%') or a.NationalDrugNameWithDose like ('%Clavulanate%') or 
a.NationalDrugNameWithDose like ('%Amphotericin B%') or a.NationalDrugNameWithDose like ('%Ampicillin%') or a.NationalDrugNameWithDose like ('%Sulbactam%') or 
a.NationalDrugNameWithDose like ('%Anidulafungin%') or a.NationalDrugNameWithDose like ('%Azithromycin%') or a.NationalDrugNameWithDose like ('%Aztreonam%') or a.NationalDrugNameWithDose like ('%Caspofungin%') or 
a.NationalDrugNameWithDose like ('%Cefaclor%') or a.NationalDrugNameWithDose like ('%Cefadroxil%') or a.NationalDrugNameWithDose like ('%Cefamandole%') or a.NationalDrugNameWithDose like ('%Cefazolin%') or 
a.NationalDrugNameWithDose like ('%Cefdinir%') or a.NationalDrugNameWithDose like ('%Cefditoren%') or a.NationalDrugNameWithDose like ('%Cefepime%') or a.NationalDrugNameWithDose like ('%Cefixime%') or 
a.NationalDrugNameWithDose like ('%Cefmetazole%') or a.NationalDrugNameWithDose like ('%Cefonicid%') or a.NationalDrugNameWithDose like ('%Cefoperazone%') or a.NationalDrugNameWithDose like ('%Cefotaxime%') or 
a.NationalDrugNameWithDose like ('%Cefotetan%') or a.NationalDrugNameWithDose like ('%Cefoxitin%') or a.NationalDrugNameWithDose like ('%Cefpodoxime%') or 
a.NationalDrugNameWithDose like ('%Cefprozil%') or a.NationalDrugNameWithDose like ('%Ceftaroline%') or a.NationalDrugNameWithDose like ('%Ceftazidime%') or 
a.NationalDrugNameWithDose like ('%Avibactam%') or a.NationalDrugNameWithDose like ('%Ceftibuten%') or a.NationalDrugNameWithDose like ('%Ceftizoxime%') or a.NationalDrugNameWithDose like ('%Tazobactam%') or 
a.NationalDrugNameWithDose like ('%Ceftriaxone%') or a.NationalDrugNameWithDose like ('%Cefuroxime%') or a.NationalDrugNameWithDose like ('%Cephalexin%') or a.NationalDrugNameWithDose like ('%Cephalothin%') or a.NationalDrugNameWithDose like ('%Cephapirin%') or 
a.NationalDrugNameWithDose like ('%Cephradine%') or a.NationalDrugNameWithDose like ('%Chloramphenicol%') or a.NationalDrugNameWithDose like ('%Cidofovir%') or a.NationalDrugNameWithDose like ('%Cinoxacin%') or 
a.NationalDrugNameWithDose like ('%Ciprofloxacin%') or a.NationalDrugNameWithDose like ('%Clindamycin%') or a.NationalDrugNameWithDose like ('%Cloxacillin%') or a.NationalDrugNameWithDose like ('%Colistin%') or 
a.NationalDrugNameWithDose like ('%Colistimethate%') or a.NationalDrugNameWithDose like ('%Dalbavancin%') or a.NationalDrugNameWithDose like ('%Daptomycin%') or a.NationalDrugNameWithDose like ('%Dicloxacillin%') or 
a.NationalDrugNameWithDose like ('%Doripenem%') or a.NationalDrugNameWithDose like ('%Doxycycline%') or a.NationalDrugNameWithDose like ('%Ertapenem%') or 
a.NationalDrugNameWithDose like ('%Fidaxomicin%') or a.NationalDrugNameWithDose like ('%Fluconazole%') or a.NationalDrugNameWithDose like ('%Foscarnet%') or a.NationalDrugNameWithDose like ('%Fosfomycin%') or a.NationalDrugNameWithDose like ('%Ganciclovir%') or 
a.NationalDrugNameWithDose like ('%Gatifloxacin%') or a.NationalDrugNameWithDose like ('%Gentamicin%') or a.NationalDrugNameWithDose like ('%Imipenem%') or a.NationalDrugNameWithDose like ('%Itraconazole%') or 
a.NationalDrugNameWithDose like ('%Kanamycin%') or a.NationalDrugNameWithDose like ('%Levofloxacin%') or a.NationalDrugNameWithDose like ('%Lincomycin%') or a.NationalDrugNameWithDose like ('%Linezolid%') or 
a.NationalDrugNameWithDose like ('%Meropenem%') or a.NationalDrugNameWithDose like ('%Methicillin%') or a.NationalDrugNameWithDose like ('%Metronidazole%') or a.NationalDrugNameWithDose like ('%Mezlocillin%') or 
a.NationalDrugNameWithDose like ('%Micafungin%') or a.NationalDrugNameWithDose like ('%Minocycline%') or a.NationalDrugNameWithDose like ('%Moxifloxacin%') or a.NationalDrugNameWithDose like ('%Nafcillin%') or 
a.NationalDrugNameWithDose like ('%Nitrofurantoin%') or a.NationalDrugNameWithDose like ('%Norfloxacin%') or a.NationalDrugNameWithDose like ('%Ofloxacin%') or a.NationalDrugNameWithDose like ('%Oritavancin%') or 
a.NationalDrugNameWithDose like ('%Oxacillin%') or a.NationalDrugNameWithDose like ('%Penicillin%') or a.NationalDrugNameWithDose like ('%Peramivir%') or a.NationalDrugNameWithDose like ('%Piperacillin%') or 
a.NationalDrugNameWithDose like ('%Tazobactam%') or a.NationalDrugNameWithDose like ('%Pivampicillin%') or a.NationalDrugNameWithDose like ('%Polymyxin B%') or a.NationalDrugNameWithDose like ('%Posaconazole%') or 
a.NationalDrugNameWithDose like ('%Quinupristin%') or a.NationalDrugNameWithDose like ('%Dalfopristin%') or a.NationalDrugNameWithDose like ('%Streptomycin%') or a.NationalDrugNameWithDose like ('%Sulfadiazine%') or 
a.NationalDrugNameWithDose like ('%trimethoprim%') or a.NationalDrugNameWithDose like ('%Sulfamethoxazole%') or a.NationalDrugNameWithDose like ('%Sulfisoxazole%') or 
a.NationalDrugNameWithDose like ('%Tedizolid%') or a.NationalDrugNameWithDose like ('%Telavancin%') or a.NationalDrugNameWithDose like ('%Telithromycin%') or a.NationalDrugNameWithDose like ('%Tetracycline%') or 
a.NationalDrugNameWithDose like ('%Ticarcillin%') or a.NationalDrugNameWithDose like ('%Clavulanate%') or a.NationalDrugNameWithDose like ('%Tigecycline%') or a.NationalDrugNameWithDose like ('%Tobramycin%') or 
a.NationalDrugNameWithDose like ('%Trimethoprim%') or a.NationalDrugNameWithDose like ('%Sulfamethoxazole%') or a.NationalDrugNameWithDose like ('%Vancomycin%') or a.NationalDrugNameWithDose like ('%Voriconazole%')



/* get IVSolutionIngredientSID & LocalDrugSID from [CDWWORK].[Dim].[IVSolutionIngredient]*/
SELECT a.IVSolutionIngredientSID, a.LocalDrugNameWithDose, a.Sta3n, a.LocalDrugSID, a.Volume, a.IVSolutionFirstIngredientPrintName
into #IVSolutionIngredient
FROM  [CDWWORK].[Dim].[IVSolutionIngredient] AS A 
WHERE a.LocalDrugNameWithDose like ('%Acyclovir%') or  a.LocalDrugNameWithDose like ('%Amikacin%') or  a.LocalDrugNameWithDose like ('%Amoxicillin%') or  a.LocalDrugNameWithDose like ('%Clavulanate%') or 
 a.LocalDrugNameWithDose like ('%Amphotericin B%') or  a.LocalDrugNameWithDose like ('%Ampicillin%') or  a.LocalDrugNameWithDose like ('%Sulbactam%') or 
 a.LocalDrugNameWithDose like ('%Anidulafungin%') or  a.LocalDrugNameWithDose like ('%Azithromycin%') or  a.LocalDrugNameWithDose like ('%Aztreonam%') or  a.LocalDrugNameWithDose like ('%Caspofungin%') or 
 a.LocalDrugNameWithDose like ('%Cefaclor%') or  a.LocalDrugNameWithDose like ('%Cefadroxil%') or  a.LocalDrugNameWithDose like ('%Cefamandole%') or  a.LocalDrugNameWithDose like ('%Cefazolin%') or 
 a.LocalDrugNameWithDose like ('%Cefdinir%') or  a.LocalDrugNameWithDose like ('%Cefditoren%') or  a.LocalDrugNameWithDose like ('%Cefepime%') or  a.LocalDrugNameWithDose like ('%Cefixime%') or 
 a.LocalDrugNameWithDose like ('%Cefmetazole%') or  a.LocalDrugNameWithDose like ('%Cefonicid%') or  a.LocalDrugNameWithDose like ('%Cefoperazone%') or  a.LocalDrugNameWithDose like ('%Cefotaxime%') or 
 a.LocalDrugNameWithDose like ('%Cefotetan%') or  a.LocalDrugNameWithDose like ('%Cefoxitin%') or  a.LocalDrugNameWithDose like ('%Cefpodoxime%') or 
 a.LocalDrugNameWithDose like ('%Cefprozil%') or  a.LocalDrugNameWithDose like ('%Ceftaroline%') or  a.LocalDrugNameWithDose like ('%Ceftazidime%') or 
 a.LocalDrugNameWithDose like ('%Avibactam%') or  a.LocalDrugNameWithDose like ('%Ceftibuten%') or  a.LocalDrugNameWithDose like ('%Ceftizoxime%') or  a.LocalDrugNameWithDose like ('%Tazobactam%') or 
 a.LocalDrugNameWithDose like ('%Ceftriaxone%') or  a.LocalDrugNameWithDose like ('%Cefuroxime%') or  a.LocalDrugNameWithDose like ('%Cephalexin%') or  a.LocalDrugNameWithDose like ('%Cephalothin%') or  a.LocalDrugNameWithDose like ('%Cephapirin%') or 
 a.LocalDrugNameWithDose like ('%Cephradine%') or  a.LocalDrugNameWithDose like ('%Chloramphenicol%') or  a.LocalDrugNameWithDose like ('%Cidofovir%') or  a.LocalDrugNameWithDose like ('%Cinoxacin%') or 
 a.LocalDrugNameWithDose like ('%Ciprofloxacin%') or  a.LocalDrugNameWithDose like ('%Clindamycin%') or  a.LocalDrugNameWithDose like ('%Cloxacillin%') or  a.LocalDrugNameWithDose like ('%Colistin%') or 
 a.LocalDrugNameWithDose like ('%Colistimethate%') or  a.LocalDrugNameWithDose like ('%Dalbavancin%') or  a.LocalDrugNameWithDose like ('%Daptomycin%') or  a.LocalDrugNameWithDose like ('%Dicloxacillin%') or 
 a.LocalDrugNameWithDose like ('%Doripenem%') or  a.LocalDrugNameWithDose like ('%Doxycycline%') or  a.LocalDrugNameWithDose like ('%Ertapenem%') or 
 a.LocalDrugNameWithDose like ('%Fidaxomicin%') or  a.LocalDrugNameWithDose like ('%Fluconazole%') or  a.LocalDrugNameWithDose like ('%Foscarnet%') or  a.LocalDrugNameWithDose like ('%Fosfomycin%') or  a.LocalDrugNameWithDose like ('%Ganciclovir%') or 
 a.LocalDrugNameWithDose like ('%Gatifloxacin%') or  a.LocalDrugNameWithDose like ('%Gentamicin%') or  a.LocalDrugNameWithDose like ('%Imipenem%') or  a.LocalDrugNameWithDose like ('%Itraconazole%') or 
 a.LocalDrugNameWithDose like ('%Kanamycin%') or  a.LocalDrugNameWithDose like ('%Levofloxacin%') or  a.LocalDrugNameWithDose like ('%Lincomycin%') or  a.LocalDrugNameWithDose like ('%Linezolid%') or 
 a.LocalDrugNameWithDose like ('%Meropenem%') or  a.LocalDrugNameWithDose like ('%Methicillin%') or  a.LocalDrugNameWithDose like ('%Metronidazole%') or  a.LocalDrugNameWithDose like ('%Mezlocillin%') or 
 a.LocalDrugNameWithDose like ('%Micafungin%') or  a.LocalDrugNameWithDose like ('%Minocycline%') or  a.LocalDrugNameWithDose like ('%Moxifloxacin%') or  a.LocalDrugNameWithDose like ('%Nafcillin%') or 
 a.LocalDrugNameWithDose like ('%Nitrofurantoin%') or  a.LocalDrugNameWithDose like ('%Norfloxacin%') or  a.LocalDrugNameWithDose like ('%Ofloxacin%') or  a.LocalDrugNameWithDose like ('%Oritavancin%') or 
 a.LocalDrugNameWithDose like ('%Oxacillin%') or  a.LocalDrugNameWithDose like ('%Penicillin%') or  a.LocalDrugNameWithDose like ('%Peramivir%') or  a.LocalDrugNameWithDose like ('%Piperacillin%') or 
 a.LocalDrugNameWithDose like ('%Tazobactam%') or  a.LocalDrugNameWithDose like ('%Pivampicillin%') or  a.LocalDrugNameWithDose like ('%Polymyxin B%') or  a.LocalDrugNameWithDose like ('%Posaconazole%') or 
 a.LocalDrugNameWithDose like ('%Quinupristin%') or  a.LocalDrugNameWithDose like ('%Dalfopristin%') or  a.LocalDrugNameWithDose like ('%Streptomycin%') or  a.LocalDrugNameWithDose like ('%Sulfadiazine%') or 
 a.LocalDrugNameWithDose like ('%trimethoprim%') or  a.LocalDrugNameWithDose like ('%Sulfamethoxazole%') or  a.LocalDrugNameWithDose like ('%Sulfisoxazole%') or 
 a.LocalDrugNameWithDose like ('%Tedizolid%') or  a.LocalDrugNameWithDose like ('%Telavancin%') or  a.LocalDrugNameWithDose like ('%Telithromycin%') or  a.LocalDrugNameWithDose like ('%Tetracycline%') or 
 a.LocalDrugNameWithDose like ('%Ticarcillin%') or  a.LocalDrugNameWithDose like ('%Clavulanate%') or  a.LocalDrugNameWithDose like ('%Tigecycline%') or  a.LocalDrugNameWithDose like ('%Tobramycin%') or 
 a.LocalDrugNameWithDose like ('%Trimethoprim%') or  a.LocalDrugNameWithDose like ('%Sulfamethoxazole%') or  a.LocalDrugNameWithDose like ('%Vancomycin%') or  a.LocalDrugNameWithDose like ('%Voriconazole%')
OR
a.IVSolutionFirstIngredientPrintName like ('%Acyclovir%') or a.IVSolutionFirstIngredientPrintName like ('%Amikacin%') or a.IVSolutionFirstIngredientPrintName like ('%Amoxicillin%') or a.IVSolutionFirstIngredientPrintName like ('%Clavulanate%') or 
a.IVSolutionFirstIngredientPrintName like ('%Amphotericin B%') or a.IVSolutionFirstIngredientPrintName like ('%Ampicillin%') or a.IVSolutionFirstIngredientPrintName like ('%Sulbactam%') or 
a.IVSolutionFirstIngredientPrintName like ('%Anidulafungin%') or a.IVSolutionFirstIngredientPrintName like ('%Azithromycin%') or a.IVSolutionFirstIngredientPrintName like ('%Aztreonam%') or a.IVSolutionFirstIngredientPrintName like ('%Caspofungin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cefaclor%') or a.IVSolutionFirstIngredientPrintName like ('%Cefadroxil%') or a.IVSolutionFirstIngredientPrintName like ('%Cefamandole%') or a.IVSolutionFirstIngredientPrintName like ('%Cefazolin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cefdinir%') or a.IVSolutionFirstIngredientPrintName like ('%Cefditoren%') or a.IVSolutionFirstIngredientPrintName like ('%Cefepime%') or a.IVSolutionFirstIngredientPrintName like ('%Cefixime%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cefmetazole%') or a.IVSolutionFirstIngredientPrintName like ('%Cefonicid%') or a.IVSolutionFirstIngredientPrintName like ('%Cefoperazone%') or a.IVSolutionFirstIngredientPrintName like ('%Cefotaxime%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cefotetan%') or a.IVSolutionFirstIngredientPrintName like ('%Cefoxitin%') or a.IVSolutionFirstIngredientPrintName like ('%Cefpodoxime%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cefprozil%') or a.IVSolutionFirstIngredientPrintName like ('%Ceftaroline%') or a.IVSolutionFirstIngredientPrintName like ('%Ceftazidime%') or 
a.IVSolutionFirstIngredientPrintName like ('%Avibactam%') or a.IVSolutionFirstIngredientPrintName like ('%Ceftibuten%') or a.IVSolutionFirstIngredientPrintName like ('%Ceftizoxime%') or a.IVSolutionFirstIngredientPrintName like ('%Tazobactam%') or 
a.IVSolutionFirstIngredientPrintName like ('%Ceftriaxone%') or a.IVSolutionFirstIngredientPrintName like ('%Cefuroxime%') or a.IVSolutionFirstIngredientPrintName like ('%Cephalexin%') or a.IVSolutionFirstIngredientPrintName like ('%Cephalothin%') or a.IVSolutionFirstIngredientPrintName like ('%Cephapirin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cephradine%') or a.IVSolutionFirstIngredientPrintName like ('%Chloramphenicol%') or a.IVSolutionFirstIngredientPrintName like ('%Cidofovir%') or a.IVSolutionFirstIngredientPrintName like ('%Cinoxacin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Ciprofloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Clindamycin%') or a.IVSolutionFirstIngredientPrintName like ('%Cloxacillin%') or a.IVSolutionFirstIngredientPrintName like ('%Colistin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Colistimethate%') or a.IVSolutionFirstIngredientPrintName like ('%Dalbavancin%') or a.IVSolutionFirstIngredientPrintName like ('%Daptomycin%') or a.IVSolutionFirstIngredientPrintName like ('%Dicloxacillin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Doripenem%') or a.IVSolutionFirstIngredientPrintName like ('%Doxycycline%') or a.IVSolutionFirstIngredientPrintName like ('%Ertapenem%') or 
a.IVSolutionFirstIngredientPrintName like ('%Fidaxomicin%') or a.IVSolutionFirstIngredientPrintName like ('%Fluconazole%') or a.IVSolutionFirstIngredientPrintName like ('%Foscarnet%') or a.IVSolutionFirstIngredientPrintName like ('%Fosfomycin%') or a.IVSolutionFirstIngredientPrintName like ('%Ganciclovir%') or 
a.IVSolutionFirstIngredientPrintName like ('%Gatifloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Gentamicin%') or a.IVSolutionFirstIngredientPrintName like ('%Imipenem%') or a.IVSolutionFirstIngredientPrintName like ('%Itraconazole%') or 
a.IVSolutionFirstIngredientPrintName like ('%Kanamycin%') or a.IVSolutionFirstIngredientPrintName like ('%Levofloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Lincomycin%') or a.IVSolutionFirstIngredientPrintName like ('%Linezolid%') or 
a.IVSolutionFirstIngredientPrintName like ('%Meropenem%') or a.IVSolutionFirstIngredientPrintName like ('%Methicillin%') or a.IVSolutionFirstIngredientPrintName like ('%Metronidazole%') or a.IVSolutionFirstIngredientPrintName like ('%Mezlocillin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Micafungin%') or a.IVSolutionFirstIngredientPrintName like ('%Minocycline%') or a.IVSolutionFirstIngredientPrintName like ('%Moxifloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Nafcillin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Nitrofurantoin%') or a.IVSolutionFirstIngredientPrintName like ('%Norfloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Ofloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Oritavancin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Oxacillin%') or a.IVSolutionFirstIngredientPrintName like ('%Penicillin%') or a.IVSolutionFirstIngredientPrintName like ('%Peramivir%') or a.IVSolutionFirstIngredientPrintName like ('%Piperacillin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Tazobactam%') or a.IVSolutionFirstIngredientPrintName like ('%Pivampicillin%') or a.IVSolutionFirstIngredientPrintName like ('%Polymyxin B%') or a.IVSolutionFirstIngredientPrintName like ('%Posaconazole%') or 
a.IVSolutionFirstIngredientPrintName like ('%Quinupristin%') or a.IVSolutionFirstIngredientPrintName like ('%Dalfopristin%') or a.IVSolutionFirstIngredientPrintName like ('%Streptomycin%') or a.IVSolutionFirstIngredientPrintName like ('%Sulfadiazine%') or 
a.IVSolutionFirstIngredientPrintName like ('%trimethoprim%') or a.IVSolutionFirstIngredientPrintName like ('%Sulfamethoxazole%') or a.IVSolutionFirstIngredientPrintName like ('%Sulfisoxazole%') or 
a.IVSolutionFirstIngredientPrintName like ('%Tedizolid%') or a.IVSolutionFirstIngredientPrintName like ('%Telavancin%') or a.IVSolutionFirstIngredientPrintName like ('%Telithromycin%') or a.IVSolutionFirstIngredientPrintName like ('%Tetracycline%') or 
a.IVSolutionFirstIngredientPrintName like ('%Ticarcillin%') or a.IVSolutionFirstIngredientPrintName like ('%Clavulanate%') or a.IVSolutionFirstIngredientPrintName like ('%Tigecycline%') or a.IVSolutionFirstIngredientPrintName like ('%Tobramycin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Trimethoprim%') or a.IVSolutionFirstIngredientPrintName like ('%Sulfamethoxazole%') or a.IVSolutionFirstIngredientPrintName like ('%Vancomycin%') or a.IVSolutionFirstIngredientPrintName like ('%Voriconazole%');



/*get IVAdditiveIngredientSID & LocalDrugSID from [CDWWORK].[Dim].[IVAdditiveIngredient]*/ 
SELECT a.IVAdditiveIngredientSID, a.LocalDrugNameWithDose, a.Sta3n,  a.LocalDrugSID, a.DrugUnit, a.IVAdditiveIngredientPrintName
into #IVAdditiveIngredient
FROM  [CDWWORK].[Dim].[IVAdditiveIngredient] AS A 
WHERE a.LocalDrugNameWithDose like ('%Acyclovir%') or a.LocalDrugNameWithDose like ('%Amikacin%') or a.LocalDrugNameWithDose like ('%Amoxicillin%') or a.LocalDrugNameWithDose like ('%Clavulanate%') or 
a.LocalDrugNameWithDose like ('%Amphotericin B%') or a.LocalDrugNameWithDose like ('%Ampicillin%') or a.LocalDrugNameWithDose like ('%Sulbactam%') or 
a.LocalDrugNameWithDose like ('%Anidulafungin%') or a.LocalDrugNameWithDose like ('%Azithromycin%') or a.LocalDrugNameWithDose like ('%Aztreonam%') or a.LocalDrugNameWithDose like ('%Caspofungin%') or 
a.LocalDrugNameWithDose like ('%Cefaclor%') or a.LocalDrugNameWithDose like ('%Cefadroxil%') or a.LocalDrugNameWithDose like ('%Cefamandole%') or a.LocalDrugNameWithDose like ('%Cefazolin%') or 
a.LocalDrugNameWithDose like ('%Cefdinir%') or a.LocalDrugNameWithDose like ('%Cefditoren%') or a.LocalDrugNameWithDose like ('%Cefepime%') or a.LocalDrugNameWithDose like ('%Cefixime%') or 
a.LocalDrugNameWithDose like ('%Cefmetazole%') or a.LocalDrugNameWithDose like ('%Cefonicid%') or a.LocalDrugNameWithDose like ('%Cefoperazone%') or a.LocalDrugNameWithDose like ('%Cefotaxime%') or 
a.LocalDrugNameWithDose like ('%Cefotetan%') or a.LocalDrugNameWithDose like ('%Cefoxitin%') or a.LocalDrugNameWithDose like ('%Cefpodoxime%') or 
a.LocalDrugNameWithDose like ('%Cefprozil%') or a.LocalDrugNameWithDose like ('%Ceftaroline%') or a.LocalDrugNameWithDose like ('%Ceftazidime%') or 
a.LocalDrugNameWithDose like ('%Avibactam%') or a.LocalDrugNameWithDose like ('%Ceftibuten%') or a.LocalDrugNameWithDose like ('%Ceftizoxime%') or a.LocalDrugNameWithDose like ('%Tazobactam%') or 
a.LocalDrugNameWithDose like ('%Ceftriaxone%') or a.LocalDrugNameWithDose like ('%Cefuroxime%') or a.LocalDrugNameWithDose like ('%Cephalexin%') or a.LocalDrugNameWithDose like ('%Cephalothin%') or a.LocalDrugNameWithDose like ('%Cephapirin%') or 
a.LocalDrugNameWithDose like ('%Cephradine%') or a.LocalDrugNameWithDose like ('%Chloramphenicol%') or a.LocalDrugNameWithDose like ('%Cidofovir%') or a.LocalDrugNameWithDose like ('%Cinoxacin%') or 
a.LocalDrugNameWithDose like ('%Ciprofloxacin%') or a.LocalDrugNameWithDose like ('%Clindamycin%') or a.LocalDrugNameWithDose like ('%Cloxacillin%') or a.LocalDrugNameWithDose like ('%Colistin%') or 
a.LocalDrugNameWithDose like ('%Colistimethate%') or a.LocalDrugNameWithDose like ('%Dalbavancin%') or a.LocalDrugNameWithDose like ('%Daptomycin%') or a.LocalDrugNameWithDose like ('%Dicloxacillin%') or 
a.LocalDrugNameWithDose like ('%Doripenem%') or a.LocalDrugNameWithDose like ('%Doxycycline%') or a.LocalDrugNameWithDose like ('%Ertapenem%') or 
a.LocalDrugNameWithDose like ('%Fidaxomicin%') or a.LocalDrugNameWithDose like ('%Fluconazole%') or a.LocalDrugNameWithDose like ('%Foscarnet%') or a.LocalDrugNameWithDose like ('%Fosfomycin%') or a.LocalDrugNameWithDose like ('%Ganciclovir%') or 
a.LocalDrugNameWithDose like ('%Gatifloxacin%') or a.LocalDrugNameWithDose like ('%Gentamicin%') or a.LocalDrugNameWithDose like ('%Imipenem%') or a.LocalDrugNameWithDose like ('%Itraconazole%') or 
a.LocalDrugNameWithDose like ('%Kanamycin%') or a.LocalDrugNameWithDose like ('%Levofloxacin%') or a.LocalDrugNameWithDose like ('%Lincomycin%') or a.LocalDrugNameWithDose like ('%Linezolid%') or 
a.LocalDrugNameWithDose like ('%Meropenem%') or a.LocalDrugNameWithDose like ('%Methicillin%') or a.LocalDrugNameWithDose like ('%Metronidazole%') or a.LocalDrugNameWithDose like ('%Mezlocillin%') or 
a.LocalDrugNameWithDose like ('%Micafungin%') or a.LocalDrugNameWithDose like ('%Minocycline%') or a.LocalDrugNameWithDose like ('%Moxifloxacin%') or a.LocalDrugNameWithDose like ('%Nafcillin%') or 
a.LocalDrugNameWithDose like ('%Nitrofurantoin%') or a.LocalDrugNameWithDose like ('%Norfloxacin%') or a.LocalDrugNameWithDose like ('%Ofloxacin%') or a.LocalDrugNameWithDose like ('%Oritavancin%') or 
a.LocalDrugNameWithDose like ('%Oxacillin%') or a.LocalDrugNameWithDose like ('%Penicillin%') or a.LocalDrugNameWithDose like ('%Peramivir%') or a.LocalDrugNameWithDose like ('%Piperacillin%') or 
a.LocalDrugNameWithDose like ('%Tazobactam%') or a.LocalDrugNameWithDose like ('%Pivampicillin%') or a.LocalDrugNameWithDose like ('%Polymyxin B%') or a.LocalDrugNameWithDose like ('%Posaconazole%') or 
a.LocalDrugNameWithDose like ('%Quinupristin%') or a.LocalDrugNameWithDose like ('%Dalfopristin%') or a.LocalDrugNameWithDose like ('%Streptomycin%') or a.LocalDrugNameWithDose like ('%Sulfadiazine%') or 
a.LocalDrugNameWithDose like ('%trimethoprim%') or a.LocalDrugNameWithDose like ('%Sulfamethoxazole%') or a.LocalDrugNameWithDose like ('%Sulfisoxazole%') or 
a.LocalDrugNameWithDose like ('%Tedizolid%') or a.LocalDrugNameWithDose like ('%Telavancin%') or a.LocalDrugNameWithDose like ('%Telithromycin%') or a.LocalDrugNameWithDose like ('%Tetracycline%') or 
a.LocalDrugNameWithDose like ('%Ticarcillin%') or a.LocalDrugNameWithDose like ('%Clavulanate%') or a.LocalDrugNameWithDose like ('%Tigecycline%') or a.LocalDrugNameWithDose like ('%Tobramycin%') or 
a.LocalDrugNameWithDose like ('%Trimethoprim%') or a.LocalDrugNameWithDose like ('%Sulfamethoxazole%') or a.LocalDrugNameWithDose like ('%Vancomycin%') or a.LocalDrugNameWithDose like ('%Voriconazole%')
or 
a.IVAdditiveIngredientPrintName like ('%Acyclovir%') or a.IVAdditiveIngredientPrintName like ('%Amikacin%') or a.IVAdditiveIngredientPrintName like ('%Amoxicillin%') or a.IVAdditiveIngredientPrintName like ('%Clavulanate%') or 
a.IVAdditiveIngredientPrintName like ('%Amphotericin B%') or a.IVAdditiveIngredientPrintName like ('%Ampicillin%') or a.IVAdditiveIngredientPrintName like ('%Sulbactam%') or 
a.IVAdditiveIngredientPrintName like ('%Anidulafungin%') or a.IVAdditiveIngredientPrintName like ('%Azithromycin%') or a.IVAdditiveIngredientPrintName like ('%Aztreonam%') or a.IVAdditiveIngredientPrintName like ('%Caspofungin%') or 
a.IVAdditiveIngredientPrintName like ('%Cefaclor%') or a.IVAdditiveIngredientPrintName like ('%Cefadroxil%') or a.IVAdditiveIngredientPrintName like ('%Cefamandole%') or a.IVAdditiveIngredientPrintName like ('%Cefazolin%') or 
a.IVAdditiveIngredientPrintName like ('%Cefdinir%') or a.IVAdditiveIngredientPrintName like ('%Cefditoren%') or a.IVAdditiveIngredientPrintName like ('%Cefepime%') or a.IVAdditiveIngredientPrintName like ('%Cefixime%') or 
a.IVAdditiveIngredientPrintName like ('%Cefmetazole%') or a.IVAdditiveIngredientPrintName like ('%Cefonicid%') or a.IVAdditiveIngredientPrintName like ('%Cefoperazone%') or a.IVAdditiveIngredientPrintName like ('%Cefotaxime%') or 
a.IVAdditiveIngredientPrintName like ('%Cefotetan%') or a.IVAdditiveIngredientPrintName like ('%Cefoxitin%') or a.IVAdditiveIngredientPrintName like ('%Cefpodoxime%') or 
a.IVAdditiveIngredientPrintName like ('%Cefprozil%') or a.IVAdditiveIngredientPrintName like ('%Ceftaroline%') or a.IVAdditiveIngredientPrintName like ('%Ceftazidime%') or 
a.IVAdditiveIngredientPrintName like ('%Avibactam%') or a.IVAdditiveIngredientPrintName like ('%Ceftibuten%') or a.IVAdditiveIngredientPrintName like ('%Ceftizoxime%') or a.IVAdditiveIngredientPrintName like ('%Tazobactam%') or 
a.IVAdditiveIngredientPrintName like ('%Ceftriaxone%') or a.IVAdditiveIngredientPrintName like ('%Cefuroxime%') or a.IVAdditiveIngredientPrintName like ('%Cephalexin%') or a.IVAdditiveIngredientPrintName like ('%Cephalothin%') or a.IVAdditiveIngredientPrintName like ('%Cephapirin%') or 
a.IVAdditiveIngredientPrintName like ('%Cephradine%') or a.IVAdditiveIngredientPrintName like ('%Chloramphenicol%') or a.IVAdditiveIngredientPrintName like ('%Cidofovir%') or a.IVAdditiveIngredientPrintName like ('%Cinoxacin%') or 
a.IVAdditiveIngredientPrintName like ('%Ciprofloxacin%') or a.IVAdditiveIngredientPrintName like ('%Clindamycin%') or a.IVAdditiveIngredientPrintName like ('%Cloxacillin%') or a.IVAdditiveIngredientPrintName like ('%Colistin%') or 
a.IVAdditiveIngredientPrintName like ('%Colistimethate%') or a.IVAdditiveIngredientPrintName like ('%Dalbavancin%') or a.IVAdditiveIngredientPrintName like ('%Daptomycin%') or a.IVAdditiveIngredientPrintName like ('%Dicloxacillin%') or 
a.IVAdditiveIngredientPrintName like ('%Doripenem%') or a.IVAdditiveIngredientPrintName like ('%Doxycycline%') or a.IVAdditiveIngredientPrintName like ('%Ertapenem%') or 
a.IVAdditiveIngredientPrintName like ('%Fidaxomicin%') or a.IVAdditiveIngredientPrintName like ('%Fluconazole%') or a.IVAdditiveIngredientPrintName like ('%Foscarnet%') or a.IVAdditiveIngredientPrintName like ('%Fosfomycin%') or a.IVAdditiveIngredientPrintName like ('%Ganciclovir%') or 
a.IVAdditiveIngredientPrintName like ('%Gatifloxacin%') or a.IVAdditiveIngredientPrintName like ('%Gentamicin%') or a.IVAdditiveIngredientPrintName like ('%Imipenem%') or a.IVAdditiveIngredientPrintName like ('%Itraconazole%') or 
a.IVAdditiveIngredientPrintName like ('%Kanamycin%') or a.IVAdditiveIngredientPrintName like ('%Levofloxacin%') or a.IVAdditiveIngredientPrintName like ('%Lincomycin%') or a.IVAdditiveIngredientPrintName like ('%Linezolid%') or 
a.IVAdditiveIngredientPrintName like ('%Meropenem%') or a.IVAdditiveIngredientPrintName like ('%Methicillin%') or a.IVAdditiveIngredientPrintName like ('%Metronidazole%') or a.IVAdditiveIngredientPrintName like ('%Mezlocillin%') or 
a.IVAdditiveIngredientPrintName like ('%Micafungin%') or a.IVAdditiveIngredientPrintName like ('%Minocycline%') or a.IVAdditiveIngredientPrintName like ('%Moxifloxacin%') or a.IVAdditiveIngredientPrintName like ('%Nafcillin%') or 
a.IVAdditiveIngredientPrintName like ('%Nitrofurantoin%') or a.IVAdditiveIngredientPrintName like ('%Norfloxacin%') or a.IVAdditiveIngredientPrintName like ('%Ofloxacin%') or a.IVAdditiveIngredientPrintName like ('%Oritavancin%') or 
a.IVAdditiveIngredientPrintName like ('%Oxacillin%') or a.IVAdditiveIngredientPrintName like ('%Penicillin%') or a.IVAdditiveIngredientPrintName like ('%Peramivir%') or a.IVAdditiveIngredientPrintName like ('%Piperacillin%') or 
a.IVAdditiveIngredientPrintName like ('%Tazobactam%') or a.IVAdditiveIngredientPrintName like ('%Pivampicillin%') or a.IVAdditiveIngredientPrintName like ('%Polymyxin B%') or a.IVAdditiveIngredientPrintName like ('%Posaconazole%') or 
a.IVAdditiveIngredientPrintName like ('%Quinupristin%') or a.IVAdditiveIngredientPrintName like ('%Dalfopristin%') or a.IVAdditiveIngredientPrintName like ('%Streptomycin%') or a.IVAdditiveIngredientPrintName like ('%Sulfadiazine%') or 
a.IVAdditiveIngredientPrintName like ('%trimethoprim%') or a.IVAdditiveIngredientPrintName like ('%Sulfamethoxazole%') or a.IVAdditiveIngredientPrintName like ('%Sulfisoxazole%') or 
a.IVAdditiveIngredientPrintName like ('%Tedizolid%') or a.IVAdditiveIngredientPrintName like ('%Telavancin%') or a.IVAdditiveIngredientPrintName like ('%Telithromycin%') or a.IVAdditiveIngredientPrintName like ('%Tetracycline%') or 
a.IVAdditiveIngredientPrintName like ('%Ticarcillin%') or a.IVAdditiveIngredientPrintName like ('%Clavulanate%') or a.IVAdditiveIngredientPrintName like ('%Tigecycline%') or a.IVAdditiveIngredientPrintName like ('%Tobramycin%') or 
a.IVAdditiveIngredientPrintName like ('%Trimethoprim%') or a.IVAdditiveIngredientPrintName like ('%Sulfamethoxazole%') or a.IVAdditiveIngredientPrintName like ('%Vancomycin%') or a.IVAdditiveIngredientPrintName like ('%Voriconazole%');

-----------------------------------------
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
into  dflt.BCMADispensedDrug_ABX
from   #medlog a
left join [Src].[CohortCrosswalk] b on a.medlog_PatientSID=b.patientsid 
order by patienticn, ActionDateTime;

------------------------------------------------------------------------------------------------------------------------
/*pull from IV Additive for study years */

SELECT  a.LocalDrugNameWithDose, a.DrugUnit, a.IVAdditiveIngredientPrintName, a.LocalDrugSID, b.*
into #BCMA_Additive_v1
FROM  #IVAdditiveIngredient  as A
left join [[Src].[BCMA_BCMAAdditive] as B on a.IVAdditiveIngredientSID=b.IVAdditiveIngredientSID
where b.ActionDateTime>= '2014-01-01' and b.ActionDateTime <'2018-01-01'


/*get patientsid for BCMAAdditive from BCMAMedicationLog*/
SELECT A.*, B.PatientSID, c.patienticn
into dflt.BCMA_Additive_ABX
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
into dflt.BCMA_Solution_ABX
FROM   #BCMA_Solution_v1  A
LEFT JOIN  [Src].[BCMA_BCMAMedicationLog]  B ON A.BCMAMedicationLogSID =B.BCMAMedicationLogSID 
left join [Src].[CohortCrosswalk] c on b.patientsid=c.patientsid;/*Add patientICN*/
-------------------------------------------------------------------------------------------------------------------


-- move on to step 2, download dflt tables into SAS tables for data management