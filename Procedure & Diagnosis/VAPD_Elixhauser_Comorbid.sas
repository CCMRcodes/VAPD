/* Date Modified: June 2018
   Author: Shirley */

/*the goal of this SAS code is to use all the diagnosis (icd-10 and icd-9) from VAPD and group them into the 31 Elixhauser Comorbidities groups*/

/*DIAGNOSIS, get elixhauser comorbidities*/
libname final  'INSERT FOLDER PATH WHERE VAPD DATASET IS SAVED IN STEP 2';
%let year=20142017;

/*remove duplicate specialty stay*/
PROC SORT DATA=final.vapd_daily&year._v2  nodupkey  OUT=diag_unique_specialty; 
BY  patienticn sta6a specialtytransferdate  specialtydischargedate ;
RUN;

data diag (compress=yes);
set diag_unique_specialty;
keep patienticn sta6a specialtytransferdate  specialtydischargedate icdtype icd9code1-icd9code25  icd10code1-icd10code26;
run;

/*calculate elixhauser comorbid*/
/*take out decimals in icd10 and icd9 diag codes*/
data  diag2 (compress=yes); 
set  diag;
icd10code1=compress(icd10code1,'.'); icd10code2=compress(icd10code2,'.'); icd10code3=compress(icd10code3,'.');icd10code4=compress(icd10code4,'.');icd10code5=compress(icd10code5,'.');
icd10code6=compress(icd10code6,'.');icd10code7=compress(icd10code7,'.');icd10code8=compress(icd10code8,'.');icd10code9=compress(icd10code9,'.');icd10code10=compress(icd10code10,'.');
icd10code11=compress(icd10code11,'.'); icd10code12=compress(icd10code12,'.'); icd10code13=compress(icd10code13,'.');icd10code14=compress(icd10code14,'.');icd10code15=compress(icd10code15,'.');
icd10code16=compress(icd10code16,'.');icd10code17=compress(icd10code17,'.');icd10code18=compress(icd10code18,'.');icd10code19=compress(icd10code19,'.');icd10code20=compress(icd10code20,'.');
icd10code21=compress(icd10code21,'.'); icd10code22=compress(icd10code22,'.'); icd10code23=compress(icd10code23,'.');icd10code24=compress(icd10code24,'.');icd10code25=compress(icd10code25,'.');
icd10code26=compress(icd10code26,'.');
icd9code1=compress(icd9code1,'.'); icd9code2=compress(icd9code2,'.'); icd9code3=compress(icd9code3,'.');icd9code4=compress(icd9code4,'.');icd9code5=compress(icd9code5,'.');
icd9code6=compress(icd9code6,'.');icd9code7=compress(icd9code7,'.');icd9code8=compress(icd9code8,'.');icd9code9=compress(icd9code9,'.');icd9code10=compress(icd9code10,'.');
icd9code11=compress(icd9code11,'.'); icd9code12=compress(icd9code12,'.'); icd9code13=compress(icd9code13,'.');icd9code14=compress(icd9code14,'.');icd9code15=compress(icd9code15,'.');
icd9code16=compress(icd9code16,'.');icd9code17=compress(icd9code17,'.');icd9code18=compress(icd9code18,'.');icd9code19=compress(icd9code19,'.');icd9code20=compress(icd9code20,'.');
icd9code21=compress(icd9code21,'.'); icd9code22=compress(icd9code22,'.'); icd9code23=compress(icd9code23,'.');icd9code24=compress(icd9code24,'.');icd9code25=compress(icd9code25,'.');
run;

/*separate data into icd9 and icd10*/
DATA icd9   icd10;
SET  diag2 ;
if icdtype='ICD10' then output icd10; 
if icdtype='ICD9' then output icd9; 
RUN;

/*transpose icd9 into long form*/
proc transpose data=icd9  out=icd9_long1  (rename=COL1=Diagnosiscode drop=_NAME_);
var icd9code1-icd9code25;
by patienticn sta6a specialtytransferdate  specialtydischargedate;
run;
/*transpose icd10 into long form*/
proc transpose data=icd10  out=icd10_long1 (rename=COL1=Diagnosiscode drop=_NAME_); 
var icd10code1-icd10code26;
by patienticn sta6a specialtytransferdate  specialtydischargedate;
run;

/*combine icd9long and icd10long*/
data all_dia; 
set icd10_long1 icd9_long1;
run;

/*group icd9 and icd10 Elixhauser Comorbidities*/
data all_diag_V2 (compress=yes);
set all_dia;

/* Congestive Heart Failure */
         if  Diagnosiscode IN: ('39891','40201','40211','40291','40401','40403','40411','40413','40491',
                 '40493','4254','4255','4257','4258','4259','428','I099','I110','I130','I132','I255','I420','I425','I426','I427','I428',
                          'I429','I43','I50','P290')  then ELX_GRP_1 = 1;
            LABEL ELX_GRP_1='Congestive Heart Failure';

         /* Cardiac Arrhythmia */
         if  Diagnosiscode IN: ('4260','42613','4267','4269','42610','42612','4270','4271','4272','4273',
                 '4274','4276','4278','4279','7850','99601','99604','V450','V533','I441','I442','I443','I456','I459','I47','I48','I49','R000','R001',
                          'R008','T821','Z450','Z950') then ELX_GRP_2 = 1;
            LABEL ELX_GRP_2='Cardiac Arrhythmia';

         /* Valvular Disease */
         if  Diagnosiscode IN: ('0932','394','395','396','397','424','7463','7464','7465','7466','V422','V433','A520','I05','I06','I07','I08','I091','I098','I34','I35','I36','I37',
                          'I38','I39','Q230','Q231','Q232','Q233','Z952','Z953','Z954')
                  then ELX_GRP_3 = 1;
            LABEL ELX_GRP_3='Valvular Disease';

         /* Pulmonary Circulation Disorders */
         if  Diagnosiscode IN: ('4150','4151','416','4170','4178','4179','I26','I27','I280','I288','I289') then ELX_GRP_4 = 1;
            LABEL ELX_GRP_4='Pulmonary Circulation Disorders';

         /* Peripheral Vascular Disorders */
         if  Diagnosiscode IN: ('0930','4373','440','441','4431','4432','4438','4439','4471','5571','5579','V434','I70','I71','I731','I738','I739','I771','I790','I792','K551','K558',
                          'K559','Z958','Z959')
                  then ELX_GRP_5 = 1;
            LABEL ELX_GRP_5='Peripheral Vascular Disorders';

         /* Hypertension Uncomplicated */
         if  Diagnosiscode IN: ('401') then ELX_GRP_6 = 1;
            LABEL ELX_GRP_6='Hypertension Uncomplicated';

         /* Hypertension Complicated */
         if  Diagnosiscode IN: ('402','403','404','405','I11','I12','I13','I15') then ELX_GRP_7 = 1;
            LABEL ELX_GRP_7='Hypertension Complicated';

         /* Paralysis */
         if  Diagnosiscode IN: ('3341','342','343','3440','3441','3442','3443','3444','3445','3446','3449','G041','G114','G801','G802','G81','G82','G830','G831','G832','G833',
                          'G834','G839')  then ELX_GRP_8 = 1;
           LABEL ELX_GRP_8='Paralysis';

         /* Other Neurological Disorders */
         if  Diagnosiscode IN: ('3319','3320','3321','3334','3335','33392','334','335','3362','340','341',
                  '345','3481','3483','7803','7843','G10','G11','G12','G13','G20','G21','G22','G254','G255','G312','G318',
                          'G319','G32','G35','G36','G37','G40','G41','G931','G934','R470','R56') then ELX_GRP_9 = 1;
           LABEL ELX_GRP_9='Other Neurological Disorders';

         /* Chronic Pulmonary Disease */
         if  Diagnosiscode IN: ('4168','4169','490','491','492','493','494','495','496','500','501','502',
                  '503','504','505','5064','5081','5088','I278','I279','J40','J41','J42','J43','J44','J45','J46','J47','J60','J61',
                          'J62','J63','J64','J65','J66','J67','J684','J701','J703') then ELX_GRP_10 = 1;
           LABEL ELX_GRP_10='Chronic Pulmonary Disease';

         /* Diabetes Uncomplicated */
         if  Diagnosiscode IN: ('2500','2501','2502','2503','E100','E101','E109','E110','E111','E119','E120','E121','E129','E130',
                          'E131','E139','E140','E141','E149') then ELX_GRP_11 = 1;
           LABEL ELX_GRP_11='Diabetes Uncomplicated';

         /* Diabetes Complicated */
         if  Diagnosiscode IN: ('2504','2505','2506','2507','2508','2509','E102','E103','E104','E105','E106','E107','E108','E112','E113','E114','E115',
                          'E116','E117','E118','E122','E123','E124','E125','E126','E127','E128','E132',
                          'E133','E134','E135','E136','E137','E138','E142','E143','E144','E145','E146',
                          'E147','E148') then ELX_GRP_12 = 1;
           LABEL ELX_GRP_12='Diabetes Complicated';

         /* Hypothyroidism */
         if  Diagnosiscode IN: ('2409','243','244','2461','2468','E00','E01','E02','E03','E890') then ELX_GRP_13 = 1;
           LABEL ELX_GRP_13='Hypothyroidism';

         /* Renal Failure */
         if  Diagnosiscode IN: ('40301','40311','40391','40402','40403','40412','40413','40492','40493',
                  '585','586','5880','V420','V451','V56','I120','I131','N18','N19','N250','Z490','Z491','Z492','Z940','Z992') then ELX_GRP_14 = 1;
           LABEL ELX_GRP_14='Renal Failure';

         /* Liver Disease */
         if  Diagnosiscode IN: ('07022','07023','07032','07033','07044','07054','0706','0709','4560','4561',
                  '4562','570','571','5722','5723','5724','5728','5733','5734','5738','5739','V427',
			'B18','I85','I864','I982','K70','K711','K713','K714','K715','K717','K72','K73',
                          'K74','K760','K762','K763','K764','K765','K766','K767','K768','K769','Z944')
                  then ELX_GRP_15 = 1;
           LABEL ELX_GRP_15='Liver Disease';

         /* Peptic Ulcer Disease excluding bleeding */
         if  Diagnosiscode IN: ('5317','5319','5327','5329','5337','5339','5347','5349','K257','K259','K267','K269','K277','K279','K287','K289')
                  then ELX_GRP_16 = 1;
           LABEL ELX_GRP_16='Peptic Ulcer Disease excluding bleeding';

         /* AIDS/HIV */
         if  Diagnosiscode IN: ('042','043','044','B20','B21','B22','B24')  then ELX_GRP_17 = 1;
           LABEL ELX_GRP_17='AIDS/HIV';

         /* Lymphoma */
         if  Diagnosiscode IN: ('200','201','202','2030','2386','C81','C82','C83','C84','C85','C88','C96','C900','C902') then ELX_GRP_18 = 1;
           LABEL ELX_GRP_18='Lymphoma';

         /* Metastatic Cancer */
         if  Diagnosiscode IN: ('196','197','198','199','C77','C78','C79','C80') then ELX_GRP_19 = 1;
           LABEL ELX_GRP_19='Metastatic Cancer';

         /* Solid Tumor without Metastasis */
         if  Diagnosiscode IN: ('140','141','142','143','144','145','146','147','148','149','150','151','152',
                  '153','154','155','156','157','158','159','160','161','162','163','164','165','166','167',
                  '168','169','170','171','172','174','175','176','177','178','179','180','181','182','183',
                  '184','185','186','187','188','189','190','191','192','193','194','195',
				'C00','C01','C02','C03','C04','C05','C06','C07','C08','C09','C10','C11','C12','C13',
                          'C14','C15','C16','C17','C18','C19','C20','C21','C22','C23','C24','C25','C26','C30',
                          'C31','C32','C33','C34','C37','C38','C39','C40','C41','C43','C45','C46','C47','C48',
                          'C49','C50','C51','C52','C53','C54','C55','C56','C57','C58','C60','C61','C62','C63',
                          'C64','C65','C66','C67','C68','C69','C70','C71','C72','C73','C74','C75','C76','C97')
                  then ELX_GRP_20 = 1;
           LABEL ELX_GRP_20='Solid Tumor without Metastasis';

         /* Rheumatoid Arthritis/collagen */
         if  Diagnosiscode IN: ('446','7010','7100','7101','7102','7103','7104','7108','7109','7112','714',
                  '7193','720','725','7285','72889','72930','L940','L941','L943','M05','M06','M08','M120','M123','M30','M310','M311','M312','M313',
                          'M32','M33','M34','M35','M45','M461','M468','M469') then ELX_GRP_21 = 1;
           LABEL ELX_GRP_21='Rheumatoid Arthritis/collagen';

         /* Coagulopathy */
         if  Diagnosiscode IN: ('286','2871','2873','2874','2875','D65','D66','D67','D68','D691','D693','D694','D695','D696')  then ELX_GRP_22 = 1;
           LABEL ELX_GRP_22='Coagulopathy';

         /* Obesity */
         if  Diagnosiscode IN: ('2780','E66') then ELX_GRP_23 = 1;
           LABEL ELX_GRP_23='Obesity';

         /* Weight Loss */
         if  Diagnosiscode IN: ('260','261','262','263','7832','7994','E40','E41','E42','E43','E44','E45','E46','R634','R64') then ELX_GRP_24 = 1;
           LABEL ELX_GRP_24='Weight Loss';

         /* Fluid and Electrolyte Disorders */
         if  Diagnosiscode IN: ('2536','276','E222','E86','E87') then ELX_GRP_25 = 1;
           LABEL ELX_GRP_25='Fluid and Electrolyte Disorders';

         /* Blood Loss Anemia */
         if  Diagnosiscode IN: ('2800','D500') then ELX_GRP_26 = 1;
           LABEL ELX_GRP_26='Blood Loss Anemia';

         /* Deficiency Anemia */
         if  Diagnosiscode IN: ('2801','2808','2809','281','D508','D509','D51','D52','D53') then ELX_GRP_27 = 1;
           LABEL ELX_GRP_27='Deficiency Anemia';

         /* Alcohol Abuse */
         if  Diagnosiscode IN: ('2652','2911','2912','2913','2915','2918','2919','3030','3039','3050',
                  '3575','4255','5353','5710','5711','5712','5713','980','V113','F10','E52','G621','I426',
			'K292','K700','K703','K709','T51','Z502','Z714','Z721') then ELX_GRP_28 = 1;
           LABEL ELX_GRP_28='Alcohol Abuse';

         /* Drug Abuse */
         if  Diagnosiscode IN: ('292','304','3052','3053','3054','3055','3056','3057','3058','3059','V6542',
			'F11','F12','F13','F14','F15','F16','F18','F19','Z715','Z722')
                  then ELX_GRP_29 = 1;	
           LABEL ELX_GRP_29='Drug Abuse';

         /* Psychoses */
         if  Diagnosiscode IN: ('2938','295','29604','29614','29644','29654','297','298',
			'F20','F22','F23','F24','F25','F28','F29','F302','F312','F315')
                  then ELX_GRP_30 = 1;
           LABEL ELX_GRP_30='Psychoses';

         /* Depression */
         if  Diagnosiscode IN: ('2962','2963','2965','3004','309','311','F204','F313','F314','F315','F32','F33','F341','F412','F432') then ELX_GRP_31 = 1;
           LABEL ELX_GRP_31='Depression';
run;


data ELX_GRP_1 ELX_GRP_2 ELX_GRP_3 ELX_GRP_4 ELX_GRP_5 ELX_GRP_6 ELX_GRP_7 ELX_GRP_8 ELX_GRP_9 ELX_GRP_10 
ELX_GRP_11 ELX_GRP_12 ELX_GRP_13 ELX_GRP_14 ELX_GRP_15 ELX_GRP_16 ELX_GRP_17 ELX_GRP_18 ELX_GRP_19 ELX_GRP_20
ELX_GRP_21 ELX_GRP_22 ELX_GRP_23 ELX_GRP_24 ELX_GRP_25 ELX_GRP_26 ELX_GRP_27 ELX_GRP_28 ELX_GRP_29 ELX_GRP_30  ELX_GRP_31;
set ALL_DIAG_V2; 
if ELX_GRP_1 = 1 then output ELX_GRP_1;
if ELX_GRP_2 = 1 then output ELX_GRP_2;
if ELX_GRP_3 = 1 then output ELX_GRP_3;
if ELX_GRP_4 = 1 then output ELX_GRP_4;
if ELX_GRP_5 = 1 then output ELX_GRP_5;
if ELX_GRP_6 = 1 then output ELX_GRP_6;
if ELX_GRP_7 = 1 then output ELX_GRP_7;
if ELX_GRP_8 = 1 then output ELX_GRP_8;
if ELX_GRP_9 = 1 then output ELX_GRP_9;
if ELX_GRP_10 = 1 then output ELX_GRP_10;
if ELX_GRP_11 = 1 then output ELX_GRP_11;
if ELX_GRP_12 = 1 then output ELX_GRP_12;
if ELX_GRP_13 = 1 then output ELX_GRP_13;
if ELX_GRP_14 = 1 then output ELX_GRP_14;
if ELX_GRP_15 = 1 then output ELX_GRP_15;
if ELX_GRP_16 = 1 then output ELX_GRP_16;
if ELX_GRP_17 = 1 then output ELX_GRP_17;
if ELX_GRP_18 = 1 then output ELX_GRP_18;
if ELX_GRP_19 = 1 then output ELX_GRP_19;
if ELX_GRP_20 = 1 then output ELX_GRP_20;
if ELX_GRP_21 = 1 then output ELX_GRP_21;
if ELX_GRP_22 = 1 then output ELX_GRP_22;
if ELX_GRP_23 = 1 then output ELX_GRP_23;
if ELX_GRP_24 = 1 then output ELX_GRP_24;
if ELX_GRP_25 = 1 then output ELX_GRP_25;
if ELX_GRP_26 = 1 then output ELX_GRP_26;
if ELX_GRP_27 = 1 then output ELX_GRP_27;
if ELX_GRP_28 = 1 then output ELX_GRP_28;
if ELX_GRP_29 = 1 then output ELX_GRP_29;
if ELX_GRP_30 = 1 then output ELX_GRP_30;
if ELX_GRP_31 = 1 then output ELX_GRP_31;
run;

/*remove duplicates*/
%macro nums (num);
PROC SORT DATA=ELX_GRP_&num.  nodupkey ;
BY patienticn sta6a specialtytransferdate  specialtydischargedate ELX_GRP_&num.;
RUN;
%mend nums;
%nums(1);
%nums(2);
%nums(3);
%nums(4);
%nums(5);
%nums(6);
%nums(7);
%nums(8);
%nums(9);
%nums(10);
%nums(11);
%nums(12);
%nums(13);
%nums(14);
%nums(15);
%nums(16);
%nums(17);
%nums(18);
%nums(19);
%nums(20);
%nums(21);
%nums(22);
%nums(23);
%nums(24);
%nums(25);
%nums(26);
%nums(27);
%nums(28);
%nums(29);
%nums(30);
%nums(31);

PROC SQL;
	CREATE TABLE  Elixhauser_V1 (compress=yes) AS 
	SELECT A.*, B.ELX_GRP_1, c.ELX_GRP_2, d.ELX_GRP_3, e.ELX_GRP_4, f.ELX_GRP_5, g.ELX_GRP_6
	FROM  diag2  A
	LEFT JOIN  elx_grp_1 B ON A.Patienticn =B.Patienticn and a.Sta6a=b.Sta6a and a.specialtytransferdate=b.specialtytransferdate and a.specialtydischargedate=b.specialtydischargedate
    LEFT JOIN  elx_grp_2 c ON A.Patienticn =c.Patienticn and a.Sta6a=c.Sta6a and a.specialtytransferdate=c.specialtytransferdate and a.specialtydischargedate=c.specialtydischargedate
	LEFT JOIN  elx_grp_3 d ON A.Patienticn =d.Patienticn and a.Sta6a=d.Sta6a and a.specialtytransferdate=d.specialtytransferdate and a.specialtydischargedate=d.specialtydischargedate
	LEFT JOIN  elx_grp_4 e ON A.Patienticn =e.Patienticn and a.Sta6a=e.Sta6a and a.specialtytransferdate=e.specialtytransferdate and a.specialtydischargedate=e.specialtydischargedate
	LEFT JOIN  elx_grp_5 f ON A.Patienticn =f.Patienticn and a.Sta6a=f.Sta6a and a.specialtytransferdate=f.specialtytransferdate and a.specialtydischargedate=f.specialtydischargedate
	LEFT JOIN  elx_grp_6 g ON A.Patienticn =g.Patienticn and a.Sta6a=g.Sta6a and a.specialtytransferdate=g.specialtytransferdate and a.specialtydischargedate=g.specialtydischargedate;
QUIT;

PROC SQL;
	CREATE TABLE  Elixhauser_V2 (compress=yes) AS 
	SELECT A.*, B.ELX_GRP_7, c.ELX_GRP_8, d.ELX_GRP_9, e.ELX_GRP_10, f.ELX_GRP_11, g.ELX_GRP_12, h.ELX_GRP_13, i.ELX_GRP_14, j.ELX_GRP_15
	FROM  Elixhauser_V1 A
	LEFT JOIN  elx_grp_7 B ON A.Patienticn =B.Patienticn and a.Sta6a=b.Sta6a and a.specialtytransferdate=b.specialtytransferdate and a.specialtydischargedate=b.specialtydischargedate
    LEFT JOIN  elx_grp_8 c ON A.Patienticn =c.Patienticn and a.Sta6a=c.Sta6a and a.specialtytransferdate=c.specialtytransferdate and a.specialtydischargedate=c.specialtydischargedate
	LEFT JOIN  elx_grp_9 d ON A.Patienticn =d.Patienticn and a.Sta6a=d.Sta6a and a.specialtytransferdate=d.specialtytransferdate and a.specialtydischargedate=d.specialtydischargedate
	LEFT JOIN  elx_grp_10 e ON A.Patienticn =e.Patienticn and a.Sta6a=e.Sta6a and a.specialtytransferdate=e.specialtytransferdate and a.specialtydischargedate=e.specialtydischargedate
	LEFT JOIN  elx_grp_11 f ON A.Patienticn =f.Patienticn and a.Sta6a=f.Sta6a and a.specialtytransferdate=f.specialtytransferdate and a.specialtydischargedate=f.specialtydischargedate
	LEFT JOIN  elx_grp_12 g ON A.Patienticn =g.Patienticn and a.Sta6a=g.Sta6a and a.specialtytransferdate=g.specialtytransferdate and a.specialtydischargedate=g.specialtydischargedate
	LEFT JOIN  elx_grp_13 h ON A.Patienticn =h.Patienticn and a.Sta6a=h.Sta6a and a.specialtytransferdate=h.specialtytransferdate and a.specialtydischargedate=h.specialtydischargedate
	LEFT JOIN  elx_grp_14 i ON A.Patienticn =i.Patienticn and a.Sta6a=i.Sta6a and a.specialtytransferdate=i.specialtytransferdate and a.specialtydischargedate=i.specialtydischargedate
	LEFT JOIN  elx_grp_15 j ON A.Patienticn =j.Patienticn and a.Sta6a=j.Sta6a and a.specialtytransferdate=j.specialtytransferdate and a.specialtydischargedate=j.specialtydischargedate;
QUIT;

PROC SQL;
	CREATE TABLE  Elixhauser_V3 (compress=yes) AS 
	SELECT A.*, B.ELX_GRP_16, c.ELX_GRP_17, d.ELX_GRP_18, e.ELX_GRP_19, f.ELX_GRP_20, g.ELX_GRP_21, h.ELX_GRP_22, i.ELX_GRP_23, j.ELX_GRP_24, k.ELX_GRP_25
	FROM  Elixhauser_V2 A
	LEFT JOIN  elx_grp_16 B ON A.Patienticn =B.Patienticn and a.Sta6a=b.Sta6a and a.specialtytransferdate=b.specialtytransferdate and a.specialtydischargedate=b.specialtydischargedate
    LEFT JOIN  elx_grp_17 c ON A.Patienticn =c.Patienticn and a.Sta6a=c.Sta6a and a.specialtytransferdate=c.specialtytransferdate and a.specialtydischargedate=c.specialtydischargedate
	LEFT JOIN  elx_grp_18 d ON A.Patienticn =d.Patienticn and a.Sta6a=d.Sta6a and a.specialtytransferdate=d.specialtytransferdate and a.specialtydischargedate=d.specialtydischargedate
	LEFT JOIN  elx_grp_19 e ON A.Patienticn =e.Patienticn and a.Sta6a=e.Sta6a and a.specialtytransferdate=e.specialtytransferdate and a.specialtydischargedate=e.specialtydischargedate
	LEFT JOIN  elx_grp_20 f ON A.Patienticn =f.Patienticn and a.Sta6a=f.Sta6a and a.specialtytransferdate=f.specialtytransferdate and a.specialtydischargedate=f.specialtydischargedate
	LEFT JOIN  elx_grp_21 g ON A.Patienticn =g.Patienticn and a.Sta6a=g.Sta6a and a.specialtytransferdate=g.specialtytransferdate and a.specialtydischargedate=g.specialtydischargedate
	LEFT JOIN  elx_grp_22 h ON A.Patienticn =h.Patienticn and a.Sta6a=h.Sta6a and a.specialtytransferdate=h.specialtytransferdate and a.specialtydischargedate=h.specialtydischargedate
	LEFT JOIN  elx_grp_23 i ON A.Patienticn =i.Patienticn and a.Sta6a=i.Sta6a and a.specialtytransferdate=i.specialtytransferdate and a.specialtydischargedate=i.specialtydischargedate
	LEFT JOIN  elx_grp_24 j ON A.Patienticn =j.Patienticn and a.Sta6a=j.Sta6a and a.specialtytransferdate=j.specialtytransferdate and a.specialtydischargedate=j.specialtydischargedate
	LEFT JOIN  elx_grp_25 k ON A.Patienticn =k.Patienticn and a.Sta6a=k.Sta6a and a.specialtytransferdate=k.specialtytransferdate and a.specialtydischargedate=k.specialtydischargedate;
QUIT;

PROC SQL;
	CREATE TABLE Elixhauser_2014_2017 (compress=yes) AS
	SELECT A.*, B.ELX_GRP_26, c.ELX_GRP_27, d.ELX_GRP_28, e.ELX_GRP_29, f.ELX_GRP_30, g.ELX_GRP_31
	FROM   Elixhauser_V3 A
	LEFT JOIN  elx_grp_26 B ON A.Patienticn =B.Patienticn and a.Sta6a=b.Sta6a and a.specialtytransferdate=b.specialtytransferdate and a.specialtydischargedate=b.specialtydischargedate
    LEFT JOIN  elx_grp_27 c ON A.Patienticn =c.Patienticn and a.Sta6a=c.Sta6a and a.specialtytransferdate=c.specialtytransferdate and a.specialtydischargedate=c.specialtydischargedate
	LEFT JOIN  elx_grp_28 d ON A.Patienticn =d.Patienticn and a.Sta6a=d.Sta6a and a.specialtytransferdate=d.specialtytransferdate and a.specialtydischargedate=d.specialtydischargedate
	LEFT JOIN  elx_grp_29 e ON A.Patienticn =e.Patienticn and a.Sta6a=e.Sta6a and a.specialtytransferdate=e.specialtytransferdate and a.specialtydischargedate=e.specialtydischargedate
	LEFT JOIN  elx_grp_30 f ON A.Patienticn =f.Patienticn and a.Sta6a=f.Sta6a and a.specialtytransferdate=f.specialtytransferdate and a.specialtydischargedate=f.specialtydischargedate
	LEFT JOIN  elx_grp_31 g ON A.Patienticn =g.Patienticn and a.Sta6a=g.Sta6a and a.specialtytransferdate=g.specialtytransferdate and a.specialtydischargedate=g.specialtydischargedate;
QUIT;

/*GET SUM OF ELIXHAUSER COMORBID COUNT*/
PROC SQL;
CREATE TABLE  final.Elixhauser_2014_2017 (compress=yes) AS 
SELECT *, sum(ELX_GRP_1, ELX_GRP_2, ELX_GRP_3, ELX_GRP_4, ELX_GRP_5, ELX_GRP_6, ELX_GRP_7 ,
              ELX_GRP_8, ELX_GRP_9, ELX_GRP_10, ELX_GRP_11, ELX_GRP_12, ELX_GRP_13, ELX_GRP_14 ,
              ELX_GRP_15, ELX_GRP_16, ELX_GRP_17, ELX_GRP_18, ELX_GRP_19, ELX_GRP_20, ELX_GRP_21,
              ELX_GRP_22, ELX_GRP_23, ELX_GRP_24, ELX_GRP_25, ELX_GRP_26, ELX_GRP_27, ELX_GRP_28,
              ELX_GRP_29, ELX_GRP_30, ELX_GRP_31) as sum_Elixhauser_count
FROM Elixhauser_2014_2017;
QUIT;

/*remove duplicates*/
PROC SORT DATA=final.Elixhauser_2014_2017  nodupkey; 
BY patienticn sta6a icdtype specialtytransferdate sum_Elixhauser_count;
RUN;

/******** end of Elixhauser Comorbidities ********/
