# VAPD---
title: "Veterans Affairs Patient Database (VAPD) ReadMe"
author: "Shirley Wang"
date: "June 28, 2018"
output: html_document
---

## Veterans Affairs Patient Database (VAPD 2014-2017): Building nationwide granular data for clinical discovery  {.tabset .tabset-fade .tabset-pills}

To study patient physiology throughout a period of acute hospitalization, we sought to create accessible, standardized nationwide data at the level of the individual patient-facility-day. This methodology paper summarizes the development, organization, and characteristics of the Veterans Affairs Patient Database 2014-2017 (VAPD 2014-2017). 

The SAS and SQL codes posted on this Github page are to be used for the creation of the VAPD.


### General Code Structure  


**1. VAPD**  

**2. Microbiology Labs & Culture**

**3. Medications**

**4. Labs**

**5. Vitals**

**6. Procedure & Diagnosis**


### VAPD  



*Follow the codes in each step to create the VAPD.* 

**1. Step 1A: pull inpatient data**

**2. Step 1B: Collasping hospitalizations**

**3. Step 2: Create patient-facility-day dataset**


### Microbiology Labs & Culture  




*Use the excel spreadsheets and CSV files in Step 1 as noted in the SQL code.* 

**1. Microbiology labs**

**2. Cultures**



### Medications  



*These codes are to be used to pull and process the below Medication variables.*

**1. Antibiotics**

**2. Vasopressors**

**3. Sedatives, Paralytic and Analgesic**


### Labs  



**ABG lab: Mechanical Ventilation Indicator**

**pH, pO2, pCO2**

**Albumin**

**Bicarbonate**

**Bilirubin**

**Creatinine**

**Glucose**

**Hematocrit**

**Hemoglobin**

**Lactate**

**Platelet**

**Potassium**

**Sodium**

**Urea**

**White Blood Cell**


### Vitals  



*The SAS code will pull and create the following vital signs for each patient-day:*

**1. Pulse**

**2. Temperature**

**3. Respiration rate**

**4. Diastolic**

**5. Systolic**

**6. Mean Arterial Pressure (MAp)**


### Procedure & Diagnosis  



*The SAS codes are to create procedure and diagnosis indicators for patient-day*

**1. Mechanical Ventilation**

**2. Dialysis**

**3. Elixhauser Comorbidities**

**4. CDC EHR Definition of Sepsis**

**5. Angus Definition of Sepsis**


