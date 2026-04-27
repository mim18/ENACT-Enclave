drop table ENACT_MAP_LOCAL_TO_STANDARD;
create table ENACT_MAP_LOCAL_TO_STANDARD 
as select * from (
with ENACT_VOCAB_MAP AS
(
select 'DEM|HISP:%' as local_prefix, 'Ethnicity' as enact_vocab
union
select 'DEM|RACE:%' as local_prefix, 'Race' as enact_vocab
union
select 'DEM|SEX:%' as local_prefix, 'Gender' as enact_vocab
union
select 'RXNORM:%' as local_prefix, 'RXNORM' as enact_vocab
union
select 'NDC:%' as local_prefix, 'NDC' as enact_vocab
union
select 'NUI:%' as local_prefix, 'NDFRT' as enact_vocab
union
select 'NUI:%' as local_prefix, 'VANDF' as enact_vocab
union
select 'ICD10CM:%' as local_prefix, 'ICD10CM' as enact_vocab
union
select 'ICD9CM:%' as local_prefix, 'ICD9CM' as enact_vocab
union
select 'ICD10PCS:%' as local_prefix, 'ICD10PCS' as enact_vocab
union
select 'ICD9PROC:%' as local_prefix, 'ICD9PROC' as enact_vocab
union
select 'LOINC:%' as local_prefix, 'LOINC' as enact_vocab
union
select 'CPT4:%' as local_prefix, 'CPT4' as enact_vocab
union
select 'HCPCS:%' as local_prefix, 'HCPCS' as enact_vocab

 ),
enact_concept_dimension as
(
    select * from @cdmDatabaseSchema.concept_dimension
),
med_standard_codes as
(
select concept_path, concept_cd, name_char from enact_concept_dimension where concept_path like '\ACT\Medications\%'  and
(concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'RXNORM')
     or concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'NDC')
     or concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'VANDF')
     or concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'HCPCS') -- leave commented out if you want hcpcs rolled up to rxnorms
     or concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'NDFRT'))

),
med_nonstandard_codes as --local codes
(
select * from enact_concept_dimension where concept_path like '\ACT\Medications\%'
and (concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'RXNORM')
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'NDC')
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'VANDF')
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'HCPCS') -- leave commented out if you want hcpcs rolled up to rxnorms
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'NDFRT'))

),
med_nonstandard_parents as
(
select
    concept_cd,
    name_char,
    concept_path,
    substring(concept_path,
			      len(concept_path) - charindex('\',reverse(concept_path),2)+2,
			      charindex('\',reverse(concept_path),2)-2
		) as path_element,
    substring(concept_path,1,len(concept_path)-charindex('\',reverse(concept_path),2)+1) as parent
from med_nonstandard_codes

),
med_nonstandard_codes_mapped as
(
select
    s.concept_cd as act_standard_code,
    p.concept_cd as local_concept_cd,
    p.name_char,
    p.parent as parent_concept_path,
    s.concept_path as concept_path,
    p.path_element
from med_nonstandard_parents p
inner join med_standard_codes s on s.concept_path = p.parent
),
-- Diagnosis Code Mapping
dx_standard_codes as
(
select concept_path, concept_cd, name_char from enact_concept_dimension
where (concept_path like '\ACT\Diagnosis\%' or concept_path like '\Diagnoses\%') and
(concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'ICD10CM')
     or concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'ICD9CM'))

),
dx_nonstandard_codes as --local codes
(
select * from enact_concept_dimension
where (concept_path like '\ACT\Diagnosis\%' or concept_path like '\Diagnoses\%') and
(concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'ICD10CM')
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'ICD9CM'))

),
dx_nonstandard_parents as
(
select
    concept_cd,
    name_char,
    substring(concept_path,1,len(concept_path)-charindex('\',reverse(concept_path),2)+1) as parent,
    substring(concept_path,
			      len(concept_path) - charindex('\',reverse(concept_path),2)+2,
			      charindex('\',reverse(concept_path),2)-2
		) as path_element,
    concept_path
from dx_nonstandard_codes

),
dx_nonstandard_codes_mapped as
(
select
    s.concept_cd as act_standard_code,
    p.concept_cd as local_concept_cd,
    p.name_char,
    p.parent as parent_concept_path,
    s.concept_path as concept_path,
    p.path_element
from dx_nonstandard_parents p
inner join dx_standard_codes s on s.concept_path = p.parent
),

-- Lab Code Mapping
lab_standard_codes as
(
select concept_path, concept_cd, name_char from enact_concept_dimension
where (concept_path like '\ACT\Labs\%' or concept_path like '\ACT\Lab\%') and
(concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'LOINC'))

),
lab_nonstandard_codes as --local codes
(
select * from enact_concept_dimension
where (concept_path like '\ACT\Labs\%' or concept_path like '\ACT\Lab\%') and
(concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'LOINC'))

),
lab_nonstandard_parents as
(
select
    concept_cd,
    name_char,
    substring(concept_path,1,len(concept_path)-charindex('\',reverse(concept_path),2)+1) as parent,
    substring(concept_path,
			      len(concept_path) - charindex('\',reverse(concept_path),2)+2,
			      charindex('\',reverse(concept_path),2)-2
		) as path_element,
    concept_path
from lab_nonstandard_codes


),
lab_nonstandard_codes_mapped as
(
select
    s.concept_cd as act_standard_code,
    p.concept_cd as local_concept_cd,
    p.name_char,
    p.parent as parent_concept_path,
    s.concept_path as concept_path,
    p.path_element
from lab_nonstandard_parents p
inner join lab_standard_codes s on s.concept_path = p.parent
),

-- Procedures Code Mapping
px_standard_codes as
(
select concept_path, concept_cd, name_char from enact_concept_dimension
where (concept_path like '\ACT\Procedures\%' or concept_path like '\Diagnoses\%') and
    (concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'ICD10PCS')
     or concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'ICD9PROC')
     or concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'CPT4' )
     or concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'HCPCS'))

),
px_nonstandard_codes as --local codes
(
select * from enact_concept_dimension
where (concept_path like '\ACT\Procedures\%' or concept_path like '\Diagnoses\%') and
    (concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'ICD10PCS')
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'ICD9PROC')
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'CPT4')
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'ICD10CM')
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'ICD9CM')
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'HCPCS'))

),
px_nonstandard_parents as
(
select
    concept_cd,
    name_char,
    substring(concept_path,1,len(concept_path)-charindex('\',reverse(concept_path),2)+1) as parent,
    substring(concept_path,
			      len(concept_path) - charindex('\',reverse(concept_path),2)+2,
			      charindex('\',reverse(concept_path),2)-2
		) as path_element,
    concept_path
from px_nonstandard_codes


),
px_nonstandard_codes_mapped as
(
select
    s.concept_cd as act_standard_code,
    p.concept_cd as local_concept_cd,
    p.name_char,
    p.parent as parent_concept_path,
    s.concept_path as concept_path,
    p.path_element
from px_nonstandard_parents p
inner join px_standard_codes s on s.concept_path = p.parent
),

-- Demographics Code Mapping
dem_standard_codes as
(
select concept_path, concept_cd, name_char from enact_concept_dimension
where concept_path like '\ACT\Demographics\%' and
(concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'Race')
     or concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'Gender')
     or concept_cd like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'Ethnicity'))

),
dem_nonstandard_codes as --local codes
(
select * from enact_concept_dimension
where concept_path like '\ACT\Demographics\%' and
    (concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'Race')
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'Gender')
     and concept_cd not like (select top 1 local_prefix from ENACT_VOCAB_MAP where enact_vocab = 'Ethnicity'))

),
dem_nonstandard_parents as
(
select
    concept_cd,
    name_char,
    substring(concept_path,1,len(concept_path)-charindex('\',reverse(concept_path),2)+1) as parent,
    substring(concept_path,
			      len(concept_path) - charindex('\',reverse(concept_path),2)+2,
			      charindex('\',reverse(concept_path),2)-2
		) as path_element,
    concept_path
from dem_nonstandard_codes

),
dem_nonstandard_codes_mapped as
(
select
    s.concept_cd as act_standard_code,
    p.concept_cd as local_concept_cd,
    p.name_char,
    p.parent as parent_concept_path,
    s.concept_path as concept_path,
    p.path_element
from dem_nonstandard_parents p
inner join dem_standard_codes s on s.concept_path = p.parent
)
select * from med_nonstandard_codes_mapped
union
select * from lab_nonstandard_codes_mapped
union
select * from dx_nonstandard_codes_mapped
union
select * from px_nonstandard_codes_mapped
union
select * from dem_nonstandard_codes_mapped
)
