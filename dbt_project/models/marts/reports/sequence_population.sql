with sequence as (
    select
        fastq_file,
        population_code
    from {{ ref('fct_sequences') }}
),

population as (
    select
        population_code,
        super_population,
        population_description,
        dna_from_blood,
        offspring_available_from_trios
    from {{ ref('dim_population_details') }}
)

select
    s.fastq_file,
    s.population_code,
    p.super_population,
    p.population_description,
    p.dna_from_blood,
    p.offspring_available_from_trios
from sequence s
left join population p
    on s.population_code = p.population_code