select
    -- Unique identifier
    population_code,

    -- Descriptive fields
    super_population,
    population_description,

    -- Additional metrics
    dna_from_blood,
    offspring_available_from_trios,
    pilot_samples,
    phase1_samples,
    final_phase_samples
from {{ source('external_source', 'population_raw') }}