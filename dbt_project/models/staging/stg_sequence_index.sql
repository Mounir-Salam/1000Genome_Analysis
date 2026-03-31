select
    -- Identifiers
    fastq_file,
    md5,
    
    -- Descriptive fields
    run_id,
    study_id,
    study_name,
    center_name,
    submission_id,
    cast(submission_date as date) as submission_date,
    sample_id,
    sample_name,
    population as population_code,
    experiment_id,
    instrument_platform,
    instrument_model,
    library_name,
    run_name,
    run_block_name,
    library_layout,
    paired_fastq,

    -- Additional metrics
    insert_size,
    withdrawn,
    cast(withdrawn_date as date) as withdrawn_date,
    comment,
    read_count,
    base_count,
    analysis_group
from {{ source('external_source', 'sequence_raw') }}