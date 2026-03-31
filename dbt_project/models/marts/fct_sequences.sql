with staging as (
    select * from {{ ref('stg_sequence_index') }}
),

final as (
    select
        fastq_file,
        md5,
        
        run_id,
        {{ dbt_utils.generate_surrogate_key(['study_id', 'study_name']) }} as study_hash,
        center_name,
        submission_id,
        submission_date,
        {{ dbt_utils.generate_surrogate_key(['sample_id', 'sample_name']) }} as sample_hash,
        population_code,
        experiment_id,
        {{ dbt_utils.generate_surrogate_key(['instrument_model', 'instrument_platform']) }} as instrument_hash,
        library_name,
        run_name,
        run_block_name,
        library_layout,
        paired_fastq,

        insert_size,
        withdrawn,
        withdrawn_date,
        comment,
        read_count,
        base_count,
        analysis_group
    from staging
)

select * from final