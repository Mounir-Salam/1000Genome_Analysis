with source as (
    select * from {{ source('external_source', 'sequence_raw') }}
),

renamed as (
    select
        -- Basic cleanup and casting
        fastq_file,
        md5,
        sample_id,
        run_id,
        population,
        instrument_platform,
        -- Spark might have loaded this as a string, let's ensure it's a DATE
        cast(submission_date as date) as submission_date
    from source
)

select * from renamed