with staging as (
    select * from {{ ref('stg_sequence_index') }}
),

unique_groups as (
    -- Get every unique combination of your sub-info
    select distinct
        study_id,
        study_name
    from staging
),

final as (
    select
        -- Generate the reference_id using a hash (dbt_utils is great for this)
        {{ dbt_utils.generate_surrogate_key(['study_id', 'study_name']) }} as study_hash,
        study_id,
        study_name
    from unique_groups
)

select * from final