select *
from {{ ref('fct_sequences') }}
where library_layout = 'PAIRED'