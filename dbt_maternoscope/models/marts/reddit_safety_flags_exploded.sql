{{ config(materialized='view') }}

select
  r.post_id,
  lower(f.value::string) as safety_flag
from {{ ref('reddit_post_review') }} r,
lateral flatten(input => r.safety_flags) f


