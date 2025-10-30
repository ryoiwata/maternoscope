{{ config(materialized='view') }}

select
  r.post_id,
  lower(f.value::string) as keyword
from {{ ref('reddit_post_review') }} r,
lateral flatten(input => r.keywords) f


