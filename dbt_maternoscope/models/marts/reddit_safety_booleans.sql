{{ config(materialized='view') }}

with base as (
  select * from {{ ref('reddit_post_review') }}
)
select
  post_id,
  array_contains(safety_flags, 'urgent_bleeding')      as is_urgent_bleeding,
  array_contains(safety_flags, 'urgent_pain')          as is_urgent_pain,
  array_contains(safety_flags, 'urgent_fetal_concern') as is_urgent_fetal_concern,
  array_contains(safety_flags, 'mental_health_crisis') as is_mh_crisis
from base


