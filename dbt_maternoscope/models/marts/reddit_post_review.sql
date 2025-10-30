{{ config(materialized='view') }}

-- Thin gold view alias to stabilize downstream refs and Looker naming
select *
from {{ ref('fct_reddit_posts_annotated') }}


