{{ config(materialized='view') }}

-- SILVER layer: Posts that need LLM annotation
-- This view identifies posts that need annotation.
-- Note: This model only shows posts needing annotation from the staging layer.
-- To see which posts haven't been annotated yet, use the marts model
-- that joins with the ML annotation table (after LLM annotation runs).

-- This staging model shows all posts that need annotation based on
-- content metrics (word count, token estimate, etc.)
-- It does NOT check against the annotation table since that schema
-- may not exist yet when staging models run.

select
  post_id,
  post_date,
  post_timestamp,
  post_flair,
  post_title,
  post_url,
  subreddit,
  score,
  num_comments,
  scraped_at,
  content_hash,
  text_raw,
  text_for_llm,  -- Cleaned post text ready for LLM
  n_chars,
  n_words,
  n_sentences,
  token_estimate,
  needs_annotation
from {{ ref('stg_reddit_posts_pii') }}
where needs_annotation = true

