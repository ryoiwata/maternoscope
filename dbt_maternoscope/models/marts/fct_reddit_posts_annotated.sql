{{ config(materialized='view') }}

-- GOLD layer: Combined view of PII-redacted posts with LLM annotations
-- This view joins the bronze staging layer (PII-redacted post content and metrics)
-- with the ML layer (LLM-generated annotations, categorizations, and care responses)

with posts_pii as (
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
    text_for_llm,
    n_chars,
    n_words,
    n_sentences,
    n_questions,
    n_exclaims,
    n_urls_redacted,
    n_emails_redacted,
    n_phones_redacted,
    n_usernames_redacted,
    n_subreddits_redacted,
    token_estimate,
    needs_annotation
  from {{ ref('stg_reddit_posts_pii') }}
),

annotations as (
  select
    post_id,
    primary_group,
    primary_topic,
    secondary_topics,
    trimester,
    sentiment,
    urgency_0_3,
    keywords,
    safety_flags,
    post_summary,
    care_response,
    model_name,
    model_version,
    prompt_hash,
    input_tokens,
    output_tokens,
    annotated_at
  from {{ source('reddit_annotated', 'REDDIT_POSTS_ANNOTATED') }}
)

select
  -- Post metadata (from bronze)
  p.post_id,
  p.post_date,
  p.post_timestamp,
  p.post_flair,
  p.post_title,
  p.post_url,
  p.subreddit,
  p.score,
  p.num_comments,
  p.scraped_at,
  p.content_hash,
  
  -- Post content (from bronze)
  p.text_raw,
  p.text_for_llm,
  
  -- Content metrics (from bronze)
  p.n_chars,
  p.n_words,
  p.n_sentences,
  p.n_questions,
  p.n_exclaims,
  p.n_urls_redacted,
  p.n_emails_redacted,
  p.n_phones_redacted,
  p.n_usernames_redacted,
  p.n_subreddits_redacted,
  p.token_estimate,
  p.needs_annotation,
  
  -- LLM annotations (from ML layer)
  a.primary_group,
  a.primary_topic,
  a.secondary_topics,
  a.trimester,
  a.sentiment,
  a.urgency_0_3,
  a.keywords,
  a.safety_flags,
  a.post_summary,
  a.care_response,
  a.model_name,
  a.model_version,
  a.prompt_hash,
  a.input_tokens,
  a.output_tokens,
  a.annotated_at

from posts_pii as p
left join annotations as a
  on p.post_id = a.post_id

