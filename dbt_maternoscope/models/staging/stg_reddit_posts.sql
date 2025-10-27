{{
  config(
    materialized='view'
  )
}}

-- Stage model: Deduplicate and basic normalization
-- Deduplicates by POST_ID and content hash, normalizes whitespace

with source_data as (
    select * from {{ source('reddit_raw', 'REDDIT_POSTS') }}
),

-- Create content hash for deduplication
with_hash as (
    select 
        *,
        -- Create deterministic hash of content for deduplication
        HASH(TITLE || '|' || COALESCE(CONTENT, '')) as content_hash,
        TRIM(COALESCE(CONTENT, '')) as content_trimmed
    from source_data
),

-- Deduplicate by POST_ID (keep most recent by SCRAPED_AT)
deduped_by_id as (
    select *
    from (
        select 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY POST_ID 
                ORDER BY SCRAPED_AT DESC
            ) as rn
        from with_hash
    )
    where rn = 1
),

-- Normalize whitespace and case
normalized as (
    select
        POST_ID,
        POST_DATE,
        POST_TIMESTAMP,
        POST_FLAIR,
        TRIM(TITLE) as post_title,
        URL,
        -- Trim and collapse whitespace in content
        REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(COALESCE(CONTENT, '')), '\\s+', ' '),
            '\\n+', '\n'
        ) as post_content_raw,
        SCORE,
        NUM_COMMENTS,
        LOWER(SUBREDDIT) as subreddit,
        SCRAPED_AT,
        content_hash
    from deduped_by_id
)

select * from normalized
