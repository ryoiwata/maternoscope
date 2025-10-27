{{ config(materialized='view') }}

-- SILVER layer: Text prep for LLM
-- PII redaction, normalization, metrics, and LLM routing flags

with base as (
  select *
  from {{ ref('stg_reddit_posts') }}
  where post_content_raw is not null
),

pii_redacted as (
  select
    post_id,
    post_date,
    post_timestamp,
    post_flair,
    post_title,
    url as post_url,
    subreddit,
    score,
    num_comments,
    scraped_at,
    content_hash,                      

    -- Keep raw text unaltered
    post_content_raw as text_raw,

    -- PII redaction chain (use Snowflake regex parameters for case-insensitive: 'i')
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              -- URLs (http/https/www)
              regexp_replace(post_content_raw, 'https?://\\S+|\\bwww\\.[^\\s]+', '[URL]', 1, 0, 'i'),
              -- emails
              '[a-z0-9][a-z0-9._%+-]*@[a-z0-9][a-z0-9.-]*\\.[a-z]{2,}', '[EMAIL]', 1, 0, 'i'
            ),
            -- phones
            '(\\+?[0-9][0-9\\-\\s\\(\\)]{7,}[0-9])', '[PHONE]', 1, 0
          ),
          -- usernames u/...
          '\\bu/[a-z0-9_\\-]+', 'u/[USER]', 1, 0, 'i'
        ),
        -- subreddits r/...
        '\\br/[a-z0-9_]+', 'r/[SUB]', 1, 0, 'i'
      ),
      -- optional: collapse whitespace
      '\\s+', ' ', 1, 0
    ) as text_for_llm

  from base
),

with_metrics as (
  select
    *,
    length(text_for_llm) as n_chars,
    array_size(split(text_for_llm, ' ')) as n_words,
    greatest(regexp_count(text_for_llm, '[\\.!?]+'), 1) as n_sentences,

    -- punctuation metrics
    regexp_count(text_for_llm, '\\?') as n_questions,
    regexp_count(text_for_llm, '!') as n_exclaims,

    -- redaction counts (match the tokens)
    regexp_count(text_for_llm, '\\[URL\\]')   as n_urls_redacted,
    regexp_count(text_for_llm, '\\[EMAIL\\]') as n_emails_redacted,
    regexp_count(text_for_llm, '\\[PHONE\\]') as n_phones_redacted,
    regexp_count(text_for_llm, 'u/\\[USER\\]') as n_usernames_redacted,
    regexp_count(text_for_llm, 'r/\\[SUB\\]')  as n_subreddits_redacted,

    -- token estimate (~4 chars/token)
    ceil(length(text_for_llm) / 4.0) as token_estimate

  from pii_redacted
),

final as (
  select
    *,
    case
      when n_words between 5 and 1000
       and token_estimate < 4000
       and text_for_llm is not null
      then true else false
    end as needs_annotation
  from with_metrics
)

select * from final
