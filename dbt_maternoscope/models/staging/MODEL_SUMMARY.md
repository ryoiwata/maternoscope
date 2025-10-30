# Reddit Post Text SILVER Model - Summary

## Model: `stg_reddit_posts_pii`

### Purpose
SILVER layer text preparation for LLM processing

### Key Features

✅ **Non-duplicates**: Starts from `stg_reddit_posts` which already deduplicates by POST_ID

✅ **Two Text Fields**:
- `text_raw`: Original unaltered content
- `text_for_llm`: PII-redacted and normalized content

✅ **PII Redaction** (tokens replace sensitive data):
- URLs → `[URL]` (http, https, www)
- Emails → `[EMAIL]`
- Phone numbers → `[PHONE]`
- Usernames → `u/[USER]`
- Subreddits → `r/[SUB]`

✅ **Metrics Computed**:
- `n_chars`: Character count
- `n_words`: Word count
- `n_sentences`: Sentence count
- `n_questions`: Number of question marks
- `n_exclaims`: Number of exclamation marks
- Redaction counts: `n_urls_redacted`, `n_emails_redacted`, `n_phones_redacted`, `n_usernames_redacted`, `n_subreddits_redacted`

✅ **Token Estimation**: `token_estimate` (calculated as ~4 chars per token for cl100k base)

✅ **Language Hint**: `lang_hint` - Fast heuristic based on non-ASCII character proportion (<10% = English)

✅ **Heuristic Flags** (seed dictionary-based):
- `has_medical_terms`: Boolean flag for medical/triage terms
- `trimester_hint`: Detection of early/mid/late pregnancy
- `engagement_level`: High/medium/low based on score and comments

✅ **Routing Flag**:
- `needs_annotation`: TRUE when:
  - Language is English-like
  - Word count between 5-1000
  - Token estimate < 4000
  - Text is not null

### Output Schema
`MATERNOSCOPE.BRONZE.STG_REDDIT_POSTS_PII`

### Columns
- **Metadata**: `post_id`, `post_date`, `post_timestamp`, `post_flair`, `post_title`, `post_url`, `subreddit`, `score`, `num_comments`, `scraped_at`, `content_hash`
- **Text**: `text_raw`, `text_for_llm`
- **Metrics**: `n_chars`, `n_words`, `n_sentences`, `n_questions`, `n_exclaims`, `n_urls_redacted`, `n_emails_redacted`, `n_phones_redacted`, `n_usernames_redacted`, `n_subreddits_redacted`, `token_estimate`
- **Language**: `lang_hint`
- **Flags**: `has_medical_terms`, `trimester_hint`, `engagement_level`, `needs_annotation`

