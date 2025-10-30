# DBT Models Documentation

This directory contains DBT models that transform raw Reddit data into analysis-ready datasets.

## Model Architecture

### 1. Sources (`sources.yml`)
- **Purpose**: Defines the raw data tables from Snowflake
- **Schema**: `MATERNOSCOPE.INGEST`
- **Main table**: `REDDIT_POSTS` - Raw scraped Reddit posts

### 2. Staging Models (`staging/`)

#### `stg_reddit_posts.sql`
- **Purpose**: Initial normalization and deduplication
- **Key operations**:
  - Deduplicate by `POST_ID` (keep most recent by `SCRAPED_AT`)
  - Create `content_hash` and trim/collapse whitespace

#### `stg_reddit_posts_pii.sql`
- **Purpose**: PII redaction + text prep for LLM
- **Key operations**:
  - Redact PII: URLs → `[URL]`, Emails → `[EMAIL]`, Phones → `[PHONE]`, Usernames → `u/[USER]`, Subreddits → `r/[SUB]`
  - Emit `text_raw` and `text_for_llm`
  - Metrics: `n_chars`, `n_words`, `n_sentences`, `n_questions`, `n_exclaims`, redaction counts, `token_estimate`
  - Routing flag: `needs_annotation`

### 3. Gold/Marts Models (`marts/`)

#### `fct_reddit_posts_annotated.sql`
- **Purpose**: Join PII-prepped posts with LLM annotations
- **Joins**: `stg_reddit_posts_pii` (bronze) LEFT JOIN `MATERNOSCOPE.ANALYTICS_ML.REDDIT_POSTS_ANNOTATED` on `post_id`
- **Outputs**: Post metadata, `text_raw`, `text_for_llm`, metrics, plus LLM labels (`primary_group/topic`, `trimester`, `sentiment`, `urgency_0_3`, `keywords`, `safety_flags`, `post_summary`, `care_response`)

#### `reddit_post_review.sql`
- Thin alias view over `fct_reddit_posts_annotated` to stabilize downstream refs and Looker naming.

#### `reddit_keywords_exploded.sql`
- Explodes `keywords` array into rows via `lateral flatten`.

#### `reddit_safety_flags_exploded.sql`
- Explodes `safety_flags` array into rows via `lateral flatten`.

#### `reddit_safety_booleans.sql`
- Convenience booleans for quick KPIs from `safety_flags` using `array_contains`.

## Model Dependencies

```
sources.yml (REDDIT_POSTS)
    ↓
stg_reddit_posts
    ↓
stg_reddit_posts_pii
    ↓                          +  MATERNOSCOPE.ANALYTICS_ML.REDDIT_POSTS_ANNOTATED (external write)
fct_reddit_posts_annotated     ←———————————————————————————————————————————————————————————————————————
    ↓
reddit_post_review
    ├─ reddit_keywords_exploded
    ├─ reddit_safety_flags_exploded
    └─ reddit_safety_booleans
```

## Running the Models

```bash
# Pre-req: Use Python 3.12 for dbt (dbt 1.10.x). Avoid Python 3.14.

cd /home/riwata/Documents/projects/data_studies/maternoscope/dbt_maternoscope

# 1) Build staging/bronze views
dbt run --select stg_reddit_posts stg_reddit_posts_pii

# 2) (External) Run LLM annotator script to populate ANALYTICS_ML.REDDIT_POSTS_ANNOTATED
#    See src/llm/annotate_reddit_posts.py

# 3) Build gold views and helpers for Looker
dbt run --select fct_reddit_posts_annotated reddit_post_review \
                   reddit_keywords_exploded reddit_safety_flags_exploded reddit_safety_booleans

# Tip: build everything in order
dbt run --select stg_reddit_posts+          # builds staging and downstream
```

## Key Features

### 1. Deduplication
- **POST_ID deduplication**: Keeps most recent by `SCRAPED_AT`
- **Content hash deduplication**: Identifies near-duplicate content

### 2. PII Redaction
- URLs, emails, phones automatically redacted
- Usernames and subreddit mentions masked
- Original content preserved for reference

### 3. Content Analysis
- Length metrics (chars, words, token estimates)
- Punctuation metrics (questions, exclamations)
- PII redaction counts

### 4. Guardrails
- Minimum length: 10 characters
- Maximum tokens: ~8000 (GPT-4 limit)
- Language filter: English only
- Medical content detection for separate handling

### 5. LLM Routing
- `needs_annotation` flag: TRUE for posts within length/token budgets and suitable for annotation

## Output Schemas (Medallion Architecture)

- **INGEST**: Raw scraped data — `MATERNOSCOPE.INGEST.REDDIT_POSTS`
- **BRONZE**: Staging/standardization (views) — `STG_REDDIT_POSTS`, `STG_REDDIT_POSTS_PII`
- **ML**: LLM annotations written by Python — `MATERNOSCOPE.ANALYTICS_ML.REDDIT_POSTS_ANNOTATED`
- **GOLD**: BI-ready views — `FCT_REDDIT_POSTS_ANNOTATED`, `REDDIT_POST_REVIEW`, exploded/booleans helpers

## Usage Examples

```sql
-- Inspect combined post + LLM annotations
select post_id, post_title, post_summary, care_response
from MATERNOSCOPE.ANALYTICS_GOLD.FCT_REDDIT_POSTS_ANNOTATED
order by annotated_at desc
limit 50;

-- Top keywords (requires exploded view)
select keyword, count(*) as cnt
from MATERNOSCOPE.ANALYTICS_GOLD.REDDIT_KEYWORDS_EXPLODED
group by 1 order by 2 desc limit 25;

-- Safety KPI booleans
select sum(is_urgent_bleeding) as urgent_bleeding_posts
from MATERNOSCOPE.ANALYTICS_GOLD.REDDIT_SAFETY_BOOLEANS;
```
