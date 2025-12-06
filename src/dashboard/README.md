# Clinician Review Dashboard

A Streamlit dashboard for clinicians to review and rate LLM-generated annotations for Reddit posts about maternal health.

## Features

- **View Reddit Posts**: Browse posts with LLM annotations from the Snowflake database
- **Filter Posts**: Filter by subreddit, primary topic, and urgency level
- **Review LLM Outputs**: View original posts, LLM summaries, and care responses
- **Rate Quality**: Provide ratings on multiple dimensions:
  - Overall Quality (required)
  - Accuracy (medical correctness)
  - Empathy (tone and emotional support)
  - Safety (appropriate escalation and warnings)
  - Relevance (addresses post concerns)
- **Submit Feedback**: Add comments and submit ratings to Snowflake

## Setup

### Prerequisites

1. Python 3.8+ with required packages installed
2. Snowflake credentials configured in `.env` file
3. Access to `MATERNOSCOPE.ANALYTICS_GOLD.REDDIT_POST_REVIEW` table

### Environment Variables

Ensure your `.env` file contains:

```bash
SNOWFLAKE_USERNAME=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MATERNOSCOPE
SNOWFLAKE_SCHEMA=ANALYTICS_GOLD
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install streamlit separately
pip install streamlit
```

## Usage

### Running the Dashboard

```bash
streamlit run src/dashboard/clinician_review_dashboard.py
```

The dashboard will open in your default web browser at `http://localhost:8501`.

### First-Time Setup

1. **Initialize Ratings Table**: Click "Initialize Ratings Table" in the sidebar to create the `MATERNOSCOPE.ANALYTICS_REF.CLINICIAN_RATINGS` table in Snowflake.

2. **Load Posts**: Use the sidebar filters to select posts:
   - Number of posts to load (10-1000)
   - Filter by subreddit (optional)
   - Filter by primary topic (optional)
   - Filter by minimum urgency level (optional)
   - Click "🔄 Load Posts" to fetch data

3. **Navigate Posts**: Use the navigation controls in the sidebar to move between posts.

4. **Review and Rate**:
   - View the original post content
   - Review LLM summary and annotations
   - Read the LLM-generated care response
   - Provide ratings and feedback in the "Rate & Review" tab
   - Click "✅ Submit Rating" to save to Snowflake

## Database Schema

### Ratings Table

The dashboard creates and writes to:

**Table**: `MATERNOSCOPE.ANALYTICS_REF.CLINICIAN_RATINGS`

**Schema**:
- `RATING_ID` (VARCHAR): Unique identifier for each rating
- `POST_ID` (VARCHAR): Foreign key to the reviewed post
- `CLINICIAN_NAME` (VARCHAR): Name of the reviewing clinician
- `CLINICIAN_EMAIL` (VARCHAR): Optional email of the clinician
- `RATING_OVERALL` (NUMBER): Overall quality rating (1-5, required)
- `RATING_ACCURACY` (NUMBER): Accuracy rating (1-5, optional)
- `RATING_EMPATHY` (NUMBER): Empathy rating (1-5, optional)
- `RATING_SAFETY` (NUMBER): Safety rating (1-5, optional)
- `RATING_RELEVANCE` (NUMBER): Relevance rating (1-5, optional)
- `COMMENTS` (VARCHAR): Free-text feedback (optional)
- `RATED_AT` (TIMESTAMP_TZ): Timestamp when rating was submitted
- `CREATED_AT` (TIMESTAMP_TZ): Record creation timestamp

## Rating Scale

- **1 = Poor**: Significant issues, not acceptable
- **2 = Below Average**: Some issues, needs improvement
- **3 = Average**: Acceptable but could be better
- **4 = Good**: High quality, minor improvements possible
- **5 = Excellent**: Outstanding quality, no issues

## Data Source

The dashboard reads from:
- **Source Table**: `MATERNOSCOPE.ANALYTICS_GOLD.REDDIT_POST_REVIEW`
- **View**: `MATERNOSCOPE.ANALYTICS_GOLD.FCT_REDDIT_POSTS_ANNOTATED` (via DBT)

This view combines:
- Post metadata and content (from Bronze layer)
- LLM annotations (from ML layer)
- PII-redacted text and metrics

## Troubleshooting

### Connection Issues

- Verify Snowflake credentials in `.env` file
- Check network connectivity
- Ensure Snowflake warehouse is running

### No Posts Loaded

- Verify that posts exist in `REDDIT_POST_REVIEW` table
- Check that posts have `CARE_RESPONSE` populated
- Adjust filters to be less restrictive

### Rating Submission Fails

- Ensure ratings table has been initialized
- Check that `CLINICIAN_NAME` is provided
- Verify Snowflake permissions for `ANALYTICS_REF` schema

## Future Enhancements

- Export ratings to CSV/Excel
- View rating statistics and trends
- Compare ratings across multiple clinicians
- Filter by previously rated posts
- Batch rating mode for multiple posts


















