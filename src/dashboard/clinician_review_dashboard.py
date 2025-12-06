#!/usr/bin/env python3
"""
Clinician Review Dashboard for LLM Annotations

This Streamlit dashboard allows clinicians to:
1. View Reddit posts with LLM annotations
2. Rate the quality of LLM outputs
3. Submit ratings to Snowflake for analysis

Usage:
    streamlit run src/dashboard/clinician_review_dashboard.py
"""

import os
import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime, timezone
from dotenv import load_dotenv
from typing import Optional

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="MaternoScope - Clinician Review",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'snowflake_conn' not in st.session_state:
    st.session_state.snowflake_conn = None
if 'posts_df' not in st.session_state:
    st.session_state.posts_df = None
if 'current_post_idx' not in st.session_state:
    st.session_state.current_post_idx = 0
if 'ratings_submitted' not in st.session_state:
    st.session_state.ratings_submitted = []


def connect_snowflake():
    """Connect to Snowflake and store connection in session state."""
    if st.session_state.snowflake_conn is None:
        try:
            conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USERNAME"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS_ML"),
                role=os.getenv("SNOWFLAKE_ROLE")
            )
            st.session_state.snowflake_conn = conn
            return conn
        except Exception as e:
            st.error(f"Error connecting to Snowflake: {e}")
            return None
    return st.session_state.snowflake_conn


def create_ratings_table_if_not_exists():
    """Create the clinician ratings table if it doesn't exist."""
    conn = connect_snowflake()
    if conn is None:
        return False

    try:
        cursor = conn.cursor()

        # Create schema if it doesn't exist
        schema_sql = ("CREATE SCHEMA IF NOT EXISTS "
                      "MATERNOSCOPE.ANALYTICS_REF")
        cursor.execute(schema_sql)

        # Create ratings table
        # Note: Snowflake doesn't support inline CHECK constraints
        # Validation should be handled in the application layer
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS
        MATERNOSCOPE.ANALYTICS_REF.CLINICIAN_RATINGS (
            RATING_ID VARCHAR(255) PRIMARY KEY,
            POST_ID VARCHAR(255) NOT NULL,
            PROMPT_HASH VARCHAR(50),
            CLINICIAN_NAME VARCHAR(255) NOT NULL,
            CLINICIAN_EMAIL VARCHAR(255),
            RATING_OVERALL NUMBER(1,0) NOT NULL,
            RATING_ACCURACY NUMBER(1,0),
            RATING_EMPATHY NUMBER(1,0),
            RATING_SAFETY NUMBER(1,0),
            RATING_RELEVANCE NUMBER(1,0),
            COMMENTS VARCHAR(5000),
            RATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Check and add PROMPT_HASH column for existing tables (migration support)
        try:
            check_column_sql = """
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'ANALYTICS_REF' 
            AND TABLE_NAME = 'CLINICIAN_RATINGS' 
            AND COLUMN_NAME = 'PROMPT_HASH'
            """
            cursor.execute(check_column_sql)
            column_exists = cursor.fetchone()[0] > 0
            
            if not column_exists:
                alter_table_sql = """
                ALTER TABLE MATERNOSCOPE.ANALYTICS_REF.CLINICIAN_RATINGS 
                ADD COLUMN PROMPT_HASH VARCHAR(50)
                """
                cursor.execute(alter_table_sql)
        except Exception:
            # Column might already exist or table doesn't exist yet
            pass
        
        cursor.close()
        return True
    except Exception as e:
        st.error(f"Error creating ratings table: {e}")
        return False


def fetch_posts(limit: int = 100,
                subreddit: Optional[str] = None,
                primary_topic: Optional[str] = None,
                urgency_min: Optional[int] = None,
                prompt_hash: Optional[str] = None,
                reviewed_filter: Optional[str] = None):
    """Fetch posts from Snowflake for review.
    
    Args:
        reviewed_filter: "all", "reviewed", or "not_reviewed"
    """
    conn = connect_snowflake()
    if conn is None:
        return None

    try:
        # Join REDDIT_POSTS_ANNOTATED with staging table and ratings table
        query = """
        SELECT
            a.POST_ID,
            COALESCE(s.POST_DATE, a.ANNOTATED_AT::DATE) as POST_DATE,
            s.SUBREDDIT,
            s.POST_TITLE,
            s.POST_URL,
            a.TEXT_FOR_LLM,
            a.POST_SUMMARY,
            a.CARE_RESPONSE,
            a.PRIMARY_GROUP,
            a.PRIMARY_TOPIC,
            a.TRIMESTER,
            a.SENTIMENT,
            a.URGENCY_0_3,
            a.KEYWORDS,
            a.SAFETY_FLAGS,
            a.MODEL_NAME,
            a.MODEL_VERSION,
            a.PROMPT_HASH,
            a.ANNOTATED_AT,
            CASE WHEN r.RATING_ID IS NOT NULL
                THEN TRUE ELSE FALSE END as IS_REVIEWED
        FROM MATERNOSCOPE.ANALYTICS_ML.REDDIT_POSTS_ANNOTATED a
        LEFT JOIN MATERNOSCOPE.BRONZE.STG_REDDIT_POSTS_PII s
            ON a.POST_ID = s.POST_ID
        LEFT JOIN MATERNOSCOPE.ANALYTICS_REF.CLINICIAN_RATINGS r
            ON a.POST_ID = r.POST_ID
            AND (a.PROMPT_HASH = r.PROMPT_HASH
                 OR (a.PROMPT_HASH IS NULL
                     AND r.PROMPT_HASH IS NULL))
        WHERE a.CARE_RESPONSE IS NOT NULL
        """
        
        conditions = []
        if subreddit:
            conditions.append(f"s.SUBREDDIT = '{subreddit.replace("'", "''")}'")
        if primary_topic:
            conditions.append(
                f"a.PRIMARY_TOPIC = '{primary_topic.replace("'", "''")}'"
            )
        if urgency_min is not None:
            conditions.append(f"a.URGENCY_0_3 >= {urgency_min}")
        if prompt_hash:
            conditions.append(
                f"a.PROMPT_HASH = '{prompt_hash.replace("'", "''")}'"
            )
        if reviewed_filter == "reviewed":
            conditions.append("r.RATING_ID IS NOT NULL")
        elif reviewed_filter == "not_reviewed":
            conditions.append("r.RATING_ID IS NULL")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        # Use string formatting for LIMIT (Snowflake connector works better this way)
        query += (
            f" ORDER BY COALESCE(s.POST_DATE, a.ANNOTATED_AT::DATE) DESC, "
            f"a.ANNOTATED_AT DESC LIMIT {limit}"
        )
        
        df = pd.read_sql(query, conn)
        
        # Convert arrays to readable strings
        if 'KEYWORDS' in df.columns:
            df['KEYWORDS_STR'] = df['KEYWORDS'].apply(
                lambda x: (', '.join(x) if isinstance(x, list)
                          else str(x) if x else 'None')
            )
        if 'SAFETY_FLAGS' in df.columns:
            df['SAFETY_FLAGS_STR'] = df['SAFETY_FLAGS'].apply(
                lambda x: (', '.join(x) if isinstance(x, list)
                          else str(x) if x else 'None')
            )
        
        return df
    except Exception as e:
        st.error(f"Error fetching posts: {e}")
        return None


def get_available_subreddits():
    """Fetch list of available subreddits from Snowflake."""
    conn = connect_snowflake()
    if conn is None:
        return []
    
    try:
        query = """
        SELECT DISTINCT s.SUBREDDIT
        FROM MATERNOSCOPE.ANALYTICS_ML.REDDIT_POSTS_ANNOTATED a
        LEFT JOIN MATERNOSCOPE.BRONZE.STG_REDDIT_POSTS_PII s
            ON a.POST_ID = s.POST_ID
        WHERE a.CARE_RESPONSE IS NOT NULL
            AND s.SUBREDDIT IS NOT NULL
        ORDER BY s.SUBREDDIT
        """
        
        df = pd.read_sql(query, conn)
        subreddits = df['SUBREDDIT'].dropna().unique().tolist()
        return sorted(subreddits) if subreddits else []
    except Exception as e:
        st.error(f"Error fetching subreddits: {e}")
        return []


def get_available_prompt_hashes():
    """Fetch list of available prompt hashes from Snowflake."""
    conn = connect_snowflake()
    if conn is None:
        return []
    
    try:
        query = """
        SELECT DISTINCT a.PROMPT_HASH
        FROM MATERNOSCOPE.ANALYTICS_ML.REDDIT_POSTS_ANNOTATED a
        WHERE a.CARE_RESPONSE IS NOT NULL
            AND a.PROMPT_HASH IS NOT NULL
        ORDER BY a.PROMPT_HASH
        """
        
        df = pd.read_sql(query, conn)
        prompt_hashes = df['PROMPT_HASH'].dropna().unique().tolist()
        return sorted(prompt_hashes) if prompt_hashes else []
    except Exception as e:
        st.error(f"Error fetching prompt hashes: {e}")
        return []


def submit_rating(post_id: str, prompt_hash: Optional[str],
                 clinician_name: str, clinician_email: str,
                 rating_overall: int, rating_accuracy: Optional[int],
                 rating_empathy: Optional[int], rating_safety: Optional[int],
                 rating_relevance: Optional[int], comments: str) -> bool:
    """Submit a rating to Snowflake."""
    # Ensure the ratings table exists before attempting to insert
    if not create_ratings_table_if_not_exists():
        st.error("Failed to create or verify ratings table.")
        return False
    
    conn = connect_snowflake()
    if conn is None:
        return False

    try:
        cursor = conn.cursor()

        # Generate rating ID
        rating_id = (f"{post_id}_{clinician_name}_"
                    f"{datetime.now(timezone.utc).isoformat()}")
        rating_id = rating_id.replace(':', '-').replace('.', '-')[:255]

        # Escape single quotes in string values to prevent SQL injection
        def escape_sql_string(value):
            if value is None or value == '':
                return 'NULL'
            # Escape single quotes by doubling them
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"
        
        # Format values for SQL
        rating_id_escaped = escape_sql_string(rating_id)
        post_id_escaped = escape_sql_string(post_id)
        prompt_hash_escaped = escape_sql_string(prompt_hash)
        clinician_name_escaped = escape_sql_string(clinician_name)
        clinician_email_escaped = escape_sql_string(
            clinician_email if clinician_email else None
        )
        comments_escaped = escape_sql_string(comments if comments else None)
        
        # Format numeric values (handle None)
        rating_accuracy_val = (
            str(rating_accuracy) if rating_accuracy is not None else 'NULL'
        )
        rating_empathy_val = (
            str(rating_empathy) if rating_empathy is not None else 'NULL'
        )
        rating_safety_val = (
            str(rating_safety) if rating_safety is not None else 'NULL'
        )
        rating_relevance_val = (
            str(rating_relevance) if rating_relevance is not None else 'NULL'
        )
        
        insert_sql = f"""
        INSERT INTO MATERNOSCOPE.ANALYTICS_REF.CLINICIAN_RATINGS (
            RATING_ID, POST_ID, PROMPT_HASH, CLINICIAN_NAME, CLINICIAN_EMAIL,
            RATING_OVERALL, RATING_ACCURACY, RATING_EMPATHY,
            RATING_SAFETY, RATING_RELEVANCE, COMMENTS, RATED_AT
        ) VALUES (
            {rating_id_escaped},
            {post_id_escaped},
            {prompt_hash_escaped},
            {clinician_name_escaped},
            {clinician_email_escaped},
            {rating_overall},
            {rating_accuracy_val},
            {rating_empathy_val},
            {rating_safety_val},
            {rating_relevance_val},
            {comments_escaped},
            CURRENT_TIMESTAMP()
        )
        """
        
        cursor.execute(insert_sql)
        
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        st.error(f"Error submitting rating: {e}")
        return False


def main():
    """Main dashboard application."""

    # Header
    st.title("🏥 MaternoScope - Clinician Review Dashboard")
    st.markdown("Review and rate LLM-generated annotations "
                "for Reddit posts")
    
    # Ensure ratings table exists on startup
    create_ratings_table_if_not_exists()
    
    # Sidebar for filters and navigation
    with st.sidebar:
        st.header("🔍 Filters")
        
        # Initialize ratings table (manual button for re-creation if needed)
        if st.button("Initialize Ratings Table"):
            with st.spinner("Creating ratings table..."):
                if create_ratings_table_if_not_exists():
                    st.success("Ratings table created successfully!")
                else:
                    st.error("Failed to create ratings table")
        
        st.divider()
        
        # Fetch options
        limit = st.number_input("Number of posts to load",
                                min_value=10, max_value=1000,
                                value=100, step=10)

        # Get available subreddits for dropdown
        available_subreddits = get_available_subreddits()
        subreddit_options = [None] + available_subreddits
        
        subreddit_filter = st.selectbox(
            "Filter by Subreddit (optional)",
            options=subreddit_options,
            format_func=lambda x: "All Subreddits" if x is None else x,
            index=0
        )
        topics = [
            None, "symptoms_body_changes", "medications_supplements",
            "test_results_labs", "pregnancy_complications", "labor_delivery",
            "anxiety_fear_uncertainty", "mood_depression",
            "body_image_identity", "relationship_stress",
            "peer_support_requests", "nutrition_diet", "exercise_movement",
            "sleep_fatigue", "work_leave_career", "postpartum_care",
            "choosing_provider", "hospital_clinic_experiences",
            "insurance_costs", "telehealth_virtual_care",
            "system_barriers_equity", "ask_experiences_advice",
            "share_stories_outcomes", "product_device_discussions",
            "information_validation_misinformation",
            "question_seeking_info", "experience_sharing_narrative",
            "opinion_rant_vent", "announcement_milestone",
            "policy_advocacy_news"
        ]
        primary_topic_filter = st.selectbox(
            "Filter by Primary Topic (optional)",
            topics
        )
        
        urgency_filter = st.selectbox(
            "Minimum Urgency Level (optional)",
            [None, 0, 1, 2, 3],
            format_func=lambda x: ("All" if x is None
                                  else f"Urgency {x}+")
        )
        
        # Get available prompt hashes for dropdown
        available_prompt_hashes = get_available_prompt_hashes()
        prompt_hash_options = [None] + available_prompt_hashes
        
        def format_prompt_hash(x):
            if x is None:
                return "All Prompts"
            x_str = str(x)
            return x_str[:20] + "..." if len(x_str) > 20 else x_str
        
        prompt_hash_filter = st.selectbox(
            "Filter by Prompt Hash (optional)",
            options=prompt_hash_options,
            format_func=format_prompt_hash,
            index=0
        )
        
        reviewed_filter = st.selectbox(
            "Filter by Review Status",
            options=["all", "reviewed", "not_reviewed"],
            format_func=lambda x: {
                "all": "All Posts",
                "reviewed": "Reviewed Posts",
                "not_reviewed": "Not Reviewed Posts"
            }[x],
            index=0
        )
        
        if st.button("🔄 Load Posts", type="primary"):
            with st.spinner("Loading posts from Snowflake..."):
                df = fetch_posts(
                    limit=limit,
                    subreddit=subreddit_filter if subreddit_filter else None,
                    primary_topic=primary_topic_filter,
                    urgency_min=urgency_filter,
                    prompt_hash=prompt_hash_filter if prompt_hash_filter else None,
                    reviewed_filter=reviewed_filter
                )
                if df is not None and len(df) > 0:
                    st.session_state.posts_df = df
                    st.session_state.current_post_idx = 0
                    st.success(f"Loaded {len(df)} posts")
                elif df is not None:
                    st.warning("No posts found matching criteria")
                else:
                    st.error("Failed to load posts")
        
        st.divider()
        
        # Navigation
        if st.session_state.posts_df is not None and len(st.session_state.posts_df) > 0:
            st.header("📄 Navigation")
            total_posts = len(st.session_state.posts_df)
            current_idx = st.session_state.current_post_idx
            
            st.metric("Current Post", f"{current_idx + 1} / {total_posts}")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("◀ Previous", disabled=(current_idx == 0)):
                    st.session_state.current_post_idx = max(0, current_idx - 1)
                    st.rerun()
            with col2:
                if st.button("Next ▶", disabled=(current_idx >= total_posts - 1)):
                    st.session_state.current_post_idx = min(total_posts - 1, current_idx + 1)
                    st.rerun()
            
            # Jump to post
            jump_to = st.number_input("Jump to post #",
                                     min_value=1,
                                     max_value=total_posts,
                                     value=current_idx + 1)
            if st.button("Go"):
                st.session_state.current_post_idx = jump_to - 1
                st.rerun()
    
    # Main content area
    if st.session_state.posts_df is None or len(st.session_state.posts_df) == 0:
        st.info("👈 Use the sidebar to load posts from Snowflake")
    else:
        df = st.session_state.posts_df
        current_idx = st.session_state.current_post_idx
        post = df.iloc[current_idx]
        
        # Post information
        col1, col2 = st.columns([2, 1])
        
        with col1:
            title = post.get('POST_TITLE', 'No Title')
            if title and pd.notna(title):
                title_display = title[:100] + "..." if len(str(title)) > 100 else title
            else:
                title_display = "No Title"
            st.header(f"📝 Post: {title_display}")
        
        with col2:
            if post.get('POST_URL') and pd.notna(post['POST_URL']):
                st.markdown(f"[🔗 Open Original Post]({post['POST_URL']})")
        
        # Post metadata
        meta_col1, meta_col2, meta_col3, meta_col4 = st.columns(4)
        with meta_col1:
            st.metric("Subreddit", post['SUBREDDIT'])
        with meta_col2:
            date_str = (post['POST_DATE'].strftime('%Y-%m-%d')
                       if pd.notna(post['POST_DATE']) else 'N/A')
            st.metric("Date", date_str)
        with meta_col3:
            urgency_str = (f"{int(post['URGENCY_0_3'])}/3"
                          if pd.notna(post['URGENCY_0_3']) else 'N/A')
            st.metric("Urgency", urgency_str)
        with meta_col4:
            st.metric("Sentiment", post['SENTIMENT'] if pd.notna(post['SENTIMENT']) else 'N/A')
        
        # Add custom CSS for independent column scrolling
        st.markdown("""
        <style>
        /* Create scrollable containers for each column */
        .scrollable-container {
            max-height: 85vh;
            overflow-y: auto;
            overflow-x: hidden;
            padding-right: 10px;
            padding-bottom: 20px;
        }
        /* Custom scrollbar styling */
        .scrollable-container::-webkit-scrollbar {
            width: 8px;
        }
        .scrollable-container::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 10px;
        }
        .scrollable-container::-webkit-scrollbar-thumb {
            background: #888;
            border-radius: 10px;
        }
        .scrollable-container::-webkit-scrollbar-thumb:hover {
            background: #555;
        }
        /* Firefox scrollbar */
        .scrollable-container {
            scrollbar-width: thin;
            scrollbar-color: #888 #f1f1f1;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Two-column layout with independent scrolling
        left_col, right_col = st.columns([1, 1], gap="large")
        
        # Left Column: Post Content (PII-Redacted)
        with left_col:
            st.markdown('<div class="scrollable-container">', unsafe_allow_html=True)
            
            st.subheader("📝 Post Content (PII-Redacted - sent to LLM)")
            st.markdown(f"**Post ID:** `{post['POST_ID']}`")
            
            if pd.notna(post['TEXT_FOR_LLM']) and post['TEXT_FOR_LLM']:
                # Use dynamic key based on post_id to ensure content updates
                st.text_area("", post['TEXT_FOR_LLM'], height=400,
                            disabled=True, key=f"text_for_llm_{post['POST_ID']}")
            else:
                st.info("No content available")
            
            st.markdown("---")
            
            # LLM Annotation Summary
            st.subheader("🤖 LLM Annotation Summary")
            
            summary_col1, summary_col2 = st.columns(2)
            with summary_col1:
                pg = post['PRIMARY_GROUP']
                st.markdown(f"**Primary Group:** "
                           f"{pg if pd.notna(pg) else 'N/A'}")
                pt = post['PRIMARY_TOPIC']
                st.markdown(f"**Primary Topic:** "
                           f"{pt if pd.notna(pt) else 'N/A'}")
                tr = post['TRIMESTER']
                st.markdown(f"**Trimester:** "
                           f"{tr if pd.notna(tr) else 'N/A'}")
            with summary_col2:
                mn = post['MODEL_NAME']
                st.markdown(f"**Model:** "
                           f"{mn if pd.notna(mn) else 'N/A'}")
                mv = post['MODEL_VERSION']
                st.markdown(f"**Version:** "
                           f"{mv if pd.notna(mv) else 'N/A'}")
                if pd.notna(post['ANNOTATED_AT']):
                    st.markdown(f"**Annotated At:** {post['ANNOTATED_AT']}")
            
            if pd.notna(post['POST_SUMMARY']) and post['POST_SUMMARY']:
                st.markdown("**Post Summary:**")
                st.info(post['POST_SUMMARY'])
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Right Column: LLM Output and Rating
        with right_col:
            st.markdown('<div class="scrollable-container">', unsafe_allow_html=True)
            
            # Keywords above Care Response
            if 'KEYWORDS_STR' in post and post['KEYWORDS_STR']:
                st.markdown("**Keywords:**")
                st.code(post['KEYWORDS_STR'])
                st.markdown("---")
            
            # LLM-Generated Care Response
            st.subheader("💬 LLM-Generated Care Response")
            if pd.notna(post['CARE_RESPONSE']) and post['CARE_RESPONSE']:
                # Use dynamic key based on post_id to ensure content updates
                st.text_area("", post['CARE_RESPONSE'], height=250,
                            disabled=True, key=f"care_response_{post['POST_ID']}")
            else:
                st.warning("No care response available")
            
            # Safety Flags below Care Response
            if ('SAFETY_FLAGS_STR' in post and
                post['SAFETY_FLAGS_STR'] and
                post['SAFETY_FLAGS_STR'] != 'None'):
                st.markdown("**Safety Flags:**")
                st.warning(post['SAFETY_FLAGS_STR'])
            
            st.markdown("---")
            
            # Rating Section
            st.subheader("⭐ Rate This LLM Output")
            
            # Clinician information
            clinician_name = st.text_input("Your Name *",
                                          key="clinician_name")
            clinician_email = st.text_input("Your Email (optional)",
                                           key="clinician_email")
            
            st.divider()
            
            # Ratings
            st.markdown("### Rating Scale (1 = Poor, 5 = Excellent)")
            
            rating_overall = st.slider(
                "Overall Quality *",
                min_value=1,
                max_value=5,
                value=3,
                key="rating_overall"
            )
            
            rating_col1, rating_col2 = st.columns(2)
            with rating_col1:
                rating_accuracy = st.slider(
                    "Accuracy (correctness of medical information)",
                    min_value=1, max_value=5, value=3,
                    key="rating_accuracy"
                )

                rating_empathy = st.slider(
                    "Empathy (tone and emotional support)",
                    min_value=1, max_value=5, value=3,
                    key="rating_empathy"
                )

            with rating_col2:
                rating_safety = st.slider(
                    "Safety (appropriate escalation and warnings)",
                    min_value=1, max_value=5, value=3,
                    key="rating_safety"
                )

                rating_relevance = st.slider(
                    "Relevance (addresses the post's concerns)",
                    min_value=1, max_value=5, value=3,
                    key="rating_relevance"
                )
            
            st.divider()
            
            # Comments
            comments = st.text_area(
                "Additional Comments (optional)",
                height=150,
                placeholder=("Provide specific feedback on what worked "
                           "well or what could be improved..."),
                key="comments"
            )
            
            # Submit button
            if st.button("✅ Submit Rating", type="primary", use_container_width=True):
                if not clinician_name:
                    st.error("Please enter your name")
                else:
                    with st.spinner("Submitting rating..."):
                        prompt_hash = post.get('PROMPT_HASH') if pd.notna(post.get('PROMPT_HASH')) else None
                        success = submit_rating(
                            post_id=post['POST_ID'],
                            prompt_hash=prompt_hash,
                            clinician_name=clinician_name,
                            clinician_email=clinician_email,
                            rating_overall=rating_overall,
                            rating_accuracy=rating_accuracy,
                            rating_empathy=rating_empathy,
                            rating_safety=rating_safety,
                            rating_relevance=rating_relevance,
                            comments=comments
                        )
                        
                        if success:
                            # Track submitted rating
                            title = post.get('POST_TITLE', 'No Title')
                            if title and pd.notna(title):
                                title_display = title[:50]
                            else:
                                title_display = 'No Title'
                            
                            st.session_state.ratings_submitted.append({
                                'post_id': post['POST_ID'],
                                'post_title': title_display,
                                'timestamp': datetime.now(timezone.utc)
                            })
                            
                            # Move to next post if available
                            total_posts = len(st.session_state.posts_df)
                            current_idx = st.session_state.current_post_idx
                            if current_idx < total_posts - 1:
                                st.session_state.current_post_idx = current_idx + 1
                                st.success("✅ Rating submitted successfully! Moving to next post...")
                            else:
                                st.success("✅ Rating submitted successfully! (Last post in list)")
                            
                            # Refresh the page to show next post and clear the form
                            st.rerun()
                        else:
                            st.error("Failed to submit rating. Please try again.")
            
            # Show submitted ratings for this session
            if st.session_state.ratings_submitted:
                st.divider()
                st.markdown("### Ratings Submitted This Session")
                submitted_df = pd.DataFrame(
                    st.session_state.ratings_submitted
                )
                st.dataframe(
                    submitted_df[['post_id', 'post_title', 'timestamp']],
                    use_container_width=True
                )
            
            # Close scrollable container for right column
            st.markdown('</div>', unsafe_allow_html=True)


if __name__ == "__main__":
    main()
