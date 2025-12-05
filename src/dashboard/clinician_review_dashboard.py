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
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS
        MATERNOSCOPE.ANALYTICS_REF.CLINICIAN_RATINGS (
            RATING_ID VARCHAR(255) PRIMARY KEY,
            POST_ID VARCHAR(255) NOT NULL,
            CLINICIAN_NAME VARCHAR(255) NOT NULL,
            CLINICIAN_EMAIL VARCHAR(255),
            RATING_OVERALL NUMBER(1,0) NOT NULL
                CHECK (RATING_OVERALL >= 1 AND RATING_OVERALL <= 5),
            RATING_ACCURACY NUMBER(1,0)
                CHECK (RATING_ACCURACY >= 1 AND RATING_ACCURACY <= 5),
            RATING_EMPATHY NUMBER(1,0)
                CHECK (RATING_EMPATHY >= 1 AND RATING_EMPATHY <= 5),
            RATING_SAFETY NUMBER(1,0)
                CHECK (RATING_SAFETY >= 1 AND RATING_SAFETY <= 5),
            RATING_RELEVANCE NUMBER(1,0)
                CHECK (RATING_RELEVANCE >= 1 AND RATING_RELEVANCE <= 5),
            COMMENTS VARCHAR(5000),
            RATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        
        cursor.execute(create_table_sql)
        cursor.close()
        return True
    except Exception as e:
        st.error(f"Error creating ratings table: {e}")
        return False


def fetch_posts(limit: int = 100,
                subreddit: Optional[str] = None,
                primary_topic: Optional[str] = None,
                urgency_min: Optional[int] = None):
    """Fetch posts from Snowflake for review."""
    conn = connect_snowflake()
    if conn is None:
        return None

    try:
        # Join REDDIT_POSTS_ANNOTATED with staging table to get post metadata
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
            a.ANNOTATED_AT
        FROM MATERNOSCOPE.ANALYTICS_ML.REDDIT_POSTS_ANNOTATED a
        LEFT JOIN MATERNOSCOPE.BRONZE.STG_REDDIT_POSTS_PII s
            ON a.POST_ID = s.POST_ID
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


def submit_rating(post_id: str, clinician_name: str,
                 clinician_email: str, rating_overall: int,
                 rating_accuracy: Optional[int],
                 rating_empathy: Optional[int],
                 rating_safety: Optional[int],
                 rating_relevance: Optional[int],
                 comments: str) -> bool:
    """Submit a rating to Snowflake."""
    conn = connect_snowflake()
    if conn is None:
        return False

    try:
        cursor = conn.cursor()

        # Generate rating ID
        rating_id = (f"{post_id}_{clinician_name}_"
                    f"{datetime.now(timezone.utc).isoformat()}")
        rating_id = rating_id.replace(':', '-').replace('.', '-')[:255]

        insert_sql = """
        INSERT INTO MATERNOSCOPE.ANALYTICS_REF.CLINICIAN_RATINGS (
            RATING_ID, POST_ID, CLINICIAN_NAME, CLINICIAN_EMAIL,
            RATING_OVERALL, RATING_ACCURACY, RATING_EMPATHY,
            RATING_SAFETY, RATING_RELEVANCE, COMMENTS
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.execute(insert_sql, (
            rating_id,
            post_id,
            clinician_name,
            clinician_email if clinician_email else None,
            rating_overall,
            rating_accuracy if rating_accuracy else None,
            rating_empathy if rating_empathy else None,
            rating_safety if rating_safety else None,
            rating_relevance if rating_relevance else None,
            comments if comments else None
        ))
        
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
    
    # Sidebar for filters and navigation
    with st.sidebar:
        st.header("🔍 Filters")
        
        # Initialize ratings table
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

        subreddit_filter = st.text_input("Filter by Subreddit (optional)",
                                        "")
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
        
        if st.button("🔄 Load Posts", type="primary"):
            with st.spinner("Loading posts from Snowflake..."):
                df = fetch_posts(
                    limit=limit,
                    subreddit=subreddit_filter if subreddit_filter else None,
                    primary_topic=primary_topic_filter,
                    urgency_min=urgency_filter
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
        /* Target Streamlit columns for independent scrolling */
        div[data-testid="column"] {
            max-height: 85vh;
            overflow-y: auto;
            overflow-x: hidden;
            padding-right: 10px;
        }
        /* Custom scrollbar styling */
        div[data-testid="column"]::-webkit-scrollbar {
            width: 8px;
        }
        div[data-testid="column"]::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 10px;
        }
        div[data-testid="column"]::-webkit-scrollbar-thumb {
            background: #888;
            border-radius: 10px;
        }
        div[data-testid="column"]::-webkit-scrollbar-thumb:hover {
            background: #555;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Two-column layout with independent scrolling
        left_col, right_col = st.columns([1, 1], gap="large")
        
        # Left Column: Post Content (PII-Redacted)
        with left_col:
            st.subheader("📝 Post Content (PII-Redacted - sent to LLM)")
            st.markdown(f"**Post ID:** `{post['POST_ID']}`")
            
            if pd.notna(post['TEXT_FOR_LLM']) and post['TEXT_FOR_LLM']:
                st.text_area("", post['TEXT_FOR_LLM'], height=600,
                            disabled=True, key="text_for_llm")
            else:
                st.info("No content available")
        
        # Right Column: LLM Output and Rating
        with right_col:
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
            
            if 'KEYWORDS_STR' in post and post['KEYWORDS_STR']:
                st.markdown("**Keywords:**")
                st.code(post['KEYWORDS_STR'])
            
            if ('SAFETY_FLAGS_STR' in post and
                post['SAFETY_FLAGS_STR'] and
                post['SAFETY_FLAGS_STR'] != 'None'):
                st.markdown("**Safety Flags:**")
                st.warning(post['SAFETY_FLAGS_STR'])
            
            st.markdown("---")
            
            # LLM-Generated Care Response
            st.subheader("💬 LLM-Generated Care Response")
            if pd.notna(post['CARE_RESPONSE']) and post['CARE_RESPONSE']:
                st.text_area("", post['CARE_RESPONSE'], height=250,
                            disabled=True, key="care_response")
            else:
                st.warning("No care response available")
            
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
                        success = submit_rating(
                            post_id=post['POST_ID'],
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
                            st.success("✅ Rating submitted successfully!")
                            st.balloons()
                            
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
                            
                            # Clear form
                            st.session_state.clinician_name = ""
                            st.session_state.clinician_email = ""
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


if __name__ == "__main__":
    main()
