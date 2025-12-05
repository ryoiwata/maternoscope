#!/usr/bin/env python3
"""
LLM Annotation Audit Dashboard for Streamlit in Snowflake (SiS)

This dashboard allows human reviewers to audit LLM annotations and provide
corrections. It reads from LLM annotations and writes evaluations back to
Snowflake.

Usage:
    Deploy to Streamlit in Snowflake (SiS) environment
"""

import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from typing import Optional, Dict, Any

# ============================================================================
# CONFIGURATION - Update these for your environment
# ============================================================================

DATABASE_NAME = "YOUR_DATABASE_NAME"  # e.g., "MATERNOSCOPE"
LLM_EVAL_SCHEMA = "LLM_EVAL_SCHEMA"  # e.g., "ANALYTICS_ML"
LLM_ANNOTATIONS_TABLE = "REDDIT_POSTS_ANNOTATED"  # Source Table A
HUMAN_EVALUATION_TABLE = "HUMAN_EVALUATION"  # Target Table B

# Taxonomy definitions
PRIMARY_GROUPS = [
    "clinical",
    "mental_health",
    "lifestyle_parenting",
    "access_navigation",
    "community_info",
    "meta_context"
]

# Topics by group
TOPICS_BY_GROUP = {
    "clinical": [
        "symptoms_body_changes",
        "medications_supplements",
        "test_results_labs",
        "pregnancy_complications",
        "labor_delivery"
    ],
    "mental_health": [
        "anxiety_fear_uncertainty",
        "mood_depression",
        "body_image_identity",
        "relationship_stress",
        "peer_support_requests"
    ],
    "lifestyle_parenting": [
        "nutrition_diet",
        "exercise_movement",
        "sleep_fatigue",
        "work_leave_career",
        "postpartum_care"
    ],
    "access_navigation": [
        "choosing_provider",
        "hospital_clinic_experiences",
        "insurance_costs",
        "telehealth_virtual_care",
        "system_barriers_equity"
    ],
    "community_info": [
        "ask_experiences_advice",
        "share_stories_outcomes",
        "product_device_discussions",
        "information_validation_misinformation"
    ],
    "meta_context": [
        "question_seeking_info",
        "experience_sharing_narrative",
        "opinion_rant_vent",
        "announcement_milestone",
        "policy_advocacy_news"
    ]
}

TRIMESTER_OPTIONS = [
    "preconception",
    "first",
    "second",
    "third",
    "pregnant",
    "postpartum",
    "miscarriage",
    "unclear"
]

ACCURACY_SCORES = [1, 2, 3, 4, 5]

# ============================================================================
# SNOWFLAKE CONNECTION
# ============================================================================


@st.cache_resource
def get_snowflake_session() -> Session:
    """Get active Snowflake session for Streamlit in Snowflake."""
    try:
        session = get_active_session()
        return session
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        st.stop()


# ============================================================================
# DATA RETRIEVAL FUNCTIONS
# ============================================================================


def get_unevaluated_post(session: Session) -> Optional[Dict[str, Any]]:
    """
    Get the next un-evaluated post by checking against HUMAN_EVALUATION.

    Returns the first post from LLM_ANNOTATIONS that doesn't have a
    corresponding entry in HUMAN_EVALUATION.
    """
    try:
        # Query to find unevaluated posts
        query = f"""
        SELECT 
            a.post_id,
            a.text_for_llm,
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
            a.annotated_at
        FROM {DATABASE_NAME}.{LLM_EVAL_SCHEMA}.{LLM_ANNOTATIONS_TABLE} a
        LEFT JOIN {DATABASE_NAME}.{LLM_EVAL_SCHEMA}.{HUMAN_EVALUATION_TABLE} h
            ON a.post_id = h.post_id
        WHERE h.post_id IS NULL
        ORDER BY a.annotated_at DESC
        LIMIT 1
        """
        
        df = session.sql(query).collect()
        
        if len(df) == 0:
            return None
        
        # Convert to dictionary
        row = df[0]
        post_data = {
            "post_id": row["POST_ID"],
            "text_for_llm": row["TEXT_FOR_LLM"] or "",
            "primary_group": row["PRIMARY_GROUP"] or "",
            "primary_topic": row["PRIMARY_TOPIC"] or "",
            "secondary_topics": row["SECONDARY_TOPICS"] or [],
            "trimester": row["TRIMESTER"] or "",
            "sentiment": row["SENTIMENT"] or "",
            "urgency_0_3": row["URGENCY_0_3"] or 0,
            "keywords": row["KEYWORDS"] or [],
            "safety_flags": row["SAFETY_FLAGS"] or [],
            "post_summary": row["POST_SUMMARY"] or "",
            "care_response": row["CARE_RESPONSE"] or "",
            "model_name": row["MODEL_NAME"] or "",
            "model_version": row["MODEL_VERSION"] or "",
            "prompt_hash": row["PROMPT_HASH"] or "",
            "annotated_at": row["ANNOTATED_AT"]
        }
        
        return post_data
        
    except Exception as e:
        st.error(f"Error fetching unevaluated post: {e}")
        return None


def save_evaluation(
    session: Session,
    post_id: str,
    corrected_primary_group: str,
    corrected_primary_topic: str,
    corrected_trimester: str,
    corrected_keywords: str,
    accuracy_score: int,
    evaluation_comments: str
) -> bool:
    """
    Save human evaluation to HUMAN_EVALUATION table.
    
    Returns True if successful, False otherwise.
    """
    try:
        # Parse keywords (comma-separated string to array)
        keywords_list = [
            kw.strip() for kw in corrected_keywords.split(",")
            if kw.strip()
        ]
        
        # Create array literal for Snowflake
        if keywords_list:
            kw_escaped = [f"'{kw.replace("'", "''")}'" for kw in keywords_list]
            keywords_array = "ARRAY_CONSTRUCT(" + ", ".join(kw_escaped) + ")"
        else:
            keywords_array = "ARRAY_CONSTRUCT()"
        
        # Insert evaluation
        insert_query = f"""
        INSERT INTO {DATABASE_NAME}.{LLM_EVAL_SCHEMA}.{HUMAN_EVALUATION_TABLE} (
            post_id,
            corrected_primary_group,
            corrected_primary_topic,
            corrected_trimester,
            corrected_keywords,
            accuracy_score,
            evaluation_comments,
            evaluated_at,
            evaluator_name
        ) VALUES (
            '{post_id.replace("'", "''")}',
            '{corrected_primary_group.replace("'", "''")}',
            '{corrected_primary_topic.replace("'", "''")}',
            '{corrected_trimester.replace("'", "''")}',
            {keywords_array},
            {accuracy_score},
            '{evaluation_comments.replace("'", "''")}',
            CURRENT_TIMESTAMP(),
            CURRENT_USER()
        )
        """
        
        session.sql(insert_query).collect()
        return True
        
    except Exception as e:
        st.error(f"Error saving evaluation: {e}")
        return False


def ensure_evaluation_table_exists(session: Session) -> bool:
    """
    Ensure the HUMAN_EVALUATION table exists.
    Creates it if it doesn't exist.
    """
    try:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{LLM_EVAL_SCHEMA}.{HUMAN_EVALUATION_TABLE} (
            post_id VARCHAR(255) PRIMARY KEY,
            corrected_primary_group VARCHAR(50),
            corrected_primary_topic VARCHAR(100),
            corrected_trimester VARCHAR(20),
            corrected_keywords ARRAY,
            accuracy_score INTEGER,
            evaluation_comments VARCHAR(5000),
            evaluated_at TIMESTAMP_TZ,
            evaluator_name VARCHAR(255)
        )
        """
        
        session.sql(create_table_query).collect()
        return True
        
    except Exception as e:
        st.error(f"Error creating evaluation table: {e}")
        return False


# ============================================================================
# STREAMLIT UI
# ============================================================================


def format_array_display(arr) -> str:
    """Format array for display."""
    if not arr:
        return "None"
    if isinstance(arr, list):
        return ", ".join(str(item) for item in arr)
    return str(arr)


def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="LLM Annotation Audit Dashboard",
        page_icon="🔍",
        layout="wide"
    )
    
    st.title("🔍 LLM Annotation Audit Dashboard")
    st.markdown("---")
    
    # Get Snowflake session
    session = get_snowflake_session()
    
    # Ensure evaluation table exists
    if not ensure_evaluation_table_exists(session):
        st.error("Failed to initialize evaluation table. Please check permissions.")
        st.stop()
    
    # Initialize session state
    if "current_post" not in st.session_state:
        st.session_state.current_post = None
    if "post_loaded" not in st.session_state:
        st.session_state.post_loaded = False
    
    # Load next unevaluated post
    if not st.session_state.post_loaded or st.session_state.current_post is None:
        with st.spinner("Loading next unevaluated post..."):
            post = get_unevaluated_post(session)
            if post:
                st.session_state.current_post = post
                st.session_state.post_loaded = True
            else:
                st.success("✅ All posts have been evaluated!")
                st.info("No unevaluated posts found. Great work!")
                st.stop()
    
    post = st.session_state.current_post
    
    # Display post information
    st.header("📝 Post to Evaluate")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Original Post Text")
        st.text_area(
            "Post Content",
            value=post.get("text_for_llm", ""),
            height=200,
            disabled=True,
            key="post_text_display"
        )
    
    with col2:
        st.subheader("Post Metadata")
        st.metric("Post ID", post["post_id"])
        st.metric("Model", post["model_name"])
        st.metric("Model Version", post["model_version"])
        if post.get("annotated_at"):
            st.caption(f"Annotated: {post['annotated_at']}")
    
    st.markdown("---")
    
    # Display LLM Output
    st.header("🤖 LLM Annotation Output")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Classification**")
        st.write(f"**Primary Group:** {post['primary_group'] or 'N/A'}")
        st.write(f"**Primary Topic:** {post['primary_topic'] or 'N/A'}")
        st.write(f"**Trimester:** {post['trimester'] or 'N/A'}")
        st.write(f"**Sentiment:** {post['sentiment'] or 'N/A'}")
        st.write(f"**Urgency:** {post['urgency_0_3']}")
    
    with col2:
        st.markdown("**Keywords & Safety**")
        st.write("**Keywords:**")
        st.write(format_array_display(post['keywords']))
        st.write("**Safety Flags:**")
        st.write(format_array_display(post['safety_flags']))
    
    with col3:
        st.markdown("**Summary**")
        st.text_area(
            "Post Summary",
            value=post['post_summary'] or "",
            height=150,
            disabled=True,
            key="summary_display"
        )
    
    st.markdown("---")
    
    st.subheader("💬 LLM Clinician Response")
    st.text_area(
        "Care Response",
        value=post['care_response'] or "",
        height=200,
        disabled=True,
        key="care_response_display"
    )
    
    st.markdown("---")
    
    # Evaluation Form
    st.header("✏️ Human Evaluation Form")
    
    with st.form("evaluation_form", clear_on_submit=False):
        st.markdown("**Provide corrections and evaluation:**")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            corrected_primary_group = st.selectbox(
                "Correction: Primary Group",
                options=[""] + PRIMARY_GROUPS,
                index=0 if not post['primary_group'] else (
                    PRIMARY_GROUPS.index(post['primary_group']) + 1
                    if post['primary_group'] in PRIMARY_GROUPS else 0
                ),
                key="corrected_group"
            )
        
        with col2:
            # Update topics based on selected group
            selected_group = corrected_primary_group or post.get('primary_group', '')
            available_topics = TOPICS_BY_GROUP.get(selected_group, [])
            
            # Calculate default index for topic
            topic_idx = 0
            if post['primary_topic'] and post['primary_topic'] in available_topics:
                topic_idx = available_topics.index(post['primary_topic']) + 1

            corrected_primary_topic = st.selectbox(
                "Correction: Primary Topic",
                options=[""] + available_topics,
                index=topic_idx,
                key="corrected_topic"
            )
        
        with col3:
            corrected_trimester = st.selectbox(
                "Correction: Trimester",
                options=[""] + TRIMESTER_OPTIONS,
                index=0 if not post['trimester'] else (
                    TRIMESTER_OPTIONS.index(post['trimester']) + 1
                    if post['trimester'] in TRIMESTER_OPTIONS else 0
                ),
                key="corrected_trimester"
            )
        
        # Keywords correction
        current_keywords_str = format_array_display(post['keywords'])
        corrected_keywords = st.text_area(
            "Corrected Keywords (comma-separated)",
            value=current_keywords_str,
            height=100,
            help="Enter keywords separated by commas",
            key="corrected_keywords"
        )
        
        # Accuracy score
        accuracy_score = st.radio(
            "Accuracy Score (1-5)",
            options=ACCURACY_SCORES,
            index=2,  # Default to 3
            horizontal=True,
            help="1 = Very inaccurate, 5 = Very accurate",
            key="accuracy_score"
        )
        
        # Evaluation comments
        evaluation_comments = st.text_area(
            "Evaluation Comments",
            value="",
            height=150,
            help="Provide detailed feedback on the LLM annotation",
            key="evaluation_comments"
        )
        
        # Submit button
        submitted = st.form_submit_button(
            "💾 Save Evaluation & Load Next Post",
            use_container_width=True,
            type="primary"
        )
        
        if submitted:
            # Validation
            if not corrected_primary_group:
                st.error("Please select a corrected primary group.")
            elif not corrected_primary_topic:
                st.error("Please select a corrected primary topic.")
            elif not corrected_trimester:
                st.error("Please select a corrected trimester.")
            else:
                # Save evaluation
                with st.spinner("Saving evaluation..."):
                    success = save_evaluation(
                        session,
                        post["post_id"],
                        corrected_primary_group,
                        corrected_primary_topic,
                        corrected_trimester,
                        corrected_keywords,
                        accuracy_score,
                        evaluation_comments
                    )
                
                if success:
                    st.success("✅ Evaluation saved successfully!")
                    # Clear session state to load next post
                    st.session_state.current_post = None
                    st.session_state.post_loaded = False
                    st.rerun()
                else:
                    st.error("❌ Failed to save evaluation. Please try again.")
    
    # Footer
    st.markdown("---")
    st.caption(f"Database: {DATABASE_NAME} | Schema: {LLM_EVAL_SCHEMA}")

if __name__ == "__main__":
    main()

