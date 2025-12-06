#!/usr/bin/env python3
"""
Custom LLM Interaction Dashboard for Streamlit in Snowflake (SiS)

This dashboard allows users to:
1. Type in any text input
2. Configure a custom prompt for LLM processing
3. Get LLM responses
4. Review and provide feedback on the responses
5. Save all interactions (input, prompt, output, review) to Snowflake

Usage:
    Deploy to Streamlit in Snowflake (SiS) environment
    Or run locally: streamlit run src/dashboard/custom_llm_dashboard.py
"""

import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from typing import Optional, Dict, Any, List
import os
import uuid
import hashlib
import re
import json
from pathlib import Path
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

# Load environment variables
load_dotenv()

# ============================================================================
# CONFIGURATION - Update these for your environment
# ============================================================================

# Configuration - uses environment variables with fallbacks
DATABASE_NAME = os.getenv("SNOWFLAKE_DATABASE", "MATERNOSCOPE")
SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS_ML")
# Table to store interactions
INTERACTIONS_TABLE = "CUSTOM_LLM_INTERACTIONS"

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
PROMPTS_DIR = PROJECT_ROOT / "prompts"

# Default prompt template (simple fallback)
DEFAULT_SYSTEM_PROMPT = (
    "You are a helpful assistant. Please analyze the following "
    "input and provide a thoughtful response."
)
DEFAULT_USER_PROMPT = (
    "Input:\n{user_input}\n\n"
    "Please provide your analysis and response:"
)

# ============================================================================
# SNOWFLAKE CONNECTION
# ============================================================================


@st.cache_resource
def get_snowflake_session() -> Session:
    """
    Get Snowflake session.
    Tries Streamlit in Snowflake (SiS) first, falls back to local connection.
    """
    # Try to get active session (for Streamlit in Snowflake)
    try:
        session = get_active_session()
        return session
    except Exception:
        # Fall back to local connection using environment variables
        try:
            connection_parameters = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USERNAME"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS_ML"),
                "role": os.getenv("SNOWFLAKE_ROLE"),
            }

            # Validate required parameters
            required_params = ["account", "user", "password"]
            missing_params = [
                p for p in required_params
                if not connection_parameters.get(p)
            ]
            if missing_params:
                error_msg = (
                    f"Missing required Snowflake environment variables: "
                    f"{', '.join(missing_params)}. "
                    "Please set them in your .env file."
                )
                st.error(error_msg)
                st.stop()

            # Create session from connection parameters
            session = Session.builder.configs(connection_parameters).create()
            return session
        except Exception as e:
            error_msg = (
                f"Error connecting to Snowflake: {e}\n\n"
                "For local development, ensure your .env file contains:\n"
                "- SNOWFLAKE_ACCOUNT\n"
                "- SNOWFLAKE_USERNAME\n"
                "- SNOWFLAKE_PASSWORD\n"
                "- SNOWFLAKE_WAREHOUSE\n"
                "- SNOWFLAKE_DATABASE\n"
                "- SNOWFLAKE_SCHEMA (optional, defaults to ANALYTICS_ML)\n"
                "- SNOWFLAKE_ROLE"
            )
            st.error(error_msg)
            st.stop()


# ============================================================================
# PROMPT MANAGEMENT FUNCTIONS
# ============================================================================


def calculate_prompt_hash(system_prompt: str, user_prompt: str) -> str:
    """Calculate prompt hash from system and user prompts."""
    combined = system_prompt + "\n\n" + user_prompt
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


@st.cache_data
def scan_prompts_folder() -> List[Dict[str, Any]]:
    """
    Scan prompts folder and return available prompt pairs.
    Returns list of dicts with system_file, user_file, and calculated hash.
    """
    prompts = []
    
    if not PROMPTS_DIR.exists():
        return prompts
    
    # Find system prompt files
    system_files = sorted(PROMPTS_DIR.glob("clinical_annotation_system_*.txt"))
    
    # Find user prompt files
    user_files = sorted(PROMPTS_DIR.glob("clinical_annotation_user_*.txt"))
    
    # Try to match system and user prompts
    for system_file in system_files:
        for user_file in user_files:
            try:
                system_text = system_file.read_text(encoding='utf-8').strip()
                user_text = user_file.read_text(encoding='utf-8').strip()
                prompt_hash = calculate_prompt_hash(system_text, user_text)
                
                prompts.append({
                    'system_file': system_file.name,
                    'user_file': user_file.name,
                    'system_prompt': system_text,
                    'user_prompt': user_text,
                    'prompt_hash': prompt_hash,
                    'display_name': (
                        f"{system_file.stem} + {user_file.stem} "
                        f"(hash: {prompt_hash})"
                    )
                })
            except Exception as e:
                st.warning(f"Error reading {system_file.name} or {user_file.name}: {e}")
                continue
    
    return prompts


def get_prompt_by_hash(prompt_hash: str) -> Optional[Dict[str, Any]]:
    """Get prompt pair by hash from prompts folder."""
    prompts = scan_prompts_folder()
    for prompt in prompts:
        if prompt['prompt_hash'] == prompt_hash:
            return prompt
    return None


def get_available_prompt_hashes_from_snowflake(
    session: Session
) -> List[str]:
    """Get list of prompt hashes from existing interactions."""
    try:
        table_name = (
            f"{DATABASE_NAME}.{SCHEMA_NAME}.{INTERACTIONS_TABLE}"
        )
        query = f"""
        SELECT DISTINCT llm_prompt_hash
        FROM {table_name}
        WHERE llm_prompt_hash IS NOT NULL
        ORDER BY llm_prompt_hash
        """
        df = session.sql(query).collect()
        return [
            row["LLM_PROMPT_HASH"] for row in df
            if row["LLM_PROMPT_HASH"]
        ]
    except Exception:
        return []


# ============================================================================
# LLM FUNCTIONS
# ============================================================================


@st.cache_resource
def get_llm_client():
    """Initialize and cache LLM client."""
    try:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            st.error("OPENAI_API_KEY not found in environment variables")
            st.stop()
        
        model_name = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        
        # Get temperature from env var, or use None (default) if not set
        # Some models only support default temperature (1)
        temp_str = os.getenv("OPENAI_TEMPERATURE", None)
        temperature = float(temp_str) if temp_str else None
        
        # Build LLM config
        llm_kwargs = {
            "model": model_name,
            "api_key": api_key,
        }
        
        if os.getenv("OPENAI_ORG_ID"):
            llm_kwargs["organization"] = os.getenv("OPENAI_ORG_ID")
        
        # Only set temperature if explicitly provided
        # Some newer models only support default temperature
        if temperature is not None:
            llm_kwargs["temperature"] = temperature
        
        llm = ChatOpenAI(**llm_kwargs)
        return llm
    except Exception as e:
        st.error(f"Error initializing LLM client: {e}")
        st.stop()


def call_llm(
    user_input: str,
    system_prompt: str,
    user_prompt_template: str,
    prompt_hash: str,
    request_json: bool = False
) -> Dict[str, Any]:
    """
    Call LLM with system and user prompts.
    
    Args:
        user_input: The user's input text
        system_prompt: System prompt (no placeholders)
        user_prompt_template: User prompt template (may contain {user_input},
                              {post_text}, {post_id}, etc.)
        prompt_hash: Hash of the combined prompts
        request_json: If True, request JSON format response
    
    Returns:
        Dictionary with 'response', 'input_tokens', 'output_tokens',
        'model_name', 'prompt_hash'
    """
    try:
        # Check if we should request JSON format
        # (if system prompt mentions JSON or if explicitly requested)
        should_request_json = (
            request_json or
            "json" in system_prompt.lower() or
            "json" in user_prompt_template.lower()
        )
        
        # Get or create LLM client with JSON format if needed
        if should_request_json:
            # Create a new client with JSON format for this call
            api_key = os.getenv("OPENAI_API_KEY")
            model_name = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
            temp_str = os.getenv("OPENAI_TEMPERATURE", None)
            temperature = float(temp_str) if temp_str else None
            
            llm_kwargs = {
                "model": model_name,
                "api_key": api_key,
                "model_kwargs": {"response_format": {"type": "json_object"}}
            }
            
            if os.getenv("OPENAI_ORG_ID"):
                llm_kwargs["organization"] = os.getenv("OPENAI_ORG_ID")
            
            if temperature is not None:
                llm_kwargs["temperature"] = temperature
            
            llm = ChatOpenAI(**llm_kwargs)
        else:
            llm = get_llm_client()
        
        # Replace common placeholders with {user_input} for formatting
        # This allows prompts with {post_text} or {post_id} to work
        formatted_user_prompt = user_prompt_template.replace(
            "{post_text}", "{user_input}"
        ).replace("{post_id}", "{user_input}")
        
        # Create prompt template with system and user messages
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("user", formatted_user_prompt)
        ])
        
        # Format the prompt with user input
        formatted_messages = prompt_template.format_messages(
            user_input=user_input
        )
        
        # Invoke LLM
        llm_response = llm.invoke(formatted_messages)
        
        # Extract token usage
        input_tokens = 0
        output_tokens = 0
        model_name = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        
        if hasattr(llm_response, 'response_metadata'):
            usage = llm_response.response_metadata.get('token_usage', {})
            input_tokens = usage.get('prompt_tokens', 0)
            output_tokens = usage.get('completion_tokens', 0)
        
        return {
            'response': llm_response.content,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'model_name': model_name,
            'prompt_hash': prompt_hash
        }
    except Exception as e:
        st.error(f"Error calling LLM: {e}")
        return None


# ============================================================================
# SNOWFLAKE TABLE FUNCTIONS
# ============================================================================


def ensure_interactions_table_exists(session: Session) -> bool:
    """
    Ensure the CUSTOM_LLM_INTERACTIONS table exists.
    Creates it if it doesn't exist.
    Uses llm_system_prompt and llm_user_prompt to avoid reserved keywords.
    """
    try:
        table_name = (
            f"{DATABASE_NAME}.{SCHEMA_NAME}.{INTERACTIONS_TABLE}"
        )
        # Use llm_ prefix for columns to avoid reserved keywords (SYSTEM, USER, PROMPT, INPUT, OUTPUT, MODEL)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            interaction_id VARCHAR(255) PRIMARY KEY,
            llm_user_input VARCHAR(16777216),
            llm_system_prompt VARCHAR(16777216),
            llm_user_prompt VARCHAR(16777216),
            llm_prompt_hash VARCHAR(50),
            llm_output VARCHAR(16777216),
            review_feedback VARCHAR(5000),
            review_rating INTEGER,
            rating_overall INTEGER,
            rating_accuracy INTEGER,
            rating_empathy INTEGER,
            rating_safety INTEGER,
            rating_relevance INTEGER,
            llm_model_name VARCHAR(100),
            llm_input_tokens INTEGER,
            llm_output_tokens INTEGER,
            created_at TIMESTAMP_TZ,
            reviewed_at TIMESTAMP_TZ,
            reviewer_name VARCHAR(255)
        )
        """

        session.sql(create_table_query).collect()
        
        # Check if required columns exist, add them if missing
        # (handles case where table was created with old schema)
        try:
            # List of columns that need to be checked/added
            columns_to_check = [
                ('LLM_USER_INPUT', 'llm_user_input', 'VARCHAR(16777216)'),
                ('LLM_SYSTEM_PROMPT', 'llm_system_prompt', 'VARCHAR(16777216)'),
                ('LLM_USER_PROMPT', 'llm_user_prompt', 'VARCHAR(16777216)'),
                ('LLM_PROMPT_HASH', 'llm_prompt_hash', 'VARCHAR(50)'),
                ('LLM_MODEL_NAME', 'llm_model_name', 'VARCHAR(100)'),
                ('LLM_INPUT_TOKENS', 'llm_input_tokens', 'INTEGER'),
                ('LLM_OUTPUT_TOKENS', 'llm_output_tokens', 'INTEGER'),
                ('RATING_OVERALL', 'rating_overall', 'INTEGER'),
                ('RATING_ACCURACY', 'rating_accuracy', 'INTEGER'),
                ('RATING_EMPATHY', 'rating_empathy', 'INTEGER'),
                ('RATING_SAFETY', 'rating_safety', 'INTEGER'),
                ('RATING_RELEVANCE', 'rating_relevance', 'INTEGER'),
            ]
            
            for upper_col_name, lower_col_name, col_type in columns_to_check:
                check_col = f"""
                SELECT COUNT(*) as col_exists
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = UPPER('{SCHEMA_NAME}')
                AND TABLE_NAME = UPPER('{INTERACTIONS_TABLE}')
                AND TABLE_CATALOG = UPPER('{DATABASE_NAME}')
                AND UPPER(COLUMN_NAME) = '{upper_col_name}'
                """
                col_result = session.sql(check_col).collect()
                col_exists = (
                    col_result[0]["COL_EXISTS"] > 0
                    if col_result else False
                )
                
                if not col_exists:
                    alter_query = f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN {lower_col_name} {col_type}
                    """
                    session.sql(alter_query).collect()
        except Exception as migration_error:
            # Migration check failed, but table creation succeeded
            # Log warning but continue
            st.warning(
                f"Could not verify/add required columns: "
                f"{migration_error}. Table may need manual migration."
            )
        
        return True

    except Exception as e:
        st.error(f"Error creating interactions table: {e}")
        return False


def save_interaction(
    session: Session,
    interaction_id: str,
    user_input: str,
    system_prompt: str,
    user_prompt: str,
    prompt_hash: str,
    llm_output: str,
    model_name: str,
    input_tokens: int,
    output_tokens: int,
    review_feedback: Optional[str] = None,
    review_rating: Optional[int] = None,
    reviewer_name: Optional[str] = None
) -> bool:
    """
    Save interaction to CUSTOM_LLM_INTERACTIONS table.
    
    Returns True if successful, False otherwise.
    """
    try:
        # Escape single quotes for SQL
        def escape_sql(text):
            if text is None:
                return "NULL"
            return f"'{text.replace("'", "''")}'"
        
        # Prepare values
        user_input_escaped = escape_sql(user_input)
        system_prompt_escaped = escape_sql(system_prompt)
        user_prompt_escaped = escape_sql(user_prompt)
        llm_output_escaped = escape_sql(llm_output)
        review_feedback_escaped = (
            escape_sql(review_feedback) if review_feedback else "NULL"
        )
        review_rating_val = (
            review_rating if review_rating is not None else "NULL"
        )
        reviewer_name_escaped = (
            escape_sql(reviewer_name) if reviewer_name else "NULL"
        )
        reviewed_at = (
            "CURRENT_TIMESTAMP()" if review_feedback else "NULL"
        )

        # Insert interaction
        table_name = (
            f"{DATABASE_NAME}.{SCHEMA_NAME}.{INTERACTIONS_TABLE}"
        )
        insert_query = f"""
        INSERT INTO {table_name} (
            interaction_id,
            llm_user_input,
            llm_system_prompt,
            llm_user_prompt,
            llm_prompt_hash,
            llm_output,
            review_feedback,
            review_rating,
            llm_model_name,
            llm_input_tokens,
            llm_output_tokens,
            created_at,
            reviewed_at,
            reviewer_name
        ) VALUES (
            '{interaction_id.replace("'", "''")}',
            {user_input_escaped},
            {system_prompt_escaped},
            {user_prompt_escaped},
            '{prompt_hash.replace("'", "''")}',
            {llm_output_escaped},
            {review_feedback_escaped},
            {review_rating_val},
            '{model_name.replace("'", "''")}',
            {input_tokens},
            {output_tokens},
            CURRENT_TIMESTAMP(),
            {reviewed_at},
            {reviewer_name_escaped}
        )
        """
        
        session.sql(insert_query).collect()
        return True

    except Exception as e:
        st.error(f"Error saving interaction: {e}")
        return False


def update_interaction_review(
    session: Session,
    interaction_id: str,
    review_feedback: str,
    rating_overall: int,
    rating_accuracy: Optional[int] = None,
    rating_empathy: Optional[int] = None,
    rating_safety: Optional[int] = None,
    rating_relevance: Optional[int] = None,
    reviewer_name: Optional[str] = None
) -> bool:
    """
    Update an existing interaction with review feedback.
    
    Returns True if successful, False otherwise.
    """
    try:
        # Escape single quotes for SQL
        def escape_sql(text):
            if text is None:
                return "NULL"
            return f"'{text.replace("'", "''")}'"
        
        review_feedback_escaped = escape_sql(review_feedback)
        reviewer_name_escaped = (
            escape_sql(reviewer_name) if reviewer_name else "NULL"
        )
        
        # Format rating values
        rating_overall_val = str(rating_overall)
        rating_accuracy_val = (
            str(rating_accuracy) if rating_accuracy is not None else "NULL"
        )
        rating_empathy_val = (
            str(rating_empathy) if rating_empathy is not None else "NULL"
        )
        rating_safety_val = (
            str(rating_safety) if rating_safety is not None else "NULL"
        )
        rating_relevance_val = (
            str(rating_relevance) if rating_relevance is not None else "NULL"
        )

        table_name = (
            f"{DATABASE_NAME}.{SCHEMA_NAME}.{INTERACTIONS_TABLE}"
        )
        update_query = f"""
        UPDATE {table_name}
        SET 
            review_feedback = {review_feedback_escaped},
            rating_overall = {rating_overall_val},
            rating_accuracy = {rating_accuracy_val},
            rating_empathy = {rating_empathy_val},
            rating_safety = {rating_safety_val},
            rating_relevance = {rating_relevance_val},
            reviewer_name = {reviewer_name_escaped},
            reviewed_at = CURRENT_TIMESTAMP()
        WHERE interaction_id = '{interaction_id.replace("'", "''")}'
        """
        
        session.sql(update_query).collect()
        return True

    except Exception as e:
        st.error(f"Error updating interaction review: {e}")
        return False


# ============================================================================
# JSON PARSING AND DISPLAY FUNCTIONS
# ============================================================================


def parse_llm_response(response_text: str) -> Dict[str, Any]:
    """
    Parse LLM response text, attempting to extract JSON.
    Returns dict with parsed data or raw response if not JSON.
    """
    if not response_text:
        return {"raw_response": "", "is_json": False}
    
    # Try to parse as JSON
    try:
        # Remove markdown code blocks if present
        cleaned = response_text.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        elif cleaned.startswith("```"):
            cleaned = cleaned[3:]
        
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        
        cleaned = cleaned.strip()
        
        # Try to parse JSON
        parsed = json.loads(cleaned)
        return {
            "raw_response": response_text,
            "is_json": True,
            "parsed": parsed
        }
    except (json.JSONDecodeError, ValueError):
        # Not JSON, return as raw text
        return {
            "raw_response": response_text,
            "is_json": False
        }


def format_array_display(arr) -> str:
    """Format array for display."""
    if not arr:
        return "None"
    if isinstance(arr, list):
        return ", ".join(str(item) for item in arr)
    return str(arr)


def display_parsed_json_response(
    parsed_data: Dict[str, Any],
    user_input: str,
    response_data: Dict[str, Any],
    session: Session,
    interaction_id: str
):
    """
    Display parsed JSON response in structured format,
    similar to clinician_review_dashboard.py with two-column layout
    """
    parsed = parsed_data.get("parsed", {})
    
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
    
    # Left Column: User Input & LLM Annotation Summary
    with left_col:
        st.markdown('<div class="scrollable-container">', unsafe_allow_html=True)
        
        st.subheader("📝 Your Input")
        st.text_area(
            "",
            value=user_input,
            height=300,
            disabled=True,
            key="display_user_input"
        )
        
        st.markdown("---")
        
        # LLM Annotation Summary
        st.subheader("🤖 LLM Annotation Summary")
        
        summary_col1, summary_col2 = st.columns(2)
        with summary_col1:
            primary_group = parsed.get("primary_group", "N/A")
            st.markdown(f"**Primary Group:** {primary_group}")
            
            primary_topic = parsed.get("primary_topic", "N/A")
            st.markdown(f"**Primary Topic:** {primary_topic}")
            
            trimester = parsed.get("trimester", "N/A")
            st.markdown(f"**Trimester:** {trimester}")
        
        with summary_col2:
            sentiment = parsed.get("sentiment", "N/A")
            st.markdown(f"**Sentiment:** {sentiment}")
            
            urgency = parsed.get("urgency_0_3", "N/A")
            st.markdown(f"**Urgency (0-3):** {urgency}")
            
            confidence = parsed.get("model_confidence_score", "N/A")
            if confidence != "N/A":
                st.markdown(f"**Confidence:** {confidence}/100")
        
        # Secondary topics
        secondary_topics = parsed.get("secondary_topics", [])
        if secondary_topics:
            st.markdown(f"**Secondary Topics:** {format_array_display(secondary_topics)}")
        
        # Post Summary
        post_summary = parsed.get("post_summary", "")
        if post_summary:
            st.markdown("---")
            st.markdown("**Post Summary:**")
            st.info(post_summary)
        
        # Safety Flags
        safety_flags = parsed.get("safety_flags", [])
        if safety_flags:
            st.markdown("---")
            st.markdown("**Safety Flags:**")
            st.warning(format_array_display(safety_flags))
        
        # Reasoning fields (if present)
        classification_reasoning = parsed.get("classification_reasoning", "")
        if classification_reasoning:
            st.markdown("---")
            st.markdown("### 🤔 Classification Reasoning")
            with st.expander("View Reasoning", expanded=False):
                st.text(classification_reasoning)
        
        safety_justification = parsed.get("safety_assessment_justification", "")
        if safety_justification:
            st.markdown("### 🛡️ Safety Assessment Justification")
            with st.expander("View Justification", expanded=False):
                st.text(safety_justification)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Right Column: Care Response & Review
    with right_col:
        st.markdown('<div class="scrollable-container">', unsafe_allow_html=True)
        
        # Keywords above Care Response (if present)
        keywords = parsed.get("keywords", [])
        if keywords:
            st.markdown("**Keywords:**")
            st.code(format_array_display(keywords))
            st.markdown("---")
        
        # LLM-Generated Care Response
        st.subheader("💬 LLM-Generated Care Response")
        care_response = parsed.get("care_response", "")
        if care_response:
            st.text_area(
                "",
                value=care_response,
                height=250,
                disabled=True,
                key="care_response_display"
            )
        else:
            st.warning("No care response in JSON")
        
        # Safety Flags below Care Response
        safety_flags = parsed.get("safety_flags", [])
        if safety_flags:
            st.markdown("**Safety Flags:**")
            st.warning(format_array_display(safety_flags))
        
        st.markdown("---")
        
        # Review Section
        st.subheader("⭐ Review This LLM Output")
        
        # Reviewer information
        reviewer_name = st.text_input(
            "Your Name (optional):",
            key="reviewer_name"
        )
        
        st.divider()
        
        # Ratings
        st.markdown("### Rating Scale (1 = Poor, 5 = Excellent)")
        
        rating_overall = st.slider(
            "Overall Quality *",
            min_value=1,
            max_value=5,
            value=3,
            key="rating_overall_json"
        )
        
        rating_col1, rating_col2 = st.columns(2)
        with rating_col1:
            rating_accuracy = st.slider(
                "Accuracy (correctness of medical information)",
                min_value=1, max_value=5, value=3,
                key="rating_accuracy_json"
            )

            rating_empathy = st.slider(
                "Empathy (tone and emotional support)",
                min_value=1, max_value=5, value=3,
                key="rating_empathy_json"
            )

        with rating_col2:
            rating_safety = st.slider(
                "Safety (appropriate escalation and warnings)",
                min_value=1, max_value=5, value=3,
                key="rating_safety_json"
            )

            rating_relevance = st.slider(
                "Relevance (addresses the post's concerns)",
                min_value=1, max_value=5, value=3,
                key="rating_relevance_json"
            )
        
        st.divider()
        
        # Review feedback
        review_feedback = st.text_area(
            "Additional Comments (optional)",
            height=150,
            placeholder=(
                "Provide specific feedback on what worked "
                "well or what could be improved..."
            ),
            key="review_feedback_json"
        )
        
        # Submit review button
        if st.button("💾 Save Review", type="primary", use_container_width=True, key="save_review_json"):
            with st.spinner("Saving review..."):
                success = update_interaction_review(
                    session,
                    interaction_id,
                    review_feedback if review_feedback.strip() else "",
                    rating_overall,
                    rating_accuracy,
                    rating_empathy,
                    rating_safety,
                    rating_relevance,
                    reviewer_name if reviewer_name.strip() else None
                )
                
                if success:
                    st.success("✅ Review saved successfully!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("❌ Failed to save review. Please try again.")
        
        st.markdown("---")
        
        # Metadata
        st.markdown("### 📊 Metadata")
        meta_col1, meta_col2 = st.columns(2)
        with meta_col1:
            model_name = response_data.get("model_name", "N/A")
            st.caption(f"**Model:** {model_name}")
            
            input_tokens = response_data.get("input_tokens", 0)
            st.caption(f"**Input Tokens:** {input_tokens}")
        
        with meta_col2:
            output_tokens = response_data.get("output_tokens", 0)
            st.caption(f"**Output Tokens:** {output_tokens}")
            
            prompt_hash = response_data.get("prompt_hash", "N/A")
            if prompt_hash != "N/A":
                st.caption(f"**Prompt Hash:** {prompt_hash[:8]}...")
        
        st.markdown('</div>', unsafe_allow_html=True)


# ============================================================================
# STREAMLIT UI
# ============================================================================


def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="Custom LLM Interaction Dashboard",
        page_icon="💬",
        layout="wide"
    )
    
    st.title("💬 Custom LLM Interaction Dashboard")
    st.markdown("---")

    # Get Snowflake session
    session = get_snowflake_session()

    # Ensure interactions table exists
    if not ensure_interactions_table_exists(session):
        error_msg = (
            "Failed to initialize interactions table. "
            "Please check permissions."
        )
        st.error(error_msg)
        st.stop()
    
    # Initialize session state
    if "interaction_id" not in st.session_state:
        st.session_state.interaction_id = None
    if "llm_response" not in st.session_state:
        st.session_state.llm_response = None
    if "interaction_saved" not in st.session_state:
        st.session_state.interaction_saved = False
    if "system_prompt" not in st.session_state:
        st.session_state.system_prompt = DEFAULT_SYSTEM_PROMPT
    if "user_prompt" not in st.session_state:
        st.session_state.user_prompt = DEFAULT_USER_PROMPT
    if "prompt_hash" not in st.session_state:
        st.session_state.prompt_hash = calculate_prompt_hash(
            DEFAULT_SYSTEM_PROMPT, DEFAULT_USER_PROMPT
        )
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        st.markdown(f"**Database:** {DATABASE_NAME}")
        st.markdown(f"**Schema:** {SCHEMA_NAME}")
        st.markdown(f"**Table:** {INTERACTIONS_TABLE}")
        
        st.markdown("---")
        st.markdown("### Model Settings")
        model_name = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        st.info(f"Using model: **{model_name}**")
        
        st.markdown("---")
        if st.button("🔄 Reset Session", key="reset_session", use_container_width=True):
            st.session_state.interaction_id = None
            st.session_state.llm_response = None
            st.session_state.interaction_saved = False
            st.session_state.system_prompt = DEFAULT_SYSTEM_PROMPT
            st.session_state.user_prompt = DEFAULT_USER_PROMPT
            st.session_state.prompt_hash = calculate_prompt_hash(
                DEFAULT_SYSTEM_PROMPT, DEFAULT_USER_PROMPT
            )
            st.rerun()
    
    # Main content area - single page layout
    st.header("Create New LLM Interaction")
    st.markdown("---")
    
    # User input section
    st.subheader("📥 Your Input")
    user_input = st.text_area(
        "Enter your text here:",
        height=200,
        placeholder="Type anything you want the LLM to process...",
        key="user_input"
    )
    
    # Prompt selection and editing section
    st.subheader("🎯 Prompt Configuration")
    
    # Prompt selection mode
    prompt_mode = st.radio(
        "Prompt Selection Mode:",
        ["Select by Hash", "Edit Prompts"],
        horizontal=True,
        key="prompt_mode"
    )
    
    if prompt_mode == "Select by Hash":
        # Get available prompts from folder
        folder_prompts = scan_prompts_folder()
        
        # Get available hashes from Snowflake
        snowflake_hashes = get_available_prompt_hashes_from_snowflake(
            session
        )
        
        # Combine and deduplicate
        all_hashes = set()
        hash_to_prompt = {}
        
        for prompt in folder_prompts:
            hash_val = prompt['prompt_hash']
            all_hashes.add(hash_val)
            hash_to_prompt[hash_val] = prompt
        
        for hash_val in snowflake_hashes:
            all_hashes.add(hash_val)
            # Try to find in folder prompts
            if hash_val not in hash_to_prompt:
                found = get_prompt_by_hash(hash_val)
                if found:
                    hash_to_prompt[hash_val] = found
        
        # Create options list
        hash_options = sorted(list(all_hashes))
        
        if hash_options:
            def format_hash_option(x):
                if x in hash_to_prompt and 'display_name' in hash_to_prompt[x]:
                    name = hash_to_prompt[x]['display_name']
                else:
                    name = 'from Snowflake'
                return f"{x} ({name})"

            selected_hash = st.selectbox(
                "Select Prompt by Hash:",
                options=hash_options,
                format_func=format_hash_option,
                key="selected_prompt_hash"
            )
            
            if st.button("Load Selected Prompt", key="load_prompt"):
                if selected_hash in hash_to_prompt:
                    prompt_data = hash_to_prompt[selected_hash]
                    system_prompt = prompt_data['system_prompt']
                    user_prompt = prompt_data['user_prompt']
                    
                    # Replace {post_text} with {user_input} for compatibility
                    # Also replace {post_id} with a placeholder
                    user_prompt = user_prompt.replace("{post_text}", "{user_input}")
                    user_prompt = user_prompt.replace("{post_id}", "{user_input}")
                    
                    # Recalculate hash with updated prompt
                    new_hash = calculate_prompt_hash(system_prompt, user_prompt)
                    
                    st.session_state.system_prompt = system_prompt
                    st.session_state.user_prompt = user_prompt
                    st.session_state.prompt_hash = new_hash
                    st.success(
                        f"✅ Loaded prompt with hash: {selected_hash[:8]}... "
                        f"(updated to {new_hash[:8]}...)"
                    )
                    st.rerun()
                else:
                    st.warning(
                        f"Prompt hash {selected_hash} found in Snowflake "
                        "but not in prompts folder. Please use 'Edit "
                        "Prompts' mode to recreate it."
                    )
        else:
            st.info("No prompts found. Use 'Edit Prompts' mode to create one.")
    
    else:  # Edit Prompts mode
        st.markdown("**Edit System and User Prompts:**")
        
        system_prompt = st.text_area(
            "System Prompt:",
            value=st.session_state.system_prompt,
            height=200,
            help="System prompt (instructions for the LLM)",
            key="system_prompt_editor"
        )
        
        user_prompt = st.text_area(
            "User Prompt Template:",
            value=st.session_state.user_prompt,
            height=200,
            help=(
                "User prompt template. Use {user_input}, {post_text}, "
                "or {post_id} as placeholders. They will all be "
                "replaced with your input text."
            ),
            key="user_prompt_editor"
        )
        
        # Calculate and display hash
        new_hash = calculate_prompt_hash(system_prompt, user_prompt)
        
        col1, col2 = st.columns([3, 1])
        with col1:
            st.info(f"**Prompt Hash:** `{new_hash}`")
        with col2:
            if st.button("💾 Save Prompts", key="save_prompts"):
                st.session_state.system_prompt = system_prompt
                st.session_state.user_prompt = user_prompt
                st.session_state.prompt_hash = new_hash
                st.success(f"✅ Prompts saved! Hash: {new_hash}")
                st.rerun()
    
    # Display current prompts
    st.markdown("---")
    with st.expander("📋 View Current Prompts", expanded=False):
        st.markdown("**System Prompt:**")
        # Use dynamic key based on hash to force refresh when prompts change
        system_key = f"display_system_{st.session_state.prompt_hash[:8]}"
        st.text_area(
            "System",
            value=st.session_state.system_prompt,
            height=150,
            disabled=True,
            key=system_key
        )
        st.markdown("**User Prompt:**")
        # Use dynamic key based on hash to force refresh when prompts change
        user_key = f"display_user_{st.session_state.prompt_hash[:8]}"
        st.text_area(
            "User",
            value=st.session_state.user_prompt,
            height=150,
            disabled=True,
            key=user_key
        )
        st.caption(f"**Current Hash:** `{st.session_state.prompt_hash}`")
    
    # Validate user prompt has placeholder
    has_placeholder = bool(
        re.search(r'\{[^}]+\}', st.session_state.user_prompt)
    )
    if not has_placeholder:
        warning_msg = (
            "⚠️ Your user prompt template should include a placeholder "
            "like `{user_input}`, `{post_text}`, etc."
        )
        st.warning(warning_msg)

    # Generate LLM response
    col1, col2 = st.columns([1, 4])
    with col1:
        generate_button = st.button(
            "🚀 Generate Response",
            type="primary",
            use_container_width=True
        )
    
    if generate_button:
        if not user_input.strip():
            st.error("Please enter some text input first.")
        else:
            # Check for any placeholder pattern
            has_placeholder = bool(
                re.search(r'\{[^}]+\}', st.session_state.user_prompt)
            )
            if not has_placeholder:
                error_msg = (
                    "Please include a placeholder (e.g., `{user_input}`, "
                    "`{post_text}`) in your user prompt template."
                )
                st.error(error_msg)
            else:
                # Detect if JSON format should be requested
                # (check if prompts mention JSON or JSON schema)
                system_prompt_lower = st.session_state.system_prompt.lower()
                user_prompt_lower = st.session_state.user_prompt.lower()
                request_json = (
                    "json" in system_prompt_lower or
                    "json" in user_prompt_lower or
                    "json schema" in system_prompt_lower or
                    "json schema" in user_prompt_lower
                )
                
                with st.spinner("Calling LLM..."):
                    result = call_llm(
                        user_input,
                        st.session_state.system_prompt,
                        st.session_state.user_prompt,
                        st.session_state.prompt_hash,
                        request_json=request_json
                    )

                    if result:
                        st.session_state.llm_response = result
                        st.session_state.interaction_id = str(uuid.uuid4())
                        st.session_state.interaction_saved = False
                        st.success("✅ LLM response generated!")
                        st.rerun()
                    else:
                        st.error(
                            "Failed to generate response. "
                            "Please try again."
                        )

    # Display LLM response if available
    if st.session_state.llm_response:
            st.markdown("---")
            st.header("🤖 LLM Response & Review")
            
            response_data = st.session_state.llm_response
            response_text = response_data['response']
            
            # Parse the response
            parsed_response = parse_llm_response(response_text)
            
            # Auto-save interaction (without review) if not saved yet
            if not st.session_state.interaction_saved:
                with st.spinner("Saving interaction..."):
                    success = save_interaction(
                        session,
                        st.session_state.interaction_id,
                        user_input,
                        st.session_state.system_prompt,
                        st.session_state.user_prompt,
                        st.session_state.prompt_hash,
                        response_data['response'],
                        response_data['model_name'],
                        response_data['input_tokens'],
                        response_data['output_tokens']
                    )

                    if success:
                        st.session_state.interaction_saved = True
                        st.success("✅ Interaction saved to Snowflake!")
                    else:
                        st.error("❌ Failed to save interaction.")
            
            # Display mode toggle (only show if JSON detected)
            if parsed_response.get("is_json"):
                display_mode = st.radio(
                    "Display Mode:",
                    ["Structured View", "Raw Response"],
                    horizontal=True,
                    key="display_mode"
                )
            else:
                display_mode = "Raw Response"
            
            if display_mode == "Structured View" and parsed_response.get("is_json"):
                # Display parsed JSON in structured format with review
                display_parsed_json_response(
                    parsed_response,
                    user_input,
                    response_data,
                    session,
                    st.session_state.interaction_id
                )
            else:
                # Display raw response in two-column layout
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
                
                # Two-column layout for raw response
                left_col, right_col = st.columns([1, 1], gap="large")
                
                with left_col:
                    st.markdown('<div class="scrollable-container">', unsafe_allow_html=True)
                    st.subheader("📝 Your Input")
                    st.text_area(
                        "",
                        value=user_input,
                        height=400,
                        disabled=True,
                        key="raw_input_display"
                    )
                    st.markdown('</div>', unsafe_allow_html=True)
                
                with right_col:
                    st.markdown('<div class="scrollable-container">', unsafe_allow_html=True)
                    st.subheader("🤖 LLM Response")
                    st.text_area(
                        "",
                        value=response_text,
                        height=300,
                        disabled=True,
                        key="raw_response_display"
                    )
                    
                    st.markdown("---")
                    st.subheader("⭐ Review This LLM Output")
                    
                    reviewer_name = st.text_input(
                        "Your Name (optional):",
                        key="reviewer_name_raw"
                    )
                    
                    st.divider()
                    
                    # Ratings
                    st.markdown("### Rating Scale (1 = Poor, 5 = Excellent)")
                    
                    rating_overall = st.slider(
                        "Overall Quality *",
                        min_value=1,
                        max_value=5,
                        value=3,
                        key="rating_overall_raw"
                    )
                    
                    rating_col1, rating_col2 = st.columns(2)
                    with rating_col1:
                        rating_accuracy = st.slider(
                            "Accuracy (correctness of medical information)",
                            min_value=1, max_value=5, value=3,
                            key="rating_accuracy_raw"
                        )

                        rating_empathy = st.slider(
                            "Empathy (tone and emotional support)",
                            min_value=1, max_value=5, value=3,
                            key="rating_empathy_raw"
                        )

                    with rating_col2:
                        rating_safety = st.slider(
                            "Safety (appropriate escalation and warnings)",
                            min_value=1, max_value=5, value=3,
                            key="rating_safety_raw"
                        )

                        rating_relevance = st.slider(
                            "Relevance (addresses the post's concerns)",
                            min_value=1, max_value=5, value=3,
                            key="rating_relevance_raw"
                        )
                    
                    st.divider()
                    
                    review_feedback = st.text_area(
                        "Additional Comments (optional)",
                        height=150,
                        placeholder=(
                            "Provide specific feedback on what worked "
                            "well or what could be improved..."
                        ),
                        key="review_feedback_raw"
                    )
                    
                    if st.button("💾 Save Review", type="primary", use_container_width=True, key="save_review_raw"):
                        with st.spinner("Saving review..."):
                            success = update_interaction_review(
                                session,
                                st.session_state.interaction_id,
                                review_feedback if review_feedback.strip() else "",
                                rating_overall,
                                rating_accuracy,
                                rating_empathy,
                                rating_safety,
                                rating_relevance,
                                reviewer_name if reviewer_name.strip() else None
                            )
                            
                            if success:
                                st.success("✅ Review saved successfully!")
                                st.balloons()
                                st.rerun()
                            else:
                                st.error("❌ Failed to save review.")
                    
                    st.markdown('</div>', unsafe_allow_html=True)
                
                if parsed_response.get("is_json"):
                    st.info("💡 Toggle to 'Structured View' to see parsed JSON")
                else:
                    st.warning(
                        "⚠️ Response is not valid JSON. "
                        "Showing raw text output."
                    )
            
            # Show token usage metadata below
            st.markdown("---")
            st.markdown("### 📊 Token Usage")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Input Tokens", response_data['input_tokens'])
            with col2:
                st.metric("Output Tokens", response_data['output_tokens'])
            with col3:
                st.metric("Model", response_data['model_name'])
            with col4:
                prompt_hash_display = response_data.get(
                    'prompt_hash', st.session_state.prompt_hash
                )
                short_hash = prompt_hash_display[:8] + "..."
                st.metric("Prompt Hash", short_hash)
                st.caption(f"Full: {prompt_hash_display}")

    # Footer
    st.markdown("---")
    footer_text = (
        f"Database: {DATABASE_NAME} | Schema: {SCHEMA_NAME} | "
        f"Table: {INTERACTIONS_TABLE}"
    )
    st.caption(footer_text)


if __name__ == "__main__":
    main()
