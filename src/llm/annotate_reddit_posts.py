#!/usr/bin/env python3
"""
Reddit Post LLM Annotation Script

This script:
1. Reads from Snowflake table: ANALYTICS_BRONZE.STG_REDDIT_POSTS_PII
2. Calls OpenAI API to annotate posts with taxonomy categorization
3. Writes results back to Snowflake in appropriate schema

Usage:
    python src/llm/annotate_reddit_posts.py
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from openai import OpenAI
import pandas as pd
from typing import Dict, Any, List
import hashlib

# Load environment variables
load_dotenv()

# Default logging setup (will be reconfigured in main with user-specified directory)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prompt template
PROMPT_TEMPLATE = """Task: Given a cleaned Reddit post about pregnancy or maternal care, produce ONE JSON object that includes:
1) Topic categorization per the taxonomy below,
2) A concise factual summary of the post ("post_summary"),
3) A clinician-style, empathetic Reddit reply in the Pomelo Care tone ("care_response").

Extract concise keywords actually present in the text (normalized, lowercase, no stopwords, no duplicates)—for example: "tylenol", "bleeding", "medicaid".

TAXONOMY
groups:
- clinical
  topics: symptoms_body_changes, medications_supplements, test_results_labs, pregnancy_complications, labor_delivery
- mental_health
  topics: anxiety_fear_uncertainty, mood_depression, body_image_identity, relationship_stress, peer_support_requests
- lifestyle_parenting
  topics: nutrition_diet, exercise_movement, sleep_fatigue, work_leave_career, postpartum_care
- access_navigation
  topics: choosing_provider, hospital_clinic_experiences, insurance_costs, telehealth_virtual_care, system_barriers_equity
- community_info
  topics: ask_experiences_advice, share_stories_outcomes, product_device_discussions, information_validation_misinformation
- meta_context
  topics: question_seeking_info, experience_sharing_narrative, opinion_rant_vent, announcement_milestone, policy_advocacy_news

ENUMS
- primary_group ∈ {{clinical, mental_health, lifestyle_parenting, access_navigation, community_info, meta_context}}
- primary_topic ∈ one of the topics listed under its group
- trimester ∈ {{preconception, first, second, third, postpartum, unknown}}
- sentiment ∈ {{negative, neutral, positive}}
- urgency_0_3 ∈ {{0,1,2,3}} (0=routine, 3=urgent)

RULES
- Choose exactly 1 primary_group and 1 primary_topic (topic must belong to the chosen group).
- Optionally add up to 3 secondary_topics from any group (may be []).
- Use "unknown" if unclear.
- Do NOT include the original post text in the JSON.
- `post_summary` must be a neutral, factual summary (1–3 sentences).
- `care_response` must be a safe, empathetic, Reddit-ready reply written in Pomelo's professional tone—helpful, not promotional.

JSON SCHEMA (keys & types)
{{
  "post_id": string,
  "primary_group": string,
  "primary_topic": string,
  "secondary_topics": string[],         // 0–3 items
  "trimester": string,
  "sentiment": string,
  "urgency_0_3": integer,
  "keywords": string[],                 // 0–12 normalized tokens present in the post
  "safety_flags": string[],             // any of: ["misinformation","scope_of_practice","privacy","self_harm","other"]
  "post_summary": string,               // factual summary of the Reddit post
  "care_response": string,              // warm, clinician-style Reddit reply (120–220 words)
  "model_name": string,
  "model_version": string,
  "prompt_hash": string,
  "input_tokens": integer,
  "output_tokens": integer,
  "annotated_at": string                // ISO8601
}}

Return JSON ONLY. Do not include explanations, prose, or markdown.

Now annotate and reply to this post:

post_id: "{{POST_ID}}"
post_text: "{{POST_TEXT}}"

"""


def get_prompt_hash() -> str:
    """Generate a hash of the prompt template for tracking."""
    return hashlib.sha256(PROMPT_TEMPLATE.encode()).hexdigest()[:16]


class LLMAnnotator:
    def __init__(self):
        """Initialize OpenAI and Snowflake connections."""
        # OpenAI client
        self.openai_client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            organization=os.getenv("OPENAI_ORG_ID", None)
        )
        self.model_name = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.model_version = "1.0.0"
        self.prompt_hash = get_prompt_hash()
        
        # Snowflake connection
        self.snowflake_conn = None
        self.connect_snowflake()
        
    def connect_snowflake(self):
        """Connect to Snowflake."""
        try:
            self.snowflake_conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USERNAME"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA", "BRONZE"),
                role=os.getenv("SNOWFLAKE_ROLE")
            )
            logger.info("Connected to Snowflake successfully")
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise
    
    def fetch_posts_to_annotate(self, limit: int = None) -> pd.DataFrame:
        """Fetch posts that need annotation from Snowflake."""
        try:
            query = """
            SELECT 
                post_id,
                text_for_llm,
                text_raw
            FROM ANALYTICS_BRONZE.STG_REDDIT_POSTS_PII
            WHERE needs_annotation = TRUE
            AND post_id NOT IN (
                SELECT DISTINCT post_id 
                FROM ANALYTICS_ML.REDDIT_POSTS_ANNOTATED
            )
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            logger.info(f"Fetching posts to annotate (limit={limit})...")
            df = pd.read_sql(query, self.snowflake_conn)
            # Snowflake returns uppercase column names, convert to lowercase
            df.columns = [col.lower() for col in df.columns]
            logger.info(f"Found {len(df)} posts to annotate")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching posts: {e}")
            raise
    
    def annotate_post(self, post_id: str, post_text: str) -> Dict[str, Any]:
        """Call OpenAI API to annotate a post."""
        try:
            # Prepare prompt
            prompt = PROMPT_TEMPLATE.replace("{{POST_ID}}", post_id).replace("{{POST_TEXT}}", post_text[:2000])  # Limit text length
            
            # Call OpenAI API
            system_message = """You are both a precise clinical text annotator and a Pomelo Care clinician communicator.
Return ONLY valid JSON (no prose, no markdown). If unsure, use "unknown" or [] as specified.
You must (a) categorize the post per the taxonomy, (b) summarize it objectively, and (c) draft a safe, empathetic reply written in the tone and style of Pomelo Care clinicians.

Tone & persona guidance:
- Write with the calm, supportive, and evidence-based voice of a licensed maternal-care clinician from the Pomelo Care team, but do NOT mention your role or the organization by name in the message.
- Warm, inclusive, reassuring, 6th–8th grade reading level.
- Focus on emotional validation and clear, general next steps.
- Never give a medical diagnosis or prescribe medications/doses.
- Encourage the poster to contact their own OB-GYN, midwife, or nurse for individualized advice.

Safety & escalation:
- If the post suggests serious symptoms (e.g., heavy bleeding, severe pain, headache with vision changes, chest pain, shortness of breath, fever ≥100.4°F, decreased fetal movement, suicidal thoughts),  
  → instruct the poster to seek immediate medical evaluation at the nearest ER, Labor & Delivery, or call local emergency services.
- If mental health crisis or self-harm risk appears, advise contacting emergency services or crisis resources right away.

Formatting & style:
- The reply ("care_response") should read naturally as a Reddit comment, in plain text.
- Avoid greetings or sign-offs like "As part of Pomelo Care" or "I'm a nurse." Just speak with warmth and expertise.
- Keep the response ~120–220 words unless safety requires more.
- The model should reference Pomelo Care services only indirectly and generically if helpful (e.g., "Some programs offer 24/7 virtual support or care coordination at no cost."), not as a direct plug or self-introduction."""
            
            response = self.openai_client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )
            
            # Parse response
            content = response.choices[0].message.content
            annotation = json.loads(content)
            
            # Add metadata
            annotation['model_name'] = self.model_name
            annotation['model_version'] = self.model_version
            annotation['prompt_hash'] = self.prompt_hash
            annotation['input_tokens'] = response.usage.prompt_tokens
            annotation['output_tokens'] = response.usage.completion_tokens
            annotation['annotated_at'] = datetime.now(timezone.utc).isoformat()
            
            logger.info(f"Annotated post {post_id} (tokens: {annotation['input_tokens']} + {annotation['output_tokens']})")
            return annotation
            
        except Exception as e:
            logger.error(f"Error annotating post {post_id}: {e}")
            return None
    
    def create_annotation_table(self):
        """Create the ML annotation table if it doesn't exist."""
        try:
            cursor = self.snowflake_conn.cursor()
            
            # Create schema if it doesn't exist
            create_schema_sql = "CREATE SCHEMA IF NOT EXISTS ANALYTICS_ML"
            cursor.execute(create_schema_sql)
            logger.info("Schema ANALYTICS_ML created or already exists")
            
            # Create table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS ANALYTICS_ML.REDDIT_POSTS_ANNOTATED (
                post_id VARCHAR(255) PRIMARY KEY,
                primary_group VARCHAR(50),
                primary_topic VARCHAR(100),
                secondary_topics ARRAY,
                trimester VARCHAR(20),
                sentiment VARCHAR(20),
                urgency_0_3 INTEGER,
                keywords ARRAY,
                safety_flags ARRAY,
                post_summary VARCHAR(1000),
                care_response VARCHAR(2000),
                model_name VARCHAR(100),
                model_version VARCHAR(50),
                prompt_hash VARCHAR(50),
                input_tokens INTEGER,
                output_tokens INTEGER,
                annotated_at TIMESTAMP_TZ
            )
            """
            
            cursor.execute(create_table_sql)
            cursor.close()
            logger.info("Annotation table created or already exists")
            
        except Exception as e:
            logger.error(f"Error creating annotation table: {e}")
            raise
    
    def save_annotations(self, annotations: List[Dict[str, Any]]):
        """Save annotations to Snowflake."""
        if not annotations:
            logger.warning("No annotations to save")
            return
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(annotations)
            
            # Convert column names to UPPERCASE for Snowflake
            df.columns = [col.upper() for col in df.columns]
            
            # Save to Snowflake
            write_pandas(
                self.snowflake_conn,
                df,
                'REDDIT_POSTS_ANNOTATED',
                auto_create_table=False,
                overwrite=False,
                use_logical_type=True,
                schema='ANALYTICS_ML'
            )
            
            logger.info(f"Saved {len(annotations)} annotations to Snowflake")
            
        except Exception as e:
            logger.error(f"Error saving annotations: {e}")
            raise
    
    def close(self):
        """Close Snowflake connection."""
        if self.snowflake_conn:
            self.snowflake_conn.close()
            logger.info("Snowflake connection closed")


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Annotate Reddit posts using OpenAI')
    parser.add_argument('--limit', type=int, default=10, help='Maximum number of posts to annotate')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of posts to process before saving')
    parser.add_argument('--dry-run', action='store_true', help='Fetch and display posts without annotating')
    parser.add_argument('--save-csv', action='store_true', help='Save annotations to timestamped CSV file')
    parser.add_argument('--save-logs', action='store_true', help='Save logs and errors to files')
    parser.add_argument('--csv-dir', type=str, default='data/processed', help='Directory to save CSV files (default: data/processed)')
    parser.add_argument('--log-dir', type=str, default='logs/llm', help='Directory to save log files if --save-logs is used (default: logs/llm)')
    
    args = parser.parse_args()
    
    # Set up logging with optional file output
    log_file = None
    error_file = None
    
    if args.save_logs:
        # Create directory if it doesn't exist
        os.makedirs(args.log_dir, exist_ok=True)
        log_file = f"{args.log_dir}/annotate_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
        error_file = f"{args.log_dir}/errors_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
        
        # Reconfigure logging with file handlers
        logger.handlers.clear()
        logger.addHandler(logging.FileHandler(log_file))
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(logging.INFO)
        
        # Create separate error handler
        error_handler = logging.FileHandler(error_file)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(error_handler)
        
        logger.info(f"Log files will be saved to: {args.log_dir}")
        if log_file:
            logger.info(f"Log file: {log_file}")
            logger.info(f"Error log: {error_file}")
    
    # Log the command that was run
    cmd_str = ' '.join(sys.argv)
    logger.info(f"Command executed: {cmd_str}")
    logger.info(f"Starting annotation run (limit={args.limit}, batch_size={args.batch_size})")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Error log: {error_file}")
    
    # Initialize annotator
    annotator = LLMAnnotator()
    
    try:
        # Create annotation table
        annotator.create_annotation_table()
        
        # Fetch posts to annotate
        posts_df = annotator.fetch_posts_to_annotate(limit=args.limit)
        
        if args.dry_run:
            logger.info("DRY RUN: Would annotate these posts:")
            print(posts_df[['post_id', 'text_for_llm']])
            return
        
        if len(posts_df) == 0:
            logger.info("No posts to annotate")
            return
        
        # Process posts in batches
        annotations = []
        for idx, row in posts_df.iterrows():
            post_id = row['post_id']
            post_text = row['text_for_llm']
            
            logger.info(f"Annotating post {idx+1}/{len(posts_df)}: {post_id}")
            
            annotation = annotator.annotate_post(post_id, post_text)
            
            if annotation:
                annotations.append(annotation)
            
            # Save in batches
            if len(annotations) >= args.batch_size:
                annotator.save_annotations(annotations)
                annotations = []
        
        # Save remaining annotations
        if annotations:
            annotator.save_annotations(annotations)
            
            # Optionally save to CSV file with timestamp
            if args.save_csv:
                os.makedirs(args.csv_dir, exist_ok=True)
                csv_file = f"{args.csv_dir}/annotations_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
                
                # Save to CSV
                annotations_df = pd.DataFrame(annotations)
                annotations_df.to_csv(csv_file, index=False)
                
                logger.info(f"Saved {len(annotations)} annotations to {csv_file}")
        
        logger.info("Annotation complete!")
        if log_file:
            logger.info(f"Full log available at: {log_file}")
            logger.info(f"Error log available at: {error_file}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
    finally:
        annotator.close()


if __name__ == "__main__":
    main()
