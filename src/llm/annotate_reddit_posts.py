#!/usr/bin/env python3
"""
Reddit Post LLM Annotation Script

This script:
1. Reads from Snowflake table: BRONZE.STG_REDDIT_POSTS_PII
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
3) A clinician-style, empathetic Reddit reply ("care_response"),
4) A list of meaningful care-related keywords ("keywords"),
5) A list of safety or escalation flags ("safety_flags").

---

KEYWORDS  
Extract 5–20 meaningful tokens that reflect clinical, behavioral, contextual, or social relevance — not filler or emotional words.  
You may include words from the lists below **plus any other terms the model deems informative for identifying trends or dimensions in pregnancy and maternal care.**  
This includes emerging medications, health technologies, new policies, social issues, or slang commonly used in patient discussions.

Domains (examples only, not exhaustive):
1️⃣ Clinical & symptom-related → "bleeding", "cramping", "pain", "spotting", "swelling", "contractions", "nausea", "headache", "preeclampsia", "ultrasound", "hcg", "glucose test", "infection".  
2️⃣ Medications & labs → "iron", "tylenol", "prenatal vitamin", "magnesium", "zofran", "insulin", "antibiotic", "lab results".  
3️⃣ Mental health → "anxiety", "depression", "panic", "therapy", "postpartum depression", "lonely", "stressed".  
4️⃣ Access & insurance → "medicaid", "insurance", "copay", "appointment", "ob-gyn", "midwife", "telehealth", "clinic".  
5️⃣ Parenting & postpartum → "breastfeeding", "bottle feeding", "sleep", "c-section", "NICU", "pumping", "maternity leave".  
6️⃣ Policy, geographic & social context → "texas", "rural", "law", "policy", "coverage", "equity", "leave policy".  
7️⃣ Other emerging, data-relevant, or trend-signaling terms → anything about technology, social barriers, medication shortages, new slang, or community hashtags.

Exclude stopwords, pronouns, and generic filler (e.g., "help," "please," "feel"). Include lowercase, short tokens only.

---

SAFETY FLAGS  
List all that apply. Use the categories below, adding specific triggers or urgent-care keywords if mentioned.

- "urgent_bleeding" → heavy bleeding, soaking pads, hemorrhage, etc.  
- "urgent_pain" → severe abdominal, pelvic, or back pain; contractions or cramps suggesting preterm labor.  
- "urgent_fever_infection" → fever, chills, discharge, infection, wound issues.  
- "urgent_dizziness_fainting" → fainting, dizziness, low blood pressure, weakness.  
- "urgent_breathing_chest" → shortness of breath, chest pain, heart racing.  
- "urgent_fetal_concern" → no or reduced fetal movement, kick count worries.  
- "urgent_postpartum" → heavy bleeding, severe pain, or fever after delivery.  
- "mental_health_crisis" → suicidal ideation, panic, hopelessness, severe anxiety.  
- "miscarriage_or_loss" → miscarriage, pregnancy loss, stillbirth.  
- "medication_safety" → unsafe drug use, dosing confusion, substance exposure.  
- "infection_or_sepsis" → uterine infection, endometritis, wound infection.  
- "other_concern" → other safety-relevant escalation (e.g., swelling, blurred vision, hypertension).

If no urgent or risky content, return an empty array.

---

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

---

TRIMESTER ENUM LOGIC  
- "preconception" → trying to conceive or planning pregnancy  
- "first", "second", "third" → stated or clearly implied  
- "pregnant" → clearly pregnant but trimester not specified  
- "postpartum" → after giving birth  
- "miscarriage" → discussing pregnancy loss  
- "unclear" → insufficient info to determine pregnancy status  

---

ENUMS
- primary_group ∈ {{clinical, mental_health, lifestyle_parenting, access_navigation, community_info, meta_context}}
- primary_topic ∈ one of the topics listed under its group
- trimester ∈ {{preconception, first, second, third, pregnant, postpartum, miscarriage, unclear}}
- sentiment ∈ {{negative, neutral, positive}}
- urgency_0_3 ∈ {{0,1,2,3}} (0=routine, 3=urgent)

---

RULES
- Choose exactly 1 primary_group and 1 primary_topic.
- Optionally add up to 3 secondary_topics.
- Use "unknown" or "unclear" when needed.
- Do NOT include the original post text in the JSON.
- `post_summary` must be a neutral, factual summary (1–3 sentences).
- `care_response` must be an empathetic, safe Reddit-style clinician reply consistent with Pomelo's tone.

---

JSON SCHEMA
{{
  "post_id": string,
  "primary_group": string,
  "primary_topic": string,
  "secondary_topics": string[],          // 0–3 items
  "trimester": string,
  "sentiment": string,
  "urgency_0_3": integer,
  "keywords": string[],                  // 5–20 informative, trend-aware, domain-relevant tokens
  "safety_flags": string[],              // urgent-care or risk indicators
  "post_summary": string,                // factual summary of the Reddit post
  "care_response": string,               // empathetic clinician-style Reddit reply (120–220 words)
  "model_name": string,
  "model_version": string,
  "prompt_hash": string,
  "input_tokens": integer,
  "output_tokens": integer,
  "annotated_at": string                 // ISO8601
}}

Return JSON ONLY. No explanations or markdown.

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
        """Fetch posts that need annotation from Snowflake.
        Excludes posts already annotated with the same model and prompt."""
        try:
            # Use BRONZE schema (matches dbt staging models)
            database = os.getenv("SNOWFLAKE_DATABASE", "MATERNOSCOPE")
            model_name = self.model_name
            prompt_hash = self.prompt_hash
            
            query = f"""
            SELECT 
                p.post_id,
                p.text_for_llm,
                p.text_raw
            FROM {database}.BRONZE.STG_REDDIT_POSTS_PII p
            WHERE p.needs_annotation = TRUE
            AND NOT EXISTS (
                SELECT 1 
                FROM {database}.ANALYTICS_ML.REDDIT_POSTS_ANNOTATED a
                WHERE a.post_id = p.post_id
                AND a.model_name = '{model_name}'
                AND a.prompt_hash = '{prompt_hash}'
            )
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            logger.info(
                f"Fetching posts to annotate (limit={limit}, "
                f"model={model_name}, prompt_hash={prompt_hash[:8]}...)..."
            )
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

Your tasks:
(a) Categorize the post using the taxonomy.
(b) Summarize it objectively.
(c) Generate a safe, empathetic clinician-style Reddit reply in Pomelo Care's tone.
(d) Extract care-relevant keywords and safety flags for downstream analysis.

Tone & persona:
- Write in the calm, supportive, and informed tone of a licensed maternal-care clinician.
- Do NOT introduce yourself or mention any organization.
- Warm, inclusive, reassuring, 6th–8th grade reading level.
- Provide general, educational guidance; do NOT diagnose or prescribe.
- Encourage follow-up with their OB-GYN, midwife, or nurse for individualized care.
- If serious symptoms appear (e.g., heavy bleeding, severe pain, headache with vision changes, fever ≥100.4°F, shortness of breath, chest pain, suicidal thoughts), instruct immediate evaluation at an ER, Labor & Delivery, or local emergency services.
- If a mental health crisis is implied, recommend emergency or crisis line support."""
            
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
            # Note: Using composite unique key (post_id, model_name, prompt_hash)
            # to allow multiple annotations per post with different models/prompts
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS ANALYTICS_ML.REDDIT_POSTS_ANNOTATED (
                post_id VARCHAR(255),
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
                text_for_llm VARCHAR(16777216),
                model_name VARCHAR(100),
                model_version VARCHAR(50),
                prompt_hash VARCHAR(50),
                input_tokens INTEGER,
                output_tokens INTEGER,
                annotated_at TIMESTAMP_TZ,
                PRIMARY KEY (post_id, model_name, prompt_hash)
            )
            """
            
            cursor.execute(create_table_sql)
            
            # Check if table exists and needs migration
            try:
                # Check if text_for_llm column exists
                check_column_sql = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = 'ANALYTICS_ML' 
                AND TABLE_NAME = 'REDDIT_POSTS_ANNOTATED' 
                AND COLUMN_NAME = 'TEXT_FOR_LLM'
                """
                cursor.execute(check_column_sql)
                column_exists = cursor.fetchone()[0] > 0
                
                if not column_exists:
                    alter_table_sql = """
                    ALTER TABLE ANALYTICS_ML.REDDIT_POSTS_ANNOTATED 
                    ADD COLUMN text_for_llm VARCHAR(16777216)
                    """
                    cursor.execute(alter_table_sql)
                    logger.info("Added text_for_llm column to existing table")
                
                # Check if primary key is composite (post_id, model_name, prompt_hash)
                # If not, we'll use MERGE which will work but only allow one annotation per post_id
                check_pk_sql = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                WHERE tc.TABLE_SCHEMA = 'ANALYTICS_ML'
                AND tc.TABLE_NAME = 'REDDIT_POSTS_ANNOTATED'
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND kcu.COLUMN_NAME IN ('POST_ID', 'MODEL_NAME', 'PROMPT_HASH')
                GROUP BY tc.CONSTRAINT_NAME
                HAVING COUNT(DISTINCT kcu.COLUMN_NAME) = 3
                """
                cursor.execute(check_pk_sql)
                has_composite_pk = cursor.fetchone()[0] > 0 if cursor.rowcount > 0 else False
                
                if not has_composite_pk:
                    logger.warning(
                        "Table exists with old PRIMARY KEY (post_id only). "
                        "MERGE will work but only one annotation per post_id is allowed. "
                        "Consider recreating table with composite PRIMARY KEY for full idempotency."
                    )
                    
            except Exception as e:
                # If checks fail, table might not exist yet or permission issue
                logger.warning(f"Could not check table schema: {e}")
            
            cursor.close()
            logger.info("Annotation table created or already exists")
            
        except Exception as e:
            logger.error(f"Error creating annotation table: {e}")
            raise
    
    def save_annotations(self, annotations: List[Dict[str, Any]]):
        """Save annotations to Snowflake using MERGE for idempotency.
        Updates existing annotations if post_id, model_name, and prompt_hash match."""
        if not annotations:
            logger.warning("No annotations to save")
            return
        
        try:
            cursor = self.snowflake_conn.cursor()
            database = os.getenv("SNOWFLAKE_DATABASE", "MATERNOSCOPE")
            
            # Process each annotation with MERGE for idempotency
            saved_count = 0
            skipped_count = 0
            
            for annotation in annotations:
                try:
                    # Sanitize string values
                    post_summary = str(annotation.get('post_summary', '')).replace("'", "''").replace('\n', ' ').replace('\r', ' ')
                    care_response = str(annotation.get('care_response', '')).replace("'", "''").replace('\n', ' ').replace('\r', ' ')
                    text_for_llm = str(annotation.get('text_for_llm', '')).replace("'", "''").replace('\n', ' ').replace('\r', ' ')
                    
                    # Format arrays for Snowflake
                    def format_array(arr):
                        if not arr:
                            return 'ARRAY_CONSTRUCT()'
                        # Escape single quotes in array elements
                        escaped = [str(item).replace("'", "''") for item in arr]
                        # Use single quotes in the join to avoid f-string quote issues
                        quoted_items = [f"'{item}'" for item in escaped]
                        return f"ARRAY_CONSTRUCT({', '.join(quoted_items)})"
                    
                    secondary_topics = format_array(annotation.get('secondary_topics', []))
                    keywords = format_array(annotation.get('keywords', []))
                    safety_flags = format_array(annotation.get('safety_flags', []))
                    
                    # MERGE statement for idempotent upsert
                    merge_sql = f"""
                    MERGE INTO {database}.ANALYTICS_ML.REDDIT_POSTS_ANNOTATED AS target
                    USING (
                        SELECT
                            '{annotation.get('post_id')}' AS post_id,
                            '{annotation.get('primary_group', '')}' AS primary_group,
                            '{annotation.get('primary_topic', '')}' AS primary_topic,
                            {secondary_topics} AS secondary_topics,
                            '{annotation.get('trimester', '')}' AS trimester,
                            '{annotation.get('sentiment', '')}' AS sentiment,
                            {annotation.get('urgency_0_3', 0)} AS urgency_0_3,
                            {keywords} AS keywords,
                            {safety_flags} AS safety_flags,
                            '{post_summary}' AS post_summary,
                            '{care_response}' AS care_response,
                            '{text_for_llm}' AS text_for_llm,
                            '{annotation.get('model_name', '')}' AS model_name,
                            '{annotation.get('model_version', '')}' AS model_version,
                            '{annotation.get('prompt_hash', '')}' AS prompt_hash,
                            {annotation.get('input_tokens', 0)} AS input_tokens,
                            {annotation.get('output_tokens', 0)} AS output_tokens,
                            '{annotation.get('annotated_at', '')}' AS annotated_at
                    ) AS source
                    ON target.post_id = source.post_id
                       AND target.model_name = source.model_name
                       AND target.prompt_hash = source.prompt_hash
                    WHEN MATCHED THEN
                        UPDATE SET
                            primary_group = source.primary_group,
                            primary_topic = source.primary_topic,
                            secondary_topics = source.secondary_topics,
                            trimester = source.trimester,
                            sentiment = source.sentiment,
                            urgency_0_3 = source.urgency_0_3,
                            keywords = source.keywords,
                            safety_flags = source.safety_flags,
                            post_summary = source.post_summary,
                            care_response = source.care_response,
                            text_for_llm = source.text_for_llm,
                            model_version = source.model_version,
                            input_tokens = source.input_tokens,
                            output_tokens = source.output_tokens,
                            annotated_at = source.annotated_at
                    WHEN NOT MATCHED THEN
                        INSERT (
                            post_id, primary_group, primary_topic, secondary_topics,
                            trimester, sentiment, urgency_0_3, keywords, safety_flags,
                            post_summary, care_response, text_for_llm,
                            model_name, model_version, prompt_hash,
                            input_tokens, output_tokens, annotated_at
                        )
                        VALUES (
                            source.post_id, source.primary_group, source.primary_topic,
                            source.secondary_topics, source.trimester, source.sentiment,
                            source.urgency_0_3, source.keywords, source.safety_flags,
                            source.post_summary, source.care_response, source.text_for_llm,
                            source.model_name, source.model_version, source.prompt_hash,
                            source.input_tokens, source.output_tokens, source.annotated_at
                        )
                    """
                    
                    cursor.execute(merge_sql)
                    saved_count += 1
                    
                except Exception as e:
                    logger.warning(
                        f"Error saving annotation for post_id {annotation.get('post_id')}: {e}"
                    )
                    skipped_count += 1
                    continue
            
            cursor.close()
            logger.info(
                f"Saved {saved_count} annotations to Snowflake "
                f"(skipped {skipped_count} due to errors)"
            )
            
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
    parser.add_argument('--limit', type=int, default=None, help='Maximum number of posts to annotate (default: no limit)')
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
        total_posts = len(posts_df)
        successful_annotations = 0
        failed_annotations = 0
        
        for idx, row in posts_df.iterrows():
            post_id = row['post_id']
            post_text = row['text_for_llm']
            
            logger.info(f"Annotating post {idx+1}/{total_posts}: {post_id}")
            
            try:
                annotation = annotator.annotate_post(post_id, post_text)
                
                if annotation:
                    # Add text_for_llm to the annotation before saving
                    annotation['text_for_llm'] = post_text
                    annotations.append(annotation)
                    successful_annotations += 1
                    
                    # Save in batches
                    if len(annotations) >= args.batch_size:
                        try:
                            annotator.save_annotations(annotations)
                            logger.info(f"Saved batch of {len(annotations)} annotations to Snowflake")
                        except Exception as save_error:
                            logger.error(f"Error saving batch to Snowflake: {save_error}")
                            logger.error(f"Failed to save batch of {len(annotations)} annotations")
                        finally:
                            annotations = []
                
            except Exception as e:
                failed_annotations += 1
                logger.error(f"Error annotating post {post_id}: {e}")
                logger.error("Continuing with next post...")
                continue
        
        # Save remaining annotations
        if annotations:
            try:
                annotator.save_annotations(annotations)
                logger.info(f"Saved final batch of {len(annotations)} annotations to Snowflake")
            except Exception as save_error:
                logger.error(f"Error saving final batch to Snowflake: {save_error}")
                logger.error(f"Failed to save final batch of {len(annotations)} annotations")
            
            # Optionally save to CSV file with timestamp
            if args.save_csv:
                os.makedirs(args.csv_dir, exist_ok=True)
                csv_file = f"{args.csv_dir}/annotations_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
                
                # Save to CSV
                annotations_df = pd.DataFrame(annotations)
                annotations_df.to_csv(csv_file, index=False)
                
                logger.info(f"Saved {len(annotations)} annotations to {csv_file}")
        
        logger.info("=" * 50)
        logger.info("Annotation complete!")
        logger.info(f"Total posts processed: {total_posts}")
        logger.info(f"Successful annotations: {successful_annotations}")
        logger.info(f"Failed annotations: {failed_annotations}")
        logger.info("=" * 50)
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
