#!/bin/bash
# MaternoScope Environment Setup Script
# This script creates a conda environment with all dependencies needed for the MaternoScope project

# Deactivate any currently active conda environment
conda deactivate

# Create conda environment with Python 3.12 (compatible with DBT)
# Using local path ./maternoscope instead of global environment
conda create -p ./maternoscope python=3.12 --yes

# Activate the newly created environment
conda activate ./maternoscope

# =============================================================================
# INSTALLATION ORDER: DEPENDENCIES FIRST, THEN APPLICATIONS
# =============================================================================

# =============================================================================
# 1. FOUNDATION PACKAGES (Install first - no dependencies on other packages)
# =============================================================================

# HTTP library for making API requests (used by Reddit scrapers)
conda install conda-forge::requests --yes

# Environment variable management (loads .env files for API credentials)
pip install python-dotenv

# Task scheduling library (for automated scraping jobs)
conda install conda-forge::schedule --yes

# =============================================================================
# 2. DATA PROCESSING FOUNDATION (Install before packages that depend on them)
# =============================================================================

# High-performance columnar data format (install before pandas for better compatibility)
conda install -c conda-forge pyarrow --yes

# Data manipulation and analysis library (CSV/JSON processing)
# Install pandas after pyarrow for optimal performance
conda install conda-forge::pandas --yes

# Excel file reading/writing support (depends on pandas)
conda install conda-forge::openpyxl --yes

# =============================================================================
# 3. DATABASE CONNECTORS (Install after pandas for DataFrame support)
# =============================================================================

# PostgreSQL adapter for Python (alternative database option)
conda install anaconda::psycopg2 --yes

# Core Snowflake connector for Python
conda install conda-forge::snowflake-connector-python --yes

# Enhanced Snowflake connector with pandas integration (enables direct DataFrame uploads)
# Install AFTER base snowflake-connector-python to avoid conflicts
pip install --upgrade "snowflake-connector-python[pandas]"

# =============================================================================
# 4. DATA TRANSFORMATION FRAMEWORK (DBT) - Install after database connectors
# =============================================================================

# DBT Core - data transformation framework
# Install after database connectors for proper adapter support
conda install conda-forge::dbt-core --yes

# DBT adapter for Snowflake (recommended for this project)
# Install AFTER dbt-core to ensure proper dependency resolution
conda install conda-forge::dbt-snowflake --yes

# =============================================================================
# 5. AI/ML LIBRARIES (Install after data processing foundation)
# =============================================================================

# OpenAI Python client for GPT API access (content analysis, sentiment analysis)
# Install after pandas for potential DataFrame integration
conda install conda-forge::openai --yes

# =============================================================================
# 6. WORKFLOW ORCHESTRATION (Install after all core dependencies)
# =============================================================================

# Apache Airflow - workflow orchestration and scheduling
# Install after all other packages due to complex dependency requirements
conda install conda-forge::airflow --yes

# =============================================================================
# 7. API CLIENTS (Install last - least critical dependencies)
# =============================================================================

# Python Reddit API Wrapper - official Reddit API client
# Install after all other packages for clean dependency tree
conda install conda-forge::praw --yes
