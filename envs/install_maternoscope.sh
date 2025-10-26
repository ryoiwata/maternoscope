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
# CORE PYTHON PACKAGES
# =============================================================================

# HTTP library for making API requests (used by Reddit scrapers)
conda install conda-forge::requests --yes

# Environment variable management (loads .env files for API credentials)
pip install python-dotenv

# Task scheduling library (for automated scraping jobs)
conda install conda-forge::schedule --yes

# =============================================================================
# REDDIT API & DATA PROCESSING
# =============================================================================

# Python Reddit API Wrapper - official Reddit API client
conda install conda-forge::praw --yes

# Data manipulation and analysis library (CSV/JSON processing)
conda install conda-forge::pandas --yes

# High-performance columnar data format (used by pandas for better performance)
conda install -c conda-forge pyarrow --yes

# =============================================================================
# SNOWFLAKE DATABASE CONNECTION
# =============================================================================

# Core Snowflake connector for Python
conda install conda-forge::snowflake-connector-python --yes

# Enhanced Snowflake connector with pandas integration (enables direct DataFrame uploads)
pip install --upgrade "snowflake-connector-python[pandas]"

# =============================================================================
# ADDITIONAL DATA PROCESSING TOOLS
# =============================================================================

# Excel file reading/writing support (for data export/import)
conda install conda-forge::openpyxl --yes

# PostgreSQL adapter for Python (alternative database option)
conda install anaconda::psycopg2 --yes

# =============================================================================
# DATA TRANSFORMATION (DBT) - COMMENTED OUT DUE TO PYTHON COMPATIBILITY
# =============================================================================

# TODO: Try with conda install first, then fallback to pip if needed
# DBT Core - data transformation framework
# pip install dbt-core 

# DBT adapter for PostgreSQL (if using PostgreSQL instead of Snowflake)
# pip install dbt-postgres

# DBT adapter for Snowflake (recommended for this project)
# pip install dbt-snowflake