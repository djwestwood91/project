# Pokemon Card Data Processing Pipeline

A Python-based ETL pipeline for processing Pokemon trading card data, with AWS S3 integration for file storage and PostgreSQL for data persistence.

## Overview

This project automates the ingestion, transformation, and storage of Pokemon card data. It performs the following operations:

- **S3 Integration**: Upload/download files to/from AWS S3 bucket
- **Landing Layer**: Process raw Pokemon card data into a landing schema
- **Lookup Tables**: Populate reference data (language, sets, grading companies, grades, rarity, currency, purchase source)
- **Card Processing**: Insert and manage card data with full 3NF normalization
- **Card Instance Tracking**: Track individual physical instances of each card
- **Grade Processing**: Handle card grading information with lineage tracking

## Project Structure

```
project/
├── main.py                          # Main pipeline orchestrator
├── references.py                    # Database and file path configurations
├── card.py                          # Card data processing logic
├── grade.py                         # Grade data processing logic
├── utils/
│   ├── aws_s3_utils.py              # AWS S3 client and operations
│   └── db_utils.py                  # Database utilities (model creation, table clearing)
├── landing/
│   └── pokemon_cards_landing.py     # Landing layer data processing
├── lookup/
│   └── lookup_values_load.py        # Lookup table population
├── model_references/
│   └── model.sql                    # Database schema definitions
└── files/                           # Local data files directory
    ├── processed_data/              # Output directory for processed files
    └── source_data/                 # Input directory for source files
```

## Prerequisites

- Python 3.8+
- PostgreSQL database
- AWS Account with S3 bucket access
- Required Python packages (see Installation)

## Installation

1. Clone or download the project
2. Install dependencies:
```bash
pip install boto3 sqlalchemy python-dotenv
```

3. Configure environment variables in `.env` file:
```
# Database Configuration
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=pokemon_db
DB_LANDING_SCHEMA=pokemon_landing
DB_MAIN_SCHEMA=pokemon

# S3 Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_NAME=pokemon-app-demo-bucket
S3_BUCKET_PREFIX=pokemon_data/

# File Paths
FILE_PATH=./files/
FILE_NAME=pokemon_cards.csv
FILE_SHEET_NAME=pokemon_data
```

## Pipeline Workflow

The `main.py` script orchestrates a data-driven pipeline with the following workflow:

### Database Setup Phase
1. **Create Database Model**: Executes `model.sql` to create all schemas and tables
   - `pokemon_landing` schema for raw data ingestion
   - `pokemon` schema for processed data storage with full 3NF normalization
   - Controlled by `PIPELINE_CREATE_MODEL` environment variable (default: True)

2. **Clear Landing Table**: Optionally clears existing landing table data before loading new data
   - Controlled by `PIPELINE_CLEAR_LANDING` environment variable (default: False)

### Data Processing Phase
3. **List S3 Objects**: Lists all available objects in the configured S3 bucket
   - Controlled by `PIPELINE_LIST_S3` environment variable (default: True)

4. **Upload to S3**: Uploads the local Pokemon card data file to S3
   - Controlled by `PIPELINE_UPLOAD_S3` environment variable (default: True)

5. **Load Landing Data**: Processes raw Pokemon card CSV/Excel data into the landing table

6. **Load Lookup Tables**: Populates reference/dimension tables (data-driven approach):
   - Languages
   - Card Sets
   - Grading Companies
   - Grade Descriptions
   - Card Rarity Levels
   - Currencies
   - Purchase Sources
   - Countries 

7. **Load Card Data**: Inserts normalized card records into the main `pokemon.card` table with source traceability

8. **Load Card Instance Data**: Creates individual card instance records tracking each unique physical copy collected

9. **Load Grade Data**: Processes and inserts card grading information into the `pokemon.card_grade` table linked to card instances

10. **Download from S3**: Optionally downloads processed files from S3 for archive/verification
    - Controlled by `PIPELINE_DOWNLOAD_S3` environment variable (default: False)

### Pipeline Logging
- Each step is numbered and tracked (e.g., "[Step 1/14]")
- Status indicators: `[OK]` for success, `[FAIL]` for errors
- All operations logged to both console and `db.log`
- If pipeline fails, the exact failing step is reported

### Optional Features (Currently Disabled)
- **Seller data processing**: Extract and load seller/vendor information from source data
- **Purchase transaction processing**: Extract and load purchase details including price, fees, purchase source, and purchase date

## Usage

### Running the Complete Pipeline

Execute the main pipeline script:
```bash
python main.py
```

This will run both `run_db_utils()` and `run_poke_pipeline()` functions sequentially.

### Main Functions

**`run_db_utils()`**
- Creates the database model and schema
- Optionally clears the landing table
- Must be run before the first pipeline execution

**`run_poke_pipeline()`**
- Executes the full data processing workflow
- Includes S3 operations, landing load, lookup population, and card/grade insertion
- Logs all operations to console and `db.log`

### Controlling Pipeline Behavior

Pipeline steps can be controlled via environment variables in the `.env` file, enabling flexible configuration for different environments (development, testing, production):

```bash
# Database Setup Control
PIPELINE_CREATE_MODEL=True          # Create database model on startup
PIPELINE_CLEAR_LANDING=False        # Clear landing table before loading (WARNING: causes data loss)

# S3 Operations Control
PIPELINE_LIST_S3=True               # List S3 bucket contents
PIPELINE_UPLOAD_S3=True             # Upload local file to S3
PIPELINE_DOWNLOAD_S3=False          # Download processed files from S3
```

**Example configurations:**

*Development (test everything locally):*
```bash
PIPELINE_CREATE_MODEL=True
PIPELINE_LIST_S3=False
PIPELINE_UPLOAD_S3=False
PIPELINE_DOWNLOAD_S3=False
```

*Production (skip S3, data already loaded):*
```bash
PIPELINE_CREATE_MODEL=False
PIPELINE_LIST_S3=False
PIPELINE_UPLOAD_S3=False
PIPELINE_DOWNLOAD_S3=False
```

## Database Schema

The pipeline uses two schemas with **full 3NF normalization**:

- **pokemon_landing**: Intermediate schema for raw data ingestion
- **pokemon**: Main schema for processed data storage

### Core Tables

- **card**: Base Pokemon card data (logical cards) - references `card_set`, `language`, `rarity`
- **card_instance**: Individual physical instances of cards - tracks each unique copy collected
- **card_grade**: Grading information for card instances - links to `card_instance` and `grade_description`
- **seller**: Seller information - stores seller/vendor details including name, website, and country
- **purchase**: Purchase transaction records - links `card_instance` to `seller`, tracks purchase price, postage fees, total cost, purchase date, and currency

### Lookup Tables (Dimensions)

- **card_set**: Pokemon set lookup
- **language**: Card language lookup
- **rarity**: Card rarity lookup
- **grading_company**: Grading company lookup
- **grade_description**: Grade descriptions - references `grading_company`
- **currency**: Currency codes for purchases
- **purchase_source**: Purchase source lookup

### Data Lineage

All source-derived tables include `row_id` for traceability:
- `card.row_id` - Maps card to landing table source
- `card_instance.row_id` - Maps individual card instance to landing table source
- `card_grade.row_id` - Maps grade record to landing table source
- `purchase.row_id` - Maps purchase record to landing table source
- `seller.row_id` - Maps seller record to landing table source

## Configuration

### Environment Variables

All configurations (database credentials, AWS credentials, file paths, and pipeline control) are stored in the `.env` file:

```bash
# Database Configuration
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_LANDING_SCHEMA=pokemon_landing
DB_MAIN_SCHEMA=pokemon

# Database Tables
DB_LANDING_TABLE=landing_pokemon_card
DB_CARD_TABLE=card
DB_CARD_INSTANCE_TABLE=card_instance
DB_CARD_GRADE_TABLE=card_grade
DB_SELLER_TABLE=seller
DB_PURCHASE_TABLE=purchase

# Lookup Tables
DB_LANGUAGE_LOOKUP_TABLE=language
DB_SET_LOOKUP_TABLE=card_set
DB_GRADING_COMPANY_LOOKUP_TABLE=grading_company
DB_GRADE_DESCRIPTION_LOOKUP_TABLE=grade_description
DB_RARITY_LOOKUP_TABLE=rarity
DB_CURRENCY_LOOKUP_TABLE=currency
DB_PURCHASE_SOURCE_LOOKUP_TABLE=purchase_source

# File Paths
FILE_PATH=files/source_data/
FILE_NAME=pokemon.xlsx
FILE_SHEET_NAME=Pokemon Cards
OUTPUT_FILE_PATH=files/processed_data/
OUTPUT_FILE_NAME=pokemon_cards_processed.xlsx

# S3 Configuration
AWS_REGION=eu-west-2
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_NAME=pokemon-app-demo-bucket
S3_BUCKET_PREFIX=pokemon_data/

# Pipeline Control Flags
PIPELINE_CREATE_MODEL=True
PIPELINE_CLEAR_LANDING=False
PIPELINE_LIST_S3=True
PIPELINE_UPLOAD_S3=True
PIPELINE_DOWNLOAD_S3=False
```

### Logging

The pipeline logs output to:
- Console (stdout)
- File: `db.log`

Log level is set to INFO by default.

## AWS S3 Functions

The `aws_s3_utils.py` module provides:

- `setup_s3_client()`: Initialize AWS S3 client
- `list_objects()`: List files in S3 bucket
- `upload_file_to_s3()`: Upload local file to S3
- `download_file_from_s3()`: Download file from S3

## Error Handling

The pipeline includes comprehensive error handling:
- Database connection errors
- File not found errors
- AWS S3 operation errors
- All errors are logged and re-raised for visibility

## Development Notes

- Update the `S3_BUCKET_PREFIX` environment variable to organize files in S3
- Ensure the database schemas and tables exist before running the pipeline
- Verify AWS credentials have appropriate S3 permissions
- Check file paths in `.env` match your local file structure

## License

Private project