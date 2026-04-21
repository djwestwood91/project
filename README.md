# Pokemon Card Data Processing Pipeline

A Python-based ETL pipeline for processing Pokemon trading card data, with AWS S3 integration for file storage and PostgreSQL for data persistence.

## Overview

This project automates the ingestion, transformation, and storage of Pokemon card data. It performs the following operations:

- **S3 Integration**: Upload/download files to/from AWS S3 bucket
- **Landing Layer**: Process raw Pokemon card data into a landing schema
- **Landing Validation**: Validate landing table data quality before processing
- **Lookup Tables**: Populate reference data (language, sets, grading companies, grades, rarity, currency, purchase source, countries)
- **Card Processing**: Insert and manage card data with full 3NF normalization
- **Card Instance Tracking**: Track individual physical instances of each card
- **Grade Processing**: Handle card grading information with lineage tracking
- **Seller Processing**: Extract and load seller/vendor information
- **Purchase Processing**: Track purchase transactions with pricing and source data

## Project Structure

```
project/
├── main.py                          # Main pipeline orchestrator
├── references.py                    # Database and file path configurations
├── card.py                          # Card data processing logic
├── card_instance.py                 # Card instance data processing logic
├── grade.py                         # Grade data processing logic
├── seller.py                        # Seller data processing logic
├── purchase.py                      # Purchase transaction processing logic
├── excel_data_output.py             # Excel export to Tableau with multiple sheets
├── utils/
│   ├── aws_s3_utils.py              # AWS S3 client and operations
│   └── db_utils.py                  # Database utilities (model creation, table clearing, identifier validation)
├── landing/
│   ├── pokemon_cards_landing.py     # Landing layer data processing
│   └── landing_validation.py        # Landing table data quality validation
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

6. **Validate Landing Data**: Performs quality checks on landing table data before downstream processing

7. **Load Lookup Tables**: Populates reference/dimension tables (data-driven approach):
   - Languages
   - Card Sets
   - Grading Companies
   - Grade Descriptions
   - Card Rarity Levels
   - Currencies
   - Purchase Sources
   - Countries

8. **Load Card Data**: Inserts normalized card records into the main `pokemon.card` table with source traceability

9. **Load Card Instance Data**: Creates individual card instance records tracking each unique physical copy collected

10. **Load Grade Data**: Processes and inserts card grading information into the `pokemon.card_grade` table linked to card instances

11. **Load Seller Data**: Extracts and inserts seller/vendor information into the `pokemon.seller` table

12. **Load Purchase Data**: Inserts purchase transaction records linking card instances to sellers with pricing and source information

14. **Export to Excel**: Writes all processed data tables to a single Excel file with 13 sheets for Tableau and BI tool integration
    - 5 fact table sheets: card, card_instance, card_grade, seller, purchase
    - 8 dimensional table sheets: card_set, language, rarity, grading_company, grade_description, currency, purchase_source, country
    - Automatically handles timezone-aware datetime conversion for Excel compatibility
    - Applies column autosizing to all sheets for optimal readability

15. **Download from S3**: Optionally downloads processed files from S3 for archive/verification
    - Controlled by `PIPELINE_DOWNLOAD_S3` environment variable (default: False)

### Pipeline Logging
- Each step is numbered and tracked (e.g., "[Step 1/23]")
- Status indicators: `[OK]` for success, `[FAIL]` for errors
- All operations logged to both console and `db.log`
- If pipeline fails, the exact failing step is reported

### Active Data Processing Features
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
- Includes S3 operations, landing load, lookup population, card/instance/grade insertion
- Loads seller and purchase transaction data
- Exports all processed data to Excel with multiple sheets
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
- **pokemon**: Main schema for processed data storage with **Snowflake Schema** design

### Schema Type: Snowflake Schema

This implementation uses a **Snowflake Schema** (normalized star schema) rather than a pure star schema. The key characteristic is the presence of **hierarchical relationships between dimension tables**:

- **Hierarchical Dimension**: `grade_description` contains a foreign key reference to `grading_company`, creating a normalized dimension hierarchy
- **Multiple Fact Tables**: Multiple interconnected fact tables (`card`, `card_instance`, `card_grade`, `purchase`) that reference both dimensions and other facts
- **Normalized Dimensions**: Dimensions maintain referential integrity and avoid redundant data storage

**Benefits of Snowflake Schema:**
- Maintains data integrity through normalized relationships
- Reduces data redundancy (e.g., `grading_company` data is stored once and referenced)
- Provides good balance between query performance and storage efficiency
- Suitable for transactional systems with frequent updates

### Core Tables (Facts)

- **card**: Base Pokemon card data (logical cards) - references `card_set`, `language`, `rarity`
- **card_instance**: Individual physical instances of cards - tracks each unique copy collected
- **card_grade**: Grading information for card instances - links to `card_instance` and `grade_description`
- **seller**: Seller information - stores seller/vendor details including name, website, and country
- **purchase**: Purchase transaction records - links `card_instance` to `seller`, tracks purchase price, postage fees, total cost, purchase date, and currency

### Lookup Tables (Dimensions)

**Independent Dimensions:**
- **card_set**: Pokemon set lookup
- **language**: Card language lookup
- **rarity**: Card rarity lookup
- **currency**: Currency codes for purchases
- **purchase_source**: Purchase source lookup
- **country**: Country lookup

**Hierarchical Dimensions:**
- **grading_company**: Grading company lookup (parent dimension)
- **grade_description**: Grade descriptions - references `grading_company` (child dimension)

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
DB_COUNTRY_LOOKUP_TABLE=country

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

- `setup_s3_client()`: Initialize AWS S3 client with credential validation
- `list_s3_objects()`: List files in S3 bucket with prefix support
- `upload_file_to_s3()`: Upload local file to S3 using streaming (optimized for large files, avoids memory issues)
- `download_file_from_s3()`: Download file from S3 to local storage

**S3 Upload Optimization**: Uses streaming `put_object()` instead of loading entire files into memory, resolving seekable stream errors and improving performance for larger files.

## Excel Output for Tableau

The pipeline exports processed data to a single Excel file with multiple sheets for easy integration with Tableau and other BI tools. The export includes both fact tables and all dimensional/lookup tables for complete data context.

### Excel Output Structure

All data is written to a single file (`pokemon_cards_processed.xlsx`) with 13 separate sheets:

**Fact Tables (5 sheets):**
- **card**: Base card data with set, language, and rarity information
- **card_instance**: Individual card instances with card references
- **card_grade**: Grading information with certification numbers and URLs
- **seller**: Seller/vendor information including website and country
- **purchase**: Purchase transactions linking instances to sellers with pricing

**Dimensional Tables (8 sheets):**
- **card_set**: Pokemon set reference data
- **language**: Language reference data for cards
- **rarity**: Card rarity classifications
- **grading_company**: Grading company reference data
- **grade_description**: Grade descriptions with company references
- **currency**: Currency codes for purchase transactions
- **purchase_source**: Purchase source reference data
- **country**: Country reference data for sellers and shipping

### Timezone-Aware DateTime Handling

The pipeline automatically converts timezone-aware datetime columns to timezone-naive format before exporting to Excel. This ensures compatibility with Excel's datetime format, which does not support timezone information.

**Key Features:**
- `convert_timezone_aware_datetimes()`: Strips timezone info while preserving local time values
- Uses pandas `is_datetime64tz_dtype()` for precise timezone-aware column detection
- Applied automatically during Excel export
- Column autosizing applied to all sheets for optimal readability

### Excel Output Functions

**Main Function:**
- `output_all_data_to_excel_for_tableau()`: Orchestrates the export of all 13 tables (5 fact + 8 dimensional) to a single Excel file with multiple sheets

**Fact Table Read Functions:**
- `read_card_data()`: Retrieves card table with explicit column selection
- `read_card_instance_data()`: Retrieves card instance data
- `read_card_grade_data()`: Retrieves grading information
- `read_seller_data()`: Retrieves seller/vendor information
- `read_purchase_data()`: Retrieves purchase transaction records

**Dimensional Table Read Functions:**
- `read_card_set_lookup()`: Retrieves card set lookup data
- `read_language_lookup()`: Retrieves language lookup data
- `read_rarity_lookup()`: Retrieves rarity lookup data
- `read_grading_company_lookup()`: Retrieves grading company lookup data
- `read_grade_description_lookup()`: Retrieves grade description lookup data
- `read_currency_lookup()`: Retrieves currency lookup data
- `read_purchase_source_lookup()`: Retrieves purchase source lookup data
- `read_country_lookup()`: Retrieves country lookup data

**Legacy Compatibility:**
Original five output functions (`output_card_data_to_excel_for_tableau()`, `output_card_instance_data_to_excel_for_tableau()`, etc.) now call the consolidated function, ensuring backward compatibility with existing code.

## Error Handling

The pipeline includes comprehensive error handling:
- Database connection errors with connection state verification
- File not found errors with path validation
- AWS S3 operation errors (ClientError, UnseekableStreamError, etc.)
- SQL injection prevention through identifier validation
- All errors are logged with full context and re-raised for visibility

## Development Notes

- Update the `S3_BUCKET_PREFIX` environment variable to organize files in S3
- Ensure the database schemas and tables exist before running the pipeline
- Verify AWS credentials have appropriate S3 permissions
- Check file paths in `.env` match your local file structure

## License

Private project for learning purposes