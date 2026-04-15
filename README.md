# Pokemon Card Data Processing Pipeline

A Python-based ETL pipeline for processing Pokemon trading card data, with AWS S3 integration for file storage and PostgreSQL for data persistence.

## Overview

This project automates the ingestion, transformation, and storage of Pokemon card data. It performs the following operations:

- **S3 Integration**: Upload/download files to/from AWS S3 bucket
- **Landing Layer**: Process raw Pokemon card data into a landing schema
- **Lookup Tables**: Populate reference data (language, sets, grading companies, grades, rarity)
- **Card Processing**: Insert and manage card data
- **Grade Processing**: Handle card grading information

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

The `main.py` script orchestrates the following workflow:

### Database Setup Phase
1. **Create Database Model**: Executes `model.sql` to create all schemas and tables
   - `pokemon_landing` schema for raw data ingestion
   - `pokemon` schema for processed data storage
   - Controlled by `create_model_flag` parameter (default: True)

2. **Clear Landing Table**: Optionally clears existing landing table data before loading new data
   - Controlled by `clear_landing_table_flag` parameter (default: False)

### Data Processing Phase
3. **List S3 Objects**: Lists all available objects in the configured S3 bucket
   - Controlled by `list_objects_flag` parameter (default: True)

4. **Upload to S3**: Uploads the local Pokemon card data file to S3
   - Controlled by `upload_flag` parameter (default: True)

5. **Load Landing Data**: Processes raw Pokemon card CSV/Excel data into the landing table

6. **Load Lookup Tables**: Populates reference/dimension tables
   - Languages
   - Card Sets
   - Grading Companies
   - Grade Descriptions
   - Card Rarity Levels

7. **Load Card Data**: Inserts normalized card records into the main `pokemon.card` table

8. **Load Grade Data**: Processes and inserts card grading information into the `pokemon.card_grade` table

9. **Download from S3**: Optionally downloads processed files from S3 for archive/verification
   - Controlled by `download_flag` parameter (default: False)

### Optional Features (Currently Disabled)
- Seller data processing
- Purchase transaction processing

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

Many pipeline steps can be controlled using flags in the respective function calls in `main.py`:

```python
# Database model creation
create_model(create_model_flag=True)    # Set to False to skip model creation

# Landing table cleanup
clear_landing_table(clear_landing_table_flag=False)  # Set to True to clear existing data

# S3 operations
list_objects(list_objects_flag=True)
upload_file_to_s3(upload_flag=True)
download_file_from_s3(download_flag=False)  # Set to True to download processed files
```

## Database Schema

The pipeline uses two schemas:

- **pokemon_landing**: Intermediate schema for raw data ingestion
- **pokemon**: Main schema for processed data storage

Tables include:
- `card`: Pokemon card information
- `grade`: Grading/condition information
- `grade_company`: Grading company lookup
- `grade_description`: Grade description lookup
- `language`: Language lookup
- `set`: Pokemon set lookup
- `rarity`: Card rarity lookup
- `seller`: Seller information
- `card_grade`: Card-to-grade relationships
- `card_seller`: Card-to-seller relationships

## Configuration

### Environment Variables

All sensitive configurations (AWS credentials, database passwords) should be stored in a `.env` file:

```bash
DB_USER=your_db_user
DB_PASSWORD=your_db_password
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
S3_BUCKET_NAME=your-bucket-name
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