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
├── aws_s3_utils.py                  # AWS S3 client and operations
├── card.py                          # Card data processing logic
├── grade.py                         # Grade data processing logic
├── landing/
│   └── pokemon_cards_landing.py     # Landing layer data processing
├── lookup/
│   └── lookup_values_load.py        # Lookup table population
├── model_references/
│   └── model.sql                    # Database schema definitions
└── files/                           # Local data files directory
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

The `run_pipeline()` function executes the following steps:

```
1. List objects in S3 bucket
2. Upload local file to S3
3. Process landing layer data
4. Insert lookup table data:
   - Languages
   - Sets
   - Grading companies
   - Grade descriptions
   - Rarity levels
5. Insert card data
6. Insert grade data
7. Download processed file from S3 (optional)
```

## Usage

Run the complete pipeline:
```bash
python main.py
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