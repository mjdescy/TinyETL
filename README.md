# TinyETL

![Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen)

A tiny ETL tool for moving data between sources with automatic schema inference and minimal configuration.

## Overview

TinyETL is designed to make data movement simple and efficient. Point it at a source and target, and it will automatically detect the schema, create necessary tables, and transfer your data in optimized batches.

### Key Features

- **Automatic Schema Inference**: Detects column names and data types automatically
- **Data Transformations**: Transform data during transfer using Lua scripting
- **Multiple Connectors**: Support for CSV, JSON, Parquet, SQLite, Postgres (with more coming soon)
- **Batch Processing**: Efficient streaming with configurable batch sizes
- **Progress Monitoring**: Real-time progress bars and transfer statistics
- **Preview Mode**: Inspect data and schema without transferring
- **Cross-Platform**: Works on Linux, macOS, and Windows

## For Users

### Installation

Download the latest release for your platform:

- [Latest Release](https://github.com/yourusername/tinyetl/releases/latest)

Or install from source with Rust:

```bash
cargo install tinyetl
```

### Quick Start

```
A tiny ETL tool for moving data between sources

Usage: tinyetl [OPTIONS] <SOURCE> <TARGET>

Arguments:
  <SOURCE>  Source connection string (file path or connection string)
  <TARGET>  Target connection string (file path or connection string)

Options:
      --infer-schema             Auto-detect columns and types
      --batch-size <BATCH_SIZE>  Number of rows per batch [default: 10000]
      --preview <N>              Show first N rows and inferred schema without copying
      --dry-run                  Validate source/target without transferring data
      --log-level <LOG_LEVEL>    Log level: info, warn, error [default: info]
      --skip-existing            Skip rows already in target if primary key detected
      --transform-file <FILE>    Path to Lua file containing a 'transform' function
      --transform <EXPRESSIONS>  Inline transformation expressions (semicolon-separated)
  -h, --help                     Print help
  -V, --version                  Print version
```

Basic usage examples:

```bash
# Transfer CSV to SQLite database
tinyetl data.csv output.db

# Convert CSV to Parquet
tinyetl data.csv output.parquet

# Load CSV into PostgreSQL
tinyetl data.csv "postgresql://user:pass@localhost/mydb#customers"

# Preview first 10 rows and inferred schema
tinyetl data.csv output.db --preview 10

# Transfer with custom batch size
tinyetl data.csv output.db --batch-size 5000

# Dry run to validate without transferring
tinyetl data.csv output.db --dry-run

# Apply inline transformations
tinyetl data.csv output.db --transform "full_name=row.first_name .. ' ' .. row.last_name; age_next_year=row.age + 1"

# Apply transformations from Lua file
tinyetl data.csv output.db --transform-file transform.lua
```

### Supported Data Sources

**Sources:**
- CSV files
- JSON files (array of objects)
- Parquet files
- SQLite databases
- PostgreSQL databases

**Targets:**
- CSV files
- JSON files
- Parquet files
- SQLite databases
- PostgreSQL databases

### Data Transformations

TinyETL supports powerful data transformations using Lua scripting during the ETL process. Transform, combine, filter, and modify data as it flows from source to target.

#### Using Inline Transformations

Use the `--transform` option with semicolon-separated expressions. **All original columns are automatically preserved**, and you can add new columns or override existing ones:

```bash
# Add a new column while keeping all existing columns
tinyetl users.csv output.db --transform "full_name=row.first_name .. ' ' .. row.last_name"

# Multiple transformations (original columns + new calculated columns)
tinyetl sales.csv output.db --transform "total=row.quantity * row.price; profit=total * 0.3; year=2024"

# Override existing columns and add new ones
tinyetl data.csv output.db --transform "email=string.lower(row.email); age_group=row.age < 30 and 'young' or 'mature'"
```

#### Using Lua Files

For complex transformations, create a Lua file with a `transform` function. **Note**: With Lua files, you have full control over which columns to include - only columns explicitly returned are kept:

**transform.lua:**
```lua
function transform(row)
    -- Create result table
    local result = {}
    
    -- Explicitly copy fields you want to keep
    result.id = row.id
    result.created_at = row.created_at
    
    -- Create new calculated fields
    result.full_name = row.first_name .. ' ' .. row.last_name
    result.total_amount = row.quantity * row.unit_price
    result.discount = result.total_amount > 100 and result.total_amount * 0.1 or 0
    result.final_amount = result.total_amount - result.discount
    
    -- String processing
    if row.email then
        result.email_domain = row.email:match('@(.+)')
        result.is_business_email = result.email_domain:find('%.com$') and true or false
    end
    
    -- Date manipulation (dates come as RFC3339 strings)
    if row.birth_date then
        local year = tonumber(row.birth_date:match('^(%d%d%d%d)'))
        result.age = 2024 - year
        result.generation = year < 1980 and 'Gen X' or year < 1997 and 'Millennial' or 'Gen Z'
    end
    
    return result
end
```

```bash
tinyetl data.csv output.db --transform-file transform.lua
```

#### Transformation Rules

1. **Schema Inference**: The output schema is determined by the first transformed row
2. **Column Preservation**: For inline expressions, all original columns are preserved by default
3. **Column Override**: Transformations can override existing columns with new values
4. **New Columns**: New columns returned by transform are added to the target schema  
5. **Column Filtering**: For Lua files, only columns returned by the transform function are kept
6. **Type Safety**: Lua values are automatically converted to appropriate SQL types
7. **Error Handling**: Transformation errors stop the process with clear error messages

#### Available Data Types

- **Strings**: `"text"` or `'text'`
- **Numbers**: `42`, `3.14` (integers become INTEGER, decimals become REAL)
- **Booleans**: `true`, `false`
- **Null**: `nil` (becomes NULL in target)
- **Dates**: Input as RFC3339 strings, can be manipulated as strings

#### Lua Built-ins Available

- String functions: `string.find()`, `string.match()`, `string.gsub()`, concatenation with `..`
- Math functions: `math.floor()`, `math.ceil()`, `math.abs()`, basic operators
- Logic: `and`, `or`, `not`, conditional expressions
- Pattern matching with string methods

#### Examples

```bash
# Clean and standardize phone numbers
tinyetl contacts.csv clean_contacts.db --transform "clean_phone=row.phone:gsub('[^%d]', '')"

# Category mapping
tinyetl products.csv categorized.db --transform "category=row.price < 50 and 'budget' or row.price < 200 and 'standard' or 'premium'"

# Extract year from date and calculate age
tinyetl people.csv processed.db --transform "birth_year=tonumber(row.birth_date:match('^(%d%d%d%d)')); age=2024 - birth_year"

# Preview transformations before applying
tinyetl data.csv output.db --transform "total=row.qty * row.price" --preview 5
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--infer-schema` | Auto-detect columns and types | enabled |
| `--batch-size <BATCH_SIZE>` | Number of rows per batch | 10000 |
| `--preview <N>` | Show first N rows and inferred schema without copying | - |
| `--dry-run` | Validate source/target without transferring data | disabled |
| `--log-level <LOG_LEVEL>` | Log level: info, warn, error | info |
| `--skip-existing` | Skip rows already in target if primary key detected | disabled |
| `--transform-file <FILE>` | Path to Lua file containing a 'transform' function | - |
| `--transform <EXPRESSIONS>` | Inline transformation expressions (semicolon-separated) | - |
| `-h, --help` | Print help | - |
| `-V, --version` | Print version | - |

### Examples

```bash
# Basic CSV to SQLite transfer
tinyetl sales_data.csv analytics.db

# JSON array to CSV conversion
tinyetl user_data.json users.csv

# Preview large dataset before transfer
tinyetl large_dataset.csv target.db --preview 5

# Transfer with verbose logging
tinyetl source.csv target.db --log-level info

# Validate connections without transferring
tinyetl remote_data.csv local.db --dry-run

# Apply transformations during transfer
tinyetl raw_sales.csv processed.db --transform "revenue=row.quantity * row.price; profit_margin=revenue * 0.3"

# Complex data cleaning with Lua file
tinyetl messy_data.csv clean.db --transform-file cleanup.lua --preview 3

# Load Parquet files to PostgreSQL
tinyetl large_dataset.parquet "postgresql://user:pass@localhost/db#table"

# Convert CSV to Parquet format
tinyetl data.csv output.parquet

# Transfer between PostgreSQL databases
tinyetl "postgresql://source_db/table" "postgresql://target_db/new_table"

# JSON to Parquet with transformations
tinyetl raw_events.json analytics.parquet --transform "event_date=row.timestamp:match('^[^T]+'); user_id=tonumber(row.id)"
```

### Sample Output

```
→ Connecting to source: sales.csv
→ Inferring schema...
→ 8 columns detected
→ Transformation enabled
→ Connecting to target: local.db#sales
→ Schema updated by transformations: 12 columns
→ Copying 100,000 rows
████████████ 100% (145k rows/sec)
→ Done in 2.3s
```

### Performance Goals

- Handle datasets up to 5 million rows efficiently
- Maintain low memory footprint through streaming
- Achieve transfer speeds of 40k+ rows per second for typical datasets
- Cross-platform compatibility (Linux, macOS, Windows)

### License

```
“You may use this software under the terms of the Apache 2.0 License.
If you wish to include TinyETL in a commercial SaaS or hosted product, please contact licensing@tinyetl.com"
```
### Roadmap

**MVP (Current Focus):**
- Core CSV, JSON, SQLite connectors
- Schema inference
- Lua-based data transformations
- Batch processing
- Basic CLI interface

**Future Enhancements:**
- MySQL and PostgreSQL connectors
- Advanced transformation functions and libraries
- Multi-file processing with glob patterns
- Configuration file support
- Advanced schema mapping
- Data validation and quality checks

---

Built with Rust for performance, safety, and reliability.
