# TinyETL

![Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen)

A tiny ETL tool for moving data between sources with automatic schema inference and minimal configuration.

## Overview

TinyETL is designed to make data movement simple and efficient. Point it at a source and target, and it will automatically detect the schema, create necessary tables, and transfer your data in optimized batches.

### Key Features

- **Automatic Schema Inference**: Detects column names and data types automatically
- **Multiple Connectors**: Support for CSV, JSON, and SQLite (with Postgres coming soon)
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
  -h, --help                     Print help
  -V, --version                  Print version
```

Basic usage examples:

```bash
# Transfer CSV to SQLite database
tinyetl data.csv output.db

# Preview first 10 rows and inferred schema
tinyetl data.csv output.db --preview 10

# Transfer with custom batch size
tinyetl data.csv output.db --batch-size 5000

# Dry run to validate without transferring
tinyetl data.csv output.db --dry-run
```

### Supported Data Sources

**Sources:**
- CSV files
- JSON files (array of objects)
- SQLite databases

**Targets:**
- CSV files
- SQLite databases
- PostgreSQL databases (optional)

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--infer-schema` | Auto-detect columns and types | enabled |
| `--batch-size <BATCH_SIZE>` | Number of rows per batch | 10000 |
| `--preview <N>` | Show first N rows and inferred schema without copying | - |
| `--dry-run` | Validate source/target without transferring data | disabled |
| `--log-level <LOG_LEVEL>` | Log level: info, warn, error | info |
| `--skip-existing` | Skip rows already in target if primary key detected | disabled |
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
```

### Sample Output

```
→ Connecting to source: sales.csv
→ Inferring schema...
→ 8 columns detected
→ Connecting to target: local.db#sales
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
- Batch processing
- Basic CLI interface

**Future Enhancements:**
- MySQL and PostgreSQL connectors
- Data transformations (rename, cast, filter)
- Multi-file processing with glob patterns
- Configuration file support
- Advanced schema mapping

---

Built with Rust for performance, safety, and reliability.
