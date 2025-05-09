# Live Data Processing Implementation Plan

## Overview
This document outlines the changes needed to support processing of live trading data files. The main differences in live data processing are:
1. Configuration may appear anywhere in the file, we will assume that the config has been processed prior to any other lines appearing.  Any line that we can not identify the mode configuration for will be ignored and a warning will be logged.
2. New configurations need to be stored in the database
3. Unknown mode instances need their config retrieved from database if it exists.
4. Equity data is written directly to final table instead of temporary table, but only if live is true
5. Partition creation based on the mode value for equity data if the partition does not already exist

## Required Changes

### 1. Configuration Management

#### Modified Files:
- `processor/__init__.py`
  - Add mode configuration cache to `JSNLProcessor` class
  - Modify `process_strand_started` to handle live configurations
  - Add method to handle the line type=="strand_started" which can be found anywhere in the file
  - Add method to retrieve mode config from database if it not scanned from the file.

#### New Files:
- `processor/config_cache.py`
  - Implement `ModeConfigCache` class to manage mode configurations
  - Methods:
    - `get_config(mode: str) -> Optional[Dict]`
    - `add_config(mode: str, config: Dict) -> None`
    - `update_database(mode: str, config: Dict) -> None`
    - `load_from_database(mode: str) -> Optional[Dict]`

### 2. Database Access

#### Modified Files:
- `db/__init__.py`
  - Add methods to `DatabaseHandler` class:
    - `get_mode_config(mode: str) -> Optional[Dict]`
    - `store_mode_config(mode: str, config: Dict) -> None`
    - `ensure_partition_exists(mode: str) -> None`
    - Modify `store_equity` to handle live data
    - Modify `store_equity_batch` to handle live data

### 3. File Processing

#### Modified Files:
- `processor/__init__.py`
  - Modify `_process_file_contents` to:
    - Handle live data differently
    - Use config cache for mode lookups
  - Modify `_process_equity_records` to:
    - Write directly to final table for live data
    - Ensure partition exists before writing to the equity table

### 4. Data Flow Changes

1. Initial File Processing:
   - Scan entire file for configuration
   - Store new configurations in database
   - Cache configurations for future use

2. Live Data Processing:
   - Check if mode is live
   - Ensure partition exists
   - Write equity directly to final table
   - Process other data as normal

3. Configuration Management:
   - Cache configurations in memory
   - Update database with new configurations
   - Retrieve unknown configurations from database

## Implementation Phases

### Phase 1: Configuration Management
1. Implement `ModeConfigCache` class
2. Add configuration scanning to file processor
3. Add database methods for config management

### Phase 2: Live Data Processing
1. Modify equity processing for live data
2. Implement partition management
3. Add direct writing to final table

### Phase 3: Testing and Integration
1. Add tests for live data processing
2. Add integration tests
3. Add documentation

## Questions and Information Needed

1. Database Schema:
   - Need schema for final equity table
   - Need partition management details
   - Need any additional indexes or constraints

2. File Format:
   - Need example of equity data format
   - Need example of other data types in live files

3. Performance Requirements:
   - Expected file sizes
   - Processing time requirements
   - Cache size limits

4. Error Handling:
   - How to handle missing configs
   - How to handle partition creation failures
   - How to handle database connection issues

## Dependencies

1. Database:
   - Need access to `trading_instances` table
   - Need partition management capabilities

2. File System:
   - Need read access to live data files
   - Need write access to database

3. Memory:
   - Need to implement config cache with size limits
   - Need to handle large file processing

## Next Steps

1. Review and approve implementation plan
2. Provide required schema and format information
3. Begin implementation of Phase 1
4. Regular review of progress and adjustments as needed



