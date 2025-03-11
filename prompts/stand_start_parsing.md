## Overview

We want to add a new parsing function to the `jsnl_processor.py` script.  This function will parse the strand started message and store the metadata in the database.

When it comes across a strand start message it will parse the message and either create a new row in the database or update the existing row.
## Details

Rust code to produce the strand started message.
```rust
    let message = json!({
        "type": "strand_started",
        "strand_id": &strand_id,
        "config": &strand_config,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });
```

Rust code to define the StrategyConfig struct, which is the "config" field in the strand started message.  This will be parsed into a json object.
```rust
    pub struct StrategyConfig {
    // Metadata
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,

    // Strategy Configuration
    pub strategy_type: String,
    pub strategy_binary_key: String,
    pub trading_direction: String,

    // Execution settings
    pub simulated: bool,
    pub live: bool,

    // Data configuration
    pub feed: String,
    pub instrument: String,
    pub processor_type: String,
    pub granularity: String,
    pub pip_diff: Option<f64>,
    pub interval: Option<i64>,

    // Parameters
    #[serde(default)] // This tells serde to use the default if field is missing
    pub parameters: HashMap<String, ParameterType>,

    pub preamble_period: Option<i64>,
    pub preamble_candle_granularity: Option<i64>,
    // pub timeframe: String,
    // pub symbol: String,
}
```

Database schema for the dashboard_metadata table.
```sql
CREATE TABLE `dashboard_metadata` (
  `id` varchar(255) NOT NULL,
  `component` varchar(50) NOT NULL,
  `name` varchar(255) NOT NULL,
  `data` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`data`)),
  PRIMARY KEY (`id`),
  KEY `idx_component` (`component`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
```

## Implementation

### Data Structures and Modules

1. **Required Modules**:
   - `json`: For parsing JSON data from JSNL files
   - `mysql.connector`: For database connectivity
   - `logging`: For logging processing information and errors
   - `datetime`: For timestamp handling
   - `typing`: For type hints (Optional, Dict, List, etc.)

2. **Data Structures**:
   - `StrandStartedMessage`: A parsed JSON dictionary containing the strand_started message
     - `type`: String - "strand_started"
     - `strand_id`: String - Unique identifier for the strand
     - `config`: StrategyConfig - Configuration for the strategy
     - `timestamp`: String - RFC3339 formatted timestamp
   
   - `StrategyConfig`: A parsed JSON dictionary containing strategy configuration
     - Fields match the Rust struct definition above

### Data Flow

1. **Input**: JSNL file containing "strand_started" messages
2. **Processing Steps**:
   - Parse JSNL file line by line
   - Deserialize each line to JSON
   - Filter for "strand_started" message type
   - Extract strand_id, timestamp, and config data
   - Prepare data for database insertion/update
3. **Output**: Records in the `dashboard_metadata` table

### Data Mapping

| Source Field (JSNL) | Destination Field (Database) | Description |
|---------------------|------------------------------|-------------|
| `strand_id` | `id` | Primary identifier for the strand |
| `"strand"` (constant) | `component` | Fixed value to identify the component type |
| `config.name` | `name` | Name of the strategy from config |
| `config` | `data` (JSON) | Strategy configuration serialized as JSON |

### Function Implementation

```python
def process_strand_started(message, db_connection):
    """
    Process a strand_started message and store/update its metadata in the database.
    
    Args:
        message (dict): The parsed strand_started message
        db_connection: Database connection object
    
    Returns:
        bool: True if processing succeeded, False otherwise
    """
    try:
        # Extract relevant fields
        strand_id = message.get('strand_id')
        config = message.get('config', {})
        
        # Validate required fields
        if not strand_id:
            logging.error("Missing strand_id in strand_started message")
            return False
            
        # Prepare data for database
        strategy_name = config.get('name', 'Unnamed Strategy')
        
        # Store only the config as JSON string, not the entire message
        json_data = json.dumps(config)
        
        # Check if record already exists
        cursor = db_connection.cursor()
        cursor.execute(
            "SELECT id FROM dashboard_metadata WHERE id = %s AND component = 'strand'",
            (strand_id,)
        )
        record = cursor.fetchone()
        
        if record:
            # Update existing record
            cursor.execute(
                "UPDATE dashboard_metadata SET name = %s, data = %s WHERE id = %s AND component = 'strand'",
                (strategy_name, json_data, strand_id)
            )
            logging.info(f"Updated existing strand metadata for strand_id: {strand_id}")
        else:
            # Insert new record
            cursor.execute(
                "INSERT INTO dashboard_metadata (id, component, name, data) VALUES (%s, %s, %s, %s)",
                (strand_id, 'strand', strategy_name, json_data)
            )
            logging.info(f"Inserted new strand metadata for strand_id: {strand_id}")
        
        db_connection.commit()
        return True
        
    except Exception as e:
        logging.error(f"Error processing strand_started message: {str(e)}")
        if db_connection:
            db_connection.rollback()
        return False
```

### Integration into JSNL Processor

The process_strand_started function should be integrated into the main JSNL processor's message handling logic:

```python
def process_jsnl_line(line, db_connection):
    """Process a single line from the JSNL file."""
    try:
        message = json.loads(line)
        message_type = message.get('type')
        
        if message_type == 'strand_started':
            return process_strand_started(message, db_connection)
        # Process other message types...
        
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON line: {line}")
        return False
    except Exception as e:
        logging.error(f"Error processing line: {str(e)}")
        return False
```

### Error Handling and Validation

The implementation includes:

1. **Input Validation**:
   - Check for required fields like strand_id
   - Validate message type is "strand_started"

2. **Error Handling**:
   - Try-except blocks around database operations
   - Transaction rollback on failures
   - Detailed error logging

3. **Database Integrity**:
   - Check for existing records before inserting
   - Update if record exists, insert if it doesn't
   - Use transactions to ensure atomicity

### Testing Strategy

To verify the functionality:

1. **Unit Testing**:
   - Create sample strand_started messages with various configurations
   - Test the process_strand_started function with these samples
   - Verify database state after processing

2. **Integration Testing**:
   - Create a test JSNL file with strand_started messages
   - Process the file using the complete JSNL processor
   - Verify database records match expected output

3. **Edge Cases**:
   - Missing required fields
   - Malformed JSON
   - Duplicate strand_id values
   - Very large config objects