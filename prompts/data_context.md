## Data Context and Data Mapping

### Sample jsnl record
```json
{"log_id": "0xf0afc79f6bf34e76", "timestamp": 1740743175.9850,  "component": "strand", "type": "real_time_strategy", "value": [{"candle":{"close":20622.2,"high":20629.25,"low":20622.2,"n":304,"open":20628.4,"timestamp":1740743175.985,"volume":73},"t":"c"},{"c":"green","d":"[0] local high","l":{"close":21752.6,"high":21754.1,"low":21750.2,"n":623,"open":21751.3,"timestamp":1740389040.0,"volume":124},"t":"l","w":2.0},{"c":"red","d":"[0] local low","l":{"close":20479.5,"high":20484.3,"low":20476.8,"n":1937,"open":20482.4,"timestamp":1740714540.0,"volume":219},"t":"l","w":2.0},{"a":"1.6418e0","b":"2.0131e4","c":"#7900f2","d":"[0] trend lr","n":"3.0400e2","p":10,"q":"Weak trend","r2":"4.1641e-1","s":2.0,"t":"lrf"},{"a":"2.1913e-2","b":"2.1138e4","c":"red","d":"[0] down run","dm":"-7.4349e2","mn":"2.0480e4","mx":"2.1753e4","n":5630,"s":2.0,"t":"lr","um":"2.5673e2"},{"a":"-1.0133e-2","b":"2.0590e4","c":"green","d":"[0] up run","dm":"-4.4672e1","mn":"2.0480e4","mx":"2.0639e4","n":446,"s":2.0,"t":"lr","um":"6.4765e1"},{"c":"blue","n":"[0] sliding window","st":1740743175.985,"sv":20619.27111111106,"t":"sr","w":2.0},{"cg":"trending info up","col":"","diag":[["adx","6.029"],["vix","3.304"],["atr","7.167"]],"dir":"up","id":"abc","res":true,"rsn":"Trending values","t":"cg","tn":"info","tr":-1.0},{"cg":"trending exit up","col":"","diag":[["atr","7.167"],["price","20622.200"],["sliding_window","20619.271"],["stop_loss_deviation_factor","9.487"],["deviation","2.929"],["stop_loss_deviation_threshold","-67.997"]],"dir":"up","id":"abc","res":false,"rsn":"Exit position up - Stop loss (related to ATR)","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"trending exit up","col":"","diag":[["atr","7.167"],["price","20622.200"],["sliding_window","20619.271"],["take_profit_deviation_factor","7.166"],["deviation","2.929"],["take_profit_deviation_threshold","51.360"]],"dir":"up","id":"abc","res":false,"rsn":"Exit position up - Take profit (related to ATR)","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"trending exit up","col":"","diag":[["m","true"],["d","53012.90"]],"dir":"up","id":"abc","res":false,"rsn":"Trading not allowed - During market hours only","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"trending enter up","col":"","diag":[["adx_threshold","25.000"],["adx","6.029"]],"dir":"up","id":"abc","res":true,"rsn":"Long entry - Low ADX","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"trending enter up","col":"","diag":[["atr","7.167"],["entry_deviation_threshold","14.846"],["deviation","2.929"],["entry_deviation_factor","2.071"]],"dir":"up","id":"abc","res":false,"rsn":"Entry - Deviation from mean","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"trending enter up","col":"","diag":[],"dir":"up","id":"abc","res":true,"rsn":"Trade entry - In Market Time","t":"cg","tn":"part_pass","tr":-1.0},{"b":"mean_reversion_paper","equity":"22.6000","t":"e"}]}
```

Note:
- The timestamp is the timestamp of the candle.
- The component is the component that generated the log.
- The "type" field is the mode of the log.
- The "log_id" field is the id of the log.
- The "value" field is the value of the log and is an array of objects that contains an embedded candle object and an embedded equity object.
- Each object is identified by the "t" field.
- The candle object is identified by "t":"c"
- The equity object is identified by "t":"e"

#### Examples of trading events

These events are embedded in the value array of the log object, and are identified by the "t" field.
- Open: {"instrument":"NAS100_USD","price":21324.749999999996,"profit":0.0,"t":"open"}
- Close: {"instrument":"NAS100_USD","price":21185.95,"profit":-133.29999999999637,"t":"close"}
- Adjust-Close: {"instrument":"NAS100_USD","price":21185.95,"profit":-133.29999999999637,"t":"adjust-close"}
- Adjust-Open: {"instrument":"NAS100_USD","price":21157.9,"profit":0.0,"t":"adjust-open"}

### Database schema for 'trading' database:

#### dashboard_data table
The dashboard_data table is used to store the data for the dashboard.  In our case all of the trading events are stored in this table.
```sql
CREATE TABLE `dashboard_data` (
  `timestamp` double NOT NULL,
  `id` varchar(255) NOT NULL,
  `mode` varchar(255) NOT NULL,
  `data_type` varchar(255) NOT NULL,
  `json_data` text NOT NULL,
  PRIMARY KEY (`id`,`timestamp`,`mode`,`data_type`),
  KEY `idx_timestamp_id` (`timestamp`,`id`),
  KEY `idx_timestamp_id_mode` (`timestamp`,`id`,`mode`),
  KEY `idx_timestamp_id_mode_type` (`timestamp`,`id`,`mode`,`data_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
```
Note that "id" is the log_id, "mode" is the type.
The "data_type" is the type "t" of the value array in the log object with "t":"open" or "t":"close" or "t":"adjust-close" or "t":"adjust-open".

#### dashboard_equity table
The dashboard_equity table is used to store the equity data for the dashboard.

```sql
CREATE TABLE `dashboard_equity` (
  `id` varchar(255) NOT NULL,
  `timestamp` double NOT NULL,
  `mode` varchar(255) NOT NULL,
  `equity` double NOT NULL,
  `candle` varchar(700) DEFAULT NULL,
  PRIMARY KEY (`id`,`timestamp`,`mode`),
  KEY `idx_timestamp_id` (`timestamp`,`id`),
  KEY `idx_timestamp_id_mode` (`timestamp`,`id`,`mode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
```

Note that "id" is the log_id, "mode" is the type,
The "equity" field is the equity value in found in the value array of the log object with "t":"e".
The "candle" field is the candle object in the value array of the log object with "t":"c".

## Data Mapping Between JSNL, Python Objects, and Database Tables

The `jsnl_processor.py` script processes JSNL (JSON Lines) files containing trading data and maps them to database tables. Here's how the data structures map between the JSON format and database tables:

### 1. JSNL Record to Python Objects

The JSNL processor extracts the following key fields from each JSON record:

| JSNL Field | Python Variable | Description |
|------------|-----------------|-------------|
| `log_id` | `log_id` | Unique identifier for the log entry |
| `timestamp` | `timestamp` | Unix timestamp of the record |
| `component` | `component` | Component that generated the log (e.g., "strand") Note that this is currently not used in the processing or mapped to the database tables |
| `type` | `mode` | Mode of the log (e.g., "real_time_strategy") |
| `value` | `value_obj` | Array or object containing the actual data |

### 2. Value Objects to Database Records

The `value` field contains an array of objects, each identified by a `t` field that determines its type:

| Value Type (`t`) | Description | Database Table | Processing Function |
|------------------|-------------|----------------|---------------------|
| `c` | Candle data | `dashboard_equity` (as JSON in `candle` field) | Stored in `candles` dictionary |
| `e` | Equity data | `dashboard_equity` | `store_equity()` |
| `open` | Trade open event | `dashboard_trades` | `store_trade()` |
| `close` | Trade close event | `dashboard_trades` | `store_trade()` |
| `adjust-open` | Adjusted trade open | `dashboard_trades` | `store_trade()` |
| `adjust-close` | Adjusted trade close | `dashboard_trades` | `store_trade()` |

### 3. Database Table Mappings

#### a. `dashboard_equity` Table

| JSNL Field | Database Column | Source in Code |
|------------|-----------------|----------------|
| `log_id` | `id` | `record['log_id']` |
| `timestamp` | `timestamp` | `record['timestamp']` |
| `type` | `mode` | `record['mode']` |
| `value[].equity` (where `t`=`e`) | `equity` | `record['value'].get('equity', 0.0)` |
| `value[].candle` (where `t`=`c`) | `candle` (JSON string) | `json.dumps(record['value']['candle'])` |

#### b. `dashboard_data` Table is used to store all the trade data

| JSNL Field | Database Column | Source in Code |
|------------|-----------------|----------------|
| `log_id` | `id` | `trade_data['log_id']` |
| `timestamp` | `timestamp` | `trade_data['timestamp']` |
| `value[].t` (where `t` in trade types) | `data_type` | `trade_data['type']` |
| `type` | `mode` | `trade_data['mode']` |
| `value[].instrument` | `instrument` | `value_item.get('instrument', '')` |
| `value[].price` | `price` | `value_item.get('price', 0.0)` |
| `value[].profit` | `profit` | `value_item.get('profit', 0.0)` |

### 4. Parquet File Structure

The processor also creates Parquet files with the following structure:

| Field | Description | Source |
|-------|-------------|--------|
| `log_id` | Unique identifier | From JSNL `log_id` |
| `timestamp` | Unix timestamp | From JSNL `timestamp` |
| `mode` | Mode of the log | From JSNL `type` |
| `component` | Component name | From JSNL `component` |
| `filename` | Source filename | Generated during processing |
| `data` | Raw JSON data | Original JSNL line |
| `date` | Date string (YYYY-MM-DD) | Derived from `timestamp` |

### Processing Flow

1. The processor reads JSNL files line by line
2. For each line, it extracts the common fields (`log_id`, `timestamp`, `component`, `type`, and `value`)
3. It processes the `value` array, identifying different record types by their `t` field
4. Candle records (`t`=`c`) are stored in a temporary dictionary
5. Equity records (`t`=`e`) are associated with their corresponding candle data and stored in the `dashboard_equity` table
6. Trade records (open/close events) are stored directly in the database as `dashboard_data` records
7. All records are also stored in Parquet files for efficient querying

### Key Data Structures

1. `equity_records_data`: Dictionary mapping `(log_id, timestamp)` tuples to equity records
2. `candles`: Dictionary mapping `(log_id, timestamp)` tuples to candle data
3. `records`: List of processed records for Parquet file creation

### Parquet File Organization

The processor organizes Parquet files in a hierarchical structure:

1. **Temporary Files**: Individual Parquet files created from each JSNL file
   - Location: `{PARQUET_DIR}/temp/`
   - Naming: `{original_filename}_{file_hash}.parquet`

2. **Weekly Files**: Merged files containing data for a full week
   - Location: `{PARQUET_DIR}/weekly/`
   - Naming: `weekly_{YYYYMMDD}.parquet` (where date is the start of the week)

This mapping ensures that all relevant trading data from the JSNL files is properly extracted, associated, and stored in both the relational database (for operational use) and Parquet files (for analytical queries).

