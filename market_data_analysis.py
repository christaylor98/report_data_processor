"""
Market data analysis script for identifying gaps and consistent 5-second intervals in financial market data.
Analyzes the rawprice table to find data gaps on weekdays and periods with consistent data sampling.
"""
#pylint: disable=W1203, W0718, C0301, C0303
import os
from datetime import datetime, timedelta
import argparse
import pytz

import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import seaborn as sns
# import numpy as np
# from matplotlib.dates import DateFormatter
# from urllib.parse import quote_plus

# Load environment variables
load_dotenv()
print("Environment variables loaded")

# Database connection parameters
db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': 'financedb' 
}
print(f"Database config prepared: host={db_config['host']}, user={db_config['user']}, database={db_config['database']}")

def connect_to_db():
    """Establish connection to the database"""
    print("Attempting to connect to database...")
    try:
        # Add connection timeout
        conn = mysql.connector.connect(
            **db_config,
            connection_timeout=60,  # 60 seconds timeout
            buffered=True  # Use buffered cursors
        )
        print("Database connection successful")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None

def fetch_data_since_2025():
    """Fetch data from rawprice table since January 1, 2025"""
    print("Starting fetch_data_since_2025 function")
    conn = connect_to_db()
    if not conn:
        return None
    
    cursor = conn.cursor(dictionary=True)
    print("Database cursor created")
    
    # Convert Jan 1, 2025 to Unix timestamp
    start_date = int(datetime(2025, 1, 1).timestamp())
    print(f"Start date timestamp: {start_date} ({datetime.fromtimestamp(start_date)})")
    
    # First, check for the latest record to verify February data exists
    check_query = """
    SELECT MAX(time) as max_time, FROM_UNIXTIME(MAX(time)) as max_date
    FROM rawprice
    """
    
    cursor.execute(check_query)
    max_result = cursor.fetchone()
    print(f"Latest record in database: {max_result['max_time']} ({max_result['max_date']})")
    
    # Get a count of records to understand the data size
    count_query = """
    SELECT COUNT(*) as record_count
    FROM rawprice
    WHERE time >= %s
    """
    
    try:
        print("Checking record count...")
        cursor.execute(count_query, (start_date,))
        count_result = cursor.fetchone()
        total_records = count_result['record_count']
        print(f"Total records to fetch: {total_records:,}")
        
        if total_records == 0:
            print("No data available for the specified period.")
            return None
        
        # Use record-based batching with LIMIT and OFFSET
        print("Using record-based batching with LIMIT and OFFSET...")
        
        batch_size = 500000  # Number of records per batch
        all_data = []
        
        # Calculate number of batches needed
        num_batches = (total_records + batch_size - 1) // batch_size  # Ceiling division
        print(f"Will fetch data in {num_batches} batches of {batch_size:,} records each")
        
        for batch_num in range(num_batches):
            offset = batch_num * batch_size
            
            batch_query = """
            SELECT time, nanoseconds, instrument
            FROM rawprice
            WHERE time >= %s
            ORDER BY time ASC, nanoseconds ASC
            LIMIT %s OFFSET %s
            """
            
            print(f"Fetching batch {batch_num+1}/{num_batches} (offset: {offset:,})")
            cursor.execute(batch_query, (start_date, batch_size, offset))
            batch_data = cursor.fetchall()
            
            all_data.extend(batch_data)
            records_so_far = len(all_data)
            
            print(f"Fetched {len(batch_data):,} records in batch {batch_num+1}")
            print(f"Progress: {records_so_far:,}/{total_records:,} records ({(records_so_far/total_records*100):.1f}%)")
        
        data = all_data
        print(f"Total records fetched: {len(data):,}")
        
        if not data:
            print("No data returned from query")
            return None
            
        print("Converting to DataFrame...")
        df = pd.DataFrame(data)
        
        # Convert Unix timestamp to datetime
        print("Converting timestamps to datetime...")
        df['datetime'] = pd.to_datetime(df['time'], unit='s')
        
        # Add nanoseconds to datetime (convert to microseconds for pandas)
        df['datetime'] = df['datetime'] + pd.to_timedelta(df['nanoseconds'] / 1000, unit='us')
        print(f"DataFrame created with {len(df):,} rows and columns: {df.columns.tolist()}")
        
        if not df.empty:
            print(f"Data range: {df['datetime'].min()} to {df['datetime'].max()}")
            
            # Verify if February data is included
            feb_data = df[df['datetime'].dt.month == 2]
            if not feb_data.empty:
                print(f"February data found: {len(feb_data):,} records from {feb_data['datetime'].min()} to {feb_data['datetime'].max()}")
            else:
                print("No February data found in the fetched records, despite being in the database!")
        
        return df
    except mysql.connector.Error as err:
        print(f"Error fetching data: {err}")
        return None
    finally:
        print("Closing cursor and connection")
        cursor.close()
        conn.close()

def analyze_time_gaps(df):
    """Analyze gaps in the data that are not on weekends"""
    print("Starting analyze_time_gaps function")
    if df is None or df.empty:
        print("No data available for analysis")
        return
    
    print("Sorting data by datetime")
    # Sort by datetime to ensure proper gap analysis
    df = df.sort_values('datetime')
    
    print("Calculating time differences between records")
    # Calculate time difference between consecutive records
    df['time_diff'] = df['datetime'].diff().dt.total_seconds()
    
    # First row will have NaN time_diff, replace with 0
    df['time_diff'] = df['time_diff'].fillna(0)
    
    # Define what constitutes a gap (more than 4 hours)
    gap_threshold = 4 * 60 * 60  # 4 hours in seconds
    print(f"Using gap threshold of {gap_threshold} seconds ({gap_threshold/3600} hours)")
    
    # Find gaps
    print("Finding gaps exceeding threshold")
    gaps = df[df['time_diff'] > gap_threshold].copy()
    print(f"Found {len(gaps)} total gaps > 4 hours")
    
    # Add previous timestamp for reference
    print("Calculating previous timestamps for gaps")
    gaps['prev_datetime'] = gaps['datetime'] - pd.to_timedelta(gaps['time_diff'], unit='s')
    
    # Calculate gap duration in days
    gaps['gap_days'] = gaps['time_diff'] / (24 * 60 * 60)
    
    # Filter out weekend gaps (gaps that start after Friday close and end before Monday open)
    print("Filtering out normal weekend gaps")
    weekday_gaps = []
    
    for idx, row in gaps.iterrows():
        gap_start = row['prev_datetime']
        gap_end = row['datetime']
        gap_days = row['gap_days']
        
        # Convert to Eastern time for market hours analysis
        gap_start_eastern = gap_start.tz_localize('UTC').tz_convert('US/Eastern')
        gap_end_eastern = gap_end.tz_localize('UTC').tz_convert('US/Eastern')
        
        # Check if this is a normal weekend gap (Friday to Monday)
        is_normal_weekend_gap = (
            # Gap starts on Friday
            (gap_start_eastern.dayofweek == 4 and gap_start_eastern.hour >= 16) and
            # Gap ends on Monday
            (gap_end_eastern.dayofweek == 0 and gap_end_eastern.hour < 9.5) and
            # Gap is less than 3 days (a normal weekend)
            gap_days < 3.0
        )
        
        # Always include gaps longer than 3 days, regardless of when they start/end
        if gap_days >= 3.0 or not is_normal_weekend_gap:
            weekday_gaps.append(row)
    
    weekday_gaps = pd.DataFrame(weekday_gaps)
    
    print(f"Found {len(weekday_gaps)} gaps > 4 hours during market hours or extended gaps > 3 days")
    
    if not weekday_gaps.empty:
        print("Processing weekday gaps")
        weekday_gaps['gap_duration'] = weekday_gaps['time_diff']
        weekday_gaps['gap_duration_formatted'] = weekday_gaps['gap_duration'].apply(
            lambda x: str(timedelta(seconds=int(x)))
        )
        
        # Display gaps sorted by duration (largest first)
        print("Sorting gaps by duration")
        sorted_gaps = weekday_gaps.sort_values('gap_duration', ascending=False)
        
        print("\nTop 10 largest gaps:")
        for _idx, row in sorted_gaps.head(10).iterrows():
            print(f"Gap of {row['gap_duration_formatted']} ({row['gap_days']:.1f} days) between "
                  f"{row['prev_datetime'].strftime('%Y-%m-%d %H:%M:%S')} and "
                  f"{row['datetime'].strftime('%Y-%m-%d %H:%M:%S')} "
                  f"for instrument {row['instrument']}")
        
        # Plot gap distribution
        print("Creating gap distribution plot")
        plt.figure(figsize=(12, 6))
        plt.hist(weekday_gaps['gap_days'], bins=50, alpha=0.7)  # Show in days
        plt.xlabel('Gap Duration (days)')
        plt.ylabel('Frequency')
        plt.title('Distribution of Data Gaps')
        plt.grid(True, alpha=0.3)
        plt.savefig('gap_distribution.png')
        plt.close()
        print("Gap distribution plot saved")
        
        # Plot gaps over time
        print("Creating gaps over time plot")
        plt.figure(figsize=(15, 7))
        plt.scatter(weekday_gaps['datetime'], weekday_gaps['gap_days'],  # Show in days
                   alpha=0.6, s=20, c='red')
        plt.xlabel('Date')
        plt.ylabel('Gap Duration (days)')
        plt.title('Data Gaps Over Time')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('gaps_over_time.png')
        plt.close()
        print("Gaps over time plot saved")
        
        return weekday_gaps
    else:
        print("No significant gaps found during market hours.")
        return pd.DataFrame()

def find_consistent_5sec_periods(df, window_size=100, tolerance=0.5):
    """
    Find periods where records are consistently spaced about 5 seconds apart
    
    Parameters:
    - df: DataFrame with the data
    - window_size: Number of consecutive records to consider as a consistent period
    - tolerance: Allowed deviation from 5 seconds (in seconds)
    """
    print(f"Starting find_consistent_5sec_periods function with window_size={window_size}, tolerance={tolerance}")
    if df is None or df.empty:
        print("No data available for analysis")
        return
    
    # Calculate time differences
    print("Sorting data and calculating time differences")
    df = df.sort_values('datetime')
    df['time_diff'] = df['datetime'].diff().dt.total_seconds()
    df['time_diff'] = df['time_diff'].fillna(0)
    
    # Check if time difference is close to 5 seconds
    print("Identifying records with ~5 second intervals")
    df['is_5sec'] = (df['time_diff'] >= 5 - tolerance) & (df['time_diff'] <= 5 + tolerance)
    print(f"Found {df['is_5sec'].sum()} records with ~5 second intervals")
    
    # Use rolling window to find consistent periods
    print("Applying rolling window to find consistent periods")
    df['consistent_5sec'] = df['is_5sec'].rolling(window=window_size).sum() >= window_size * 0.95
    
    # Find start and end of consistent periods
    print("Identifying start and end of consistent periods")
    consistent_periods = []
    in_period = False
    start_idx = None
    
    print(f"Iterating through dataframe to find periods (total records: {len(df)})...")
    for idx, row in df.iterrows():
        if idx % 100000 == 0:  # Changed from 1000 to 10000
            print(f"Processing record {idx:,} of {len(df):,} ({(idx/len(df)*100):.1f}%)")
            
        if row['consistent_5sec'] and not in_period:
            in_period = True
            start_idx = idx
        elif not row['consistent_5sec'] and in_period:
            in_period = False
            end_idx = idx - 1
            if end_idx - start_idx + 1 >= window_size:  # Ensure minimum length
                start_time = df.loc[start_idx, 'datetime']
                end_time = df.loc[end_idx, 'datetime']
                duration = (end_time - start_time).total_seconds() / 60  # in minutes
                instrument = df.loc[start_idx, 'instrument']
                consistent_periods.append({
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration_minutes': duration,
                    'instrument': instrument,
                    'record_count': end_idx - start_idx + 1
                })
                print(f"Found period: {start_time} to {end_time} ({duration:.2f} minutes)")
    
    # Handle case where we're still in a period at the end of the dataframe
    if in_period:
        print("Processing final period at end of dataframe")
        end_idx = df.index[-1]
        start_time = df.loc[start_idx, 'datetime']
        end_time = df.loc[end_idx, 'datetime']
        duration = (end_time - start_time).total_seconds() / 60  # in minutes
        instrument = df.loc[start_idx, 'instrument']
        consistent_periods.append({
            'start_time': start_time,
            'end_time': end_time,
            'duration_minutes': duration,
            'instrument': instrument,
            'record_count': end_idx - start_idx + 1
        })
        print(f"Found final period: {start_time} to {end_time} ({duration:.2f} minutes)")
    
    # Convert to DataFrame for easier analysis
    print("Converting periods to DataFrame")
    periods_df = pd.DataFrame(consistent_periods)
    
    if not periods_df.empty:
        # Sort by duration (longest first)
        print("Sorting periods by duration")
        periods_df = periods_df.sort_values('duration_minutes', ascending=False)
        
        print(f"\nFound {len(periods_df)} periods with consistent ~5 second intervals")
        print("\nTop 10 longest consistent 5-second periods:")
        
        for idx, row in periods_df.head(10).iterrows():
            print(f"Period from {row['start_time'].strftime('%Y-%m-%d %H:%M:%S')} to "
                  f"{row['end_time'].strftime('%Y-%m-%d %H:%M:%S')} "
                  f"({row['duration_minutes']:.2f} minutes, {row['record_count']} records) "
                  f"for instrument {row['instrument']}")
        
        # Plot distribution of consistent period durations
        print("Creating period duration distribution plot")
        plt.figure(figsize=(12, 6))
        plt.hist(periods_df['duration_minutes'], bins=50, alpha=0.7)
        plt.xlabel('Duration (minutes)')
        plt.ylabel('Frequency')
        plt.title('Distribution of Consistent 5-Second Period Durations')
        plt.grid(True, alpha=0.3)
        plt.savefig('consistent_periods_distribution.png')
        plt.close()
        print("Period distribution plot saved")
        
        return periods_df
    else:
        print("No consistent 5-second periods found in the data.")
        return pd.DataFrame()

def analyze_by_instrument(df):
    """Analyze data completeness by instrument"""
    print("Starting analyze_by_instrument function")
    if df is None or df.empty:
        print("No data available for analysis")
        return
    
    # Group by instrument and date
    print("Grouping data by instrument and date")
    df['date'] = df['datetime'].dt.date
    instrument_date_counts = df.groupby(['instrument', 'date']).size().reset_index(name='record_count')
    print(f"Created grouping with {len(instrument_date_counts)} instrument-date combinations")
    
    # Calculate statistics by instrument
    print("Calculating statistics by instrument")
    instrument_stats = instrument_date_counts.groupby('instrument').agg(
        avg_daily_records=('record_count', 'mean'),
        min_daily_records=('record_count', 'min'),
        max_daily_records=('record_count', 'max'),
        days_with_data=('record_count', 'count')
    ).reset_index()
    
    # Calculate total days in the dataset
    total_days = (df['date'].max() - df['date'].min()).days + 1
    print(f"Dataset spans {total_days} days from {df['date'].min()} to {df['date'].max()}")
    instrument_stats['coverage_pct'] = (instrument_stats['days_with_data'] / total_days) * 100
    
    print("\nData completeness by instrument:")
    print(instrument_stats.sort_values('coverage_pct', ascending=False))
    
    # Plot coverage by instrument
    print("Creating instrument coverage plot")
    plt.figure(figsize=(14, 8))
    bars = plt.barh(instrument_stats['instrument'], instrument_stats['coverage_pct'], alpha=0.7)
    plt.xlabel('Coverage (%)')
    plt.ylabel('Instrument')
    plt.title('Data Coverage by Instrument')
    plt.grid(True, alpha=0.3, axis='x')
    plt.xlim(0, 100)
    
    # Add percentage labels
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 1, bar.get_y() + bar.get_height()/2, f'{width:.1f}%', 
                 ha='left', va='center')
    
    plt.tight_layout()
    plt.savefig('instrument_coverage.png')
    plt.close()
    print("Instrument coverage plot saved")
    
    return instrument_stats

def analyze_weekly_distribution(df):
    """Analyze the weekly distribution of records by instrument"""
    print("Starting weekly distribution analysis")
    if df is None or df.empty:
        print("No data available for analysis")
        return
    
    # Create a week column (year-week format)
    print("Grouping data by week")
    df['year_week'] = df['datetime'].dt.strftime('%Y-%U')
    
    # Group by instrument and week
    weekly_counts = df.groupby(['instrument', 'year_week']).size().reset_index(name='record_count')
    
    # Pivot the data for better visualization
    pivot_table = weekly_counts.pivot(index='year_week', columns='instrument', values='record_count')
    pivot_table = pivot_table.fillna(0)
    
    # Sort by week
    pivot_table = pivot_table.sort_index()
    
    print(f"Created weekly distribution table with {len(pivot_table)} weeks and {len(pivot_table.columns)} instruments")
    
    # Save the full table to CSV
    pivot_table.to_csv('weekly_instrument_distribution.csv')
    print("Weekly distribution saved to weekly_instrument_distribution.csv")
    
    # Create a heatmap visualization
    print("Creating heatmap visualization")
    plt.figure(figsize=(16, 10))
    
    # If there are too many instruments, limit to the top ones by total count
    if len(pivot_table.columns) > 15:
        print("Many instruments detected, limiting heatmap to top 15 by volume")
        top_instruments = weekly_counts.groupby('instrument')['record_count'].sum().nlargest(15).index
        pivot_subset = pivot_table[top_instruments]
    else:
        pivot_subset = pivot_table
    
    # Create heatmap
    sns.heatmap(pivot_subset, cmap="YlGnBu", linewidths=.5, cbar_kws={'label': 'Record Count'})
    plt.title('Weekly Record Count by Instrument')
    plt.ylabel('Year-Week')
    plt.xlabel('Instrument')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('weekly_heatmap.png', dpi=300)
    plt.close()
    print("Weekly heatmap saved to weekly_heatmap.png")
    
    # Create a line plot for the top instruments
    print("Creating line plot for weekly trends")
    plt.figure(figsize=(16, 8))
    
    for instrument in pivot_subset.columns:
        plt.plot(pivot_subset.index, pivot_subset[instrument], label=instrument, linewidth=1.5, marker='o', markersize=3)
    
    plt.title('Weekly Record Count Trends by Instrument')
    plt.ylabel('Record Count')
    plt.xlabel('Year-Week')
    plt.xticks(rotation=45, ha='right')
    plt.grid(True, alpha=0.3)
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
    plt.tight_layout()
    plt.savefig('weekly_trends.png', dpi=300)
    plt.close()
    print("Weekly trends plot saved to weekly_trends.png")
    
    # Print summary statistics
    print("\nWeekly record count summary by instrument:")
    summary = weekly_counts.groupby('instrument').agg(
        total_records=('record_count', 'sum'),
        avg_weekly_records=('record_count', 'mean'),
        max_weekly_records=('record_count', 'max'),
        weeks_with_data=('record_count', 'count')
    ).sort_values('total_records', ascending=False)
    
    print(summary.head(10))
    summary.to_csv('weekly_summary_by_instrument.csv')
    print("Weekly summary saved to weekly_summary_by_instrument.csv")
    
    return pivot_table, summary

def print_weekly_counts_by_instrument(df):
    """Print a simple week-by-week record count for each instrument with first and last record dates and timestamps"""
    print("\n=== Weekly Record Counts by Instrument ===")
    if df is None or df.empty:
        print("No data available for analysis")
        return
    
    # Create a week column (year-week format)
    df['year_week'] = df['datetime'].dt.strftime('%Y-%U')
    
    # Group by instrument and week, getting first and last timestamps
    weekly_stats = df.groupby(['instrument', 'year_week']).agg(
        record_count=('time', 'count'),
        first_record=('datetime', 'min'),
        last_record=('datetime', 'max'),
        first_unix=('time', 'min'),
        last_unix=('time', 'max')
    ).reset_index()
    
    # Sort by instrument and week
    weekly_stats = weekly_stats.sort_values(['instrument', 'year_week'])
    
    # Print counts and date ranges for each instrument by week
    for instrument, group in weekly_stats.groupby('instrument'):
        print(f"\nInstrument: {instrument}")
        print("Week       | Records    | First Record           | First Unix    | Last Record            | Last Unix")
        print("-----------+------------+------------------------+---------------+------------------------+---------------")
        for _, row in group.iterrows():
            first_date = row['first_record'].strftime('%Y-%m-%d %H:%M:%S')
            last_date = row['last_record'].strftime('%Y-%m-%d %H:%M:%S')
            print(f"{row['year_week']} | {row['record_count']:10,} | {first_date} | {row['first_unix']:13} | {last_date} | {row['last_unix']:13}")
        
        # Print total for this instrument
        total = group['record_count'].sum()
        print(f"Total      | {total:10,} |")
        
        # Calculate and print gaps between weeks
        print("\nGaps between weeks:")
        print("From Week  | To Week    | Gap Duration           | From Unix     | To Unix")
        print("-----------+------------+------------------------+---------------+---------------")
        
        # Sort by year_week to ensure chronological order
        sorted_group = group.sort_values('year_week')
        
        # Calculate gaps between consecutive weeks
        for i in range(len(sorted_group) - 1):
            current_week = sorted_group.iloc[i]
            next_week = sorted_group.iloc[i + 1]
            
            gap_start_time = current_week['last_unix']
            gap_end_time = next_week['first_unix']
            
            gap_start = current_week['last_record']
            gap_end = next_week['first_record']
            
            gap_duration = gap_end - gap_start
            gap_seconds = gap_duration.total_seconds()
            gap_days = gap_seconds / (24 * 60 * 60)
            
            # Format the gap duration
            days = int(gap_days)
            hours = int((gap_seconds % (24 * 3600)) / 3600)
            minutes = int((gap_seconds % 3600) / 60)
            seconds = int(gap_seconds % 60)
            
            gap_formatted = f"{days}d {hours}h {minutes}m {seconds}s"
            
            print(f"{current_week['year_week']} | {next_week['year_week']} | {gap_formatted:22} | {gap_start_time:13} | {gap_end_time:13}")
    
    # Save to CSV for reference
    weekly_stats.to_csv('weekly_counts_by_instrument.csv', index=False)
    print("\nWeekly counts saved to weekly_counts_by_instrument.csv")

def main(sample_mode=False):
    print("=== Starting market data analysis ===")
    print(f"Running in {'sample mode' if sample_mode else 'full mode'}")
    
    print("Fetching data from database...")
    if sample_mode:
        # Fetch only a small sample for testing
        df = fetch_sample_data()
    else:
        df = fetch_data_since_2025()
    
    if df is None or df.empty:
        print("No data available for the specified period.")
        return
    
    # Add more detailed summary of loaded data
    print("\n=== DATA SUMMARY ===")
    print(f"Total records loaded: {len(df):,}")
    
    # Get time range information
    min_date = df['datetime'].min()
    max_date = df['datetime'].max()
    date_range_days = (max_date - min_date).total_seconds() / 86400
    
    print(f"Time range: {min_date.strftime('%Y-%m-%d %H:%M:%S')} to {max_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Date range span: {date_range_days:.2f} days")
    
    # Count unique instruments
    unique_instruments = df['instrument'].nunique()
    print(f"Number of unique instruments: {unique_instruments}")
    
    # Count records by instrument
    instrument_counts = df['instrument'].value_counts()
    print("\nTop 5 instruments by record count:")
    for instrument, count in instrument_counts.head(5).items():
        print(f"  {instrument}: {count:,} records")
    
    print("=== END DATA SUMMARY ===\n")
    
    # Create output directory for reports
    print("Creating output directory")
    os.makedirs('market_data_analysis', exist_ok=True)
    os.chdir('market_data_analysis')
    print(f"Working directory changed to: {os.getcwd()}")
    
    # Analyze gaps
    print("\n=== Analyzing Time Gaps ===")
    gaps_df = analyze_time_gaps(df)
    if not gaps_df.empty:
        print("Saving weekday gaps to CSV")
        gaps_df.to_csv('weekday_gaps.csv', index=False)
    
    # Find consistent 5-second periods
    print("\n=== Finding Consistent 5-Second Periods ===")
    periods_df = find_consistent_5sec_periods(df)
    if not periods_df.empty:
        print("Saving consistent periods to CSV")
        periods_df.to_csv('consistent_5sec_periods.csv', index=False)
    
    # Analyze by instrument
    print("\n=== Analyzing Data by Instrument ===")
    instrument_stats = analyze_by_instrument(df)
    if instrument_stats is not None:
        print("Saving instrument stats to CSV")
        instrument_stats.to_csv('instrument_stats.csv', index=False)
    
    # Analyze weekly distribution
    print("\n=== Analyzing Weekly Distribution ===")
    pivot_table, summary = analyze_weekly_distribution(df)
    
    # Print weekly counts by instrument
    print_weekly_counts_by_instrument(df)
    
    print("\n=== Analysis complete. Results saved to the 'market_data_analysis' directory. ===")

def fetch_sample_data():
    """Fetch a small sample of data for testing"""
    conn = connect_to_db()
    if not conn:
        return None
    
    cursor = conn.cursor(dictionary=True)
    
    # Get a small sample (e.g., 1000 records)
    query = """
    SELECT time, nanoseconds, instrument
    FROM rawprice
    LIMIT 1000
    """
    
    try:
        cursor.execute(query)
        data = cursor.fetchall()
        print(f"Fetched {len(data)} sample records")
        
        df = pd.DataFrame(data)
        df['datetime'] = pd.to_datetime(df['time'], unit='s')
        df['datetime'] = df['datetime'] + pd.to_timedelta(df['nanoseconds'] / 1000, unit='us')
        
        return df
    except mysql.connector.Error as err:
        print(f"Error fetching sample data: {err}")
        return None
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("Script started")
    
    parser = argparse.ArgumentParser(description='Analyze market data gaps')
    parser.add_argument('--sample', action='store_true', help='Run in sample mode with limited data')
    args = parser.parse_args()
    
    main(sample_mode=args.sample)
    print("Script finished") 