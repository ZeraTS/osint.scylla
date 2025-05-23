from concurrent.futures import ThreadPoolExecutor
import os
import pandas as pd
import re
from tkinter import Tk, filedialog, simpledialog, messagebox
from tqdm import tqdm
import csv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt
from rich import print as rprint
from rich import box
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.query import SimpleStatement, BatchStatement
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
import asyncio
import aiofiles
import gc
from datetime import datetime
import json
import multiprocessing
console = Console()

# ScyllaDB connection setup
class ScyllaApp:
    def __init__(self, contact_points=['localhost'], port=9042, keyspace='user_data'):
    # Create a single execution profile with optimized settings
     profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
        consistency_level=ConsistencyLevel.ONE,  # Using ONE for better performance
        request_timeout=60
     )

     self.cluster = Cluster(
        contact_points=contact_points,
        port=port,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        protocol_version=4,
        compression=True,
        control_connection_timeout=10,
        connect_timeout=10,
        executor_threads=MAX_WORKERS  
    )
     try:
        self.session = self.cluster.connect()
        console.print("[green]Connected to ScyllaDB cluster[/green]")
     except Exception as e:
        console.print(f"[red]Error connecting to ScyllaDB cluster: {str(e)}[/red]")
        raise

     self.create_keyspace_if_not_exists(keyspace)
     self.session.set_keyspace(keyspace)
     self.create_table_if_not_exists()
     self.create_indexes()
     self.prepare_statements()


    def close(self):
        """Close the cluster connection"""
        try:
            if hasattr(self, 'cluster'):
                self.cluster.shutdown()
            console.print("[green]ScyllaDB connection closed successfully[/green]")
        except Exception as e:
            console.print(f"[red]Error closing ScyllaDB connection: {str(e)}[/red]")

    def create_keyspace_if_not_exists(self, keyspace_name):
        try:
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            console.print(f"[green]Keyspace '{keyspace_name}' created or already exists[/green]")
        except Exception as e:
            console.print(f"[red]Error creating keyspace: {str(e)}[/red]")
            raise

    def create_indexes(self):
        try:
            self.session.execute("CREATE INDEX IF NOT EXISTS idx_username ON user_data (username)")
            self.session.execute("CREATE INDEX IF NOT EXISTS idx_first_name ON user_data (first_name)")
            self.session.execute("CREATE INDEX IF NOT EXISTS idx_last_name ON user_data (last_name)")
            self.session.execute("CREATE INDEX IF NOT EXISTS idx_phone_number ON user_data (phone_number)")
            console.print("[green]Secondary indexes created[/green]")
        except Exception as e:
            console.print(f"[red]Error creating indexes: {str(e)}[/red]")

    def create_table_if_not_exists(self):
        try:
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS user_data (
                    email text PRIMARY KEY,
                    username text,
                    first_name text,
                    last_name text,
                    phone_number text,
                    city text,
                    state text,
                    dob text,
                    source text,
                    data text
                );
            """)
            console.print("[green]Table 'user_data' created or already exists[/green]")
        except Exception as e:
            console.print(f"[red]Error creating table: {str(e)}[/red]")

    def prepare_statements(self):
        try:
            self.insert_stmt = self.session.prepare("""
                INSERT INTO user_data (email, username, first_name, last_name, phone_number, city, state, dob, source, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            self.select_stmt = self.session.prepare("SELECT * FROM user_data WHERE email = ?")
            console.print("[green]Prepared statements created[/green]")
        except Exception as e:
            console.print(f"[red]Error preparing statements: {str(e)}[/red]")
            raise

    def count_total_rows(self, table_name):
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            result = self.session.execute(query)
            count = result.one()[0]
            console.print(f"[green]Total number of rows in {table_name}: {count}[/green]")
            return count
        except Exception as e:
            console.print(f"[red]Error counting rows in {table_name}: {str(e)}[/red]")
            return None

    def insert_data_in_batches(self, data, batch_size=1000):
        for i in range(0, len(data), batch_size):
            batch = BatchStatement()
            for record in data[i:i + batch_size]:
                email = record.get('email')
                username = record.get('username')
                first_name = record.get('first_name')
                last_name = record.get('last_name')
                phone_number = record.get('phone_number')

                # Ensure email is present
                if not email:
                    console.print("[yellow]Skipping record due to missing email[/yellow]")
                    continue

                try:
                    batch.add(self.insert_stmt, (email, username, first_name, last_name, phone_number, record.get('city'), record.get('state'), record.get('dob'), record.get('source'), record.get('data')))
                except Exception as e:
                    console.print(f"[red]Error adding record to batch: {e}[/red]")
                    console.print(f"[yellow]Problematic record: {record}[/yellow]")

            try:
                self.session.execute(batch)
            except Exception as e:
                console.print(f"[red]Error executing batch: {e}[/red]")

# Helper functions for detecting patterns in data
def detect_phone_number(cell_value):
    phone_number_pattern = r'\+?\d{1,4}?[-.\s]?\(?\d{1,3}?\)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}'
    if isinstance(cell_value, str):
        phone_match = re.search(phone_number_pattern, cell_value)
        if (phone_match):
            return phone_match.group(0)
    return None

def detect_email(cell_value):
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if isinstance(cell_value, str):
        email_match = re.search(email_pattern, cell_value)
        if (email_match):
            return email_match.group(0)
    return None

def detect_hash(cell_value):
    if isinstance(cell_value, str):
        if re.match(r'^[a-fA-F0-9]{32}$', cell_value):
            return "MD5"
        elif re.match(r'^[a-fA-F0-9]{40}$', cell_value):
            return "SHA-1"
        elif re.match(r'^[a-fA-F0-9]{64}$', cell_value):
            return "SHA-256"
        elif re.match(r'^[a-zA-Z0-9+/]{43}=$', cell_value):
            return "Base64 SHA-256"
    return None

def detect_name(cell_value):
    if isinstance(cell_value, str):
        name_parts = cell_value.split()
        if len(name_parts) == 2:
            return {"first_name": name_parts[0], "last_name": name_parts[1]}
        elif len(name_parts) == 1:
            return {"full_name": name_parts[0]}
    return None

def convert_to_string(value):
    if pd.isna(value):
        return ''
    return str(value)

def get_value_from_record(record, possible_keys):
    for key in possible_keys:
        value = record.get(key)
        if value:
            return value
    return ''

def confirm_partition(file_path, threshold_kb=150000):
    file_size_kb = os.path.getsize(file_path) / 1024
    if file_size_kb > threshold_kb:
        return Prompt.ask(
            f"The file is {file_size_kb:.2f} KB, which is larger than {threshold_kb} KB. Partition the file?",
            choices=["y", "n"],
            default="y"
        ) == "y"
    return False

def read_malformed_csv(file_path, delimiter=',', chunksize=None):
    """Read CSV file with support for chunked reading and multiple encodings"""
    cleaned_data = []
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'utf-16', 'utf-32']
    
    for encoding in encodings:
        try:
            if chunksize:
                # Read in chunks using pandas
                return pd.read_csv(
                    file_path,
                    encoding=encoding,
                    chunksize=chunksize,
                    on_bad_lines='skip',
                    low_memory=False,
                    engine='python'
                )
            else:
                # Read entire file
                with open(file_path, 'r', encoding=encoding, errors='ignore') as file:
                    reader = csv.reader(file, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
                    headers = next(reader, None)
                    if headers:
                        cleaned_data.append(headers)
                    for row in reader:
                        try:
                            cleaned_data.append(row)
                        except csv.Error as e:
                            console.print(f"[red]Error processing row: {e}[/red]")
                            continue
                return pd.DataFrame(cleaned_data[1:], columns=cleaned_data[0])
                
        except UnicodeDecodeError:
            console.print(f"[yellow]Failed to decode with {encoding}, trying next encoding...[/yellow]")
            continue
        except Exception as e:
            console.print(f"[red]Error reading file with {encoding}: {e}[/red]")
            continue
    
    raise UnicodeDecodeError(f"Unable to decode the file with any of these encodings: {encodings}")

def read_malformed_csv(file_path, delimiter=',', chunksize=None):
    """Read CSV file with support for chunked reading and multiple encodings"""
    cleaned_data = []
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'utf-16', 'utf-32']
    
    for encoding in encodings:
        try:
            if chunksize:
                # Read in chunks using pandas
                return pd.read_csv(
                    file_path,
                    encoding=encoding,
                    chunksize=chunksize,
                    on_bad_lines='skip',
                    low_memory=False,
                    engine='python'
                )
            else:
                # Read entire file
                with open(file_path, 'r', encoding=encoding, errors='ignore') as file:
                    reader = csv.reader(file, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
                    headers = next(reader, None)
                    if headers:
                        cleaned_data.append(headers)
                    for row in reader:
                        try:
                            cleaned_data.append(row)
                        except csv.Error as e:
                            console.print(f"[red]Error processing row: {e}[/red]")
                            continue
                return pd.DataFrame(cleaned_data[1:], columns=cleaned_data[0])
                
        except UnicodeDecodeError:
            console.print(f"[yellow]Failed to decode with {encoding}, trying next encoding...[/yellow]")
            continue
        except Exception as e:
            console.print(f"[red]Error reading file with {encoding}: {e}[/red]")
            continue
    
    raise UnicodeDecodeError(f"Unable to decode the file with any of these encodings: {encodings}")

async def process_file(file_path, scylla_app, executor):
    """Process file with chunked reading for large files"""
    try:
        if file_path.endswith('.csv'):
            file_size = os.path.getsize(file_path)
            
            # Use chunked reading for large files (> 100MB)
            if file_size > 100_000_000:  # 100MB
                df_iterator = await asyncio.get_event_loop().run_in_executor(
                    executor, 
                    read_malformed_csv, 
                    file_path, 
                    chunksize=10000  # Corrected argument name
                )
                
                for chunk in df_iterator:
                    records = chunk.to_dict(orient='records')
                    await insert_records_in_batches(records, file_path, scylla_app, executor=executor)
            else:
                # Read entire file at once for smaller files
                df = await asyncio.get_event_loop().run_in_executor(executor, read_malformed_csv, file_path)
                records = df.to_dict(orient='records')
                await insert_records_in_batches(records, file_path, scylla_app, executor=executor)
                
        elif file_path.endswith('.txt'):
            async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                records = []
                async for line in file:
                    try:
                        record = json.loads(line)
                        records.append(record)
                        if len(records) >= 10000:  # Process in chunks
                            await insert_records_in_batches(records, file_path, scylla_app, executor=executor)
                            records = []
                    except json.JSONDecodeError:
                        continue
                
                # Process remaining records
                if records:
                    await insert_records_in_batches(records, file_path, scylla_app, executor=executor)
        else:
            console.print(f"[red]Unsupported file type: {file_path}[/red]")
            
    except Exception as e:
        console.print(f"[red]Error processing file {file_path}: {str(e)}[/red]")
        console.print("[yellow]Attempting to continue with next file...[/yellow]")
async def load_all_files(scylla_app, executor):
    root = Tk()
    root.withdraw()  # Hide the root window
    directory = filedialog.askdirectory(title="Select a directory")
    if directory:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".csv") or file.endswith(".txt"):
                    file_path = os.path.join(root, file)
                    await process_file(file_path, scylla_app, executor)  # Pass executor here
    else:
        console.print("[yellow]No directory selected.[/yellow]")
async def insert_batch(batch, file_path, scylla_app, pbar, executor):
    skipped_count = 0
    existing_columns = set()

    # Fetch existing columns from the table
    try:
        table_metadata = await asyncio.get_event_loop().run_in_executor(
            executor, 
            lambda: scylla_app.cluster.metadata.keyspaces[scylla_app.session.keyspace].tables['user_data']
        )
        existing_columns = set(table_metadata.columns.keys())
    except Exception as e:
        console.print(f"[red]Error fetching table metadata: {e}[/red]")
        return

    # Split the batch into smaller batches if it exceeds the limit
    max_statements_per_batch = 65535
    sub_batches = [batch[i:i + max_statements_per_batch] for i in range(0, len(batch), max_statements_per_batch)]

    for sub_batch in sub_batches:
        batch_stmt = BatchStatement()
        for record in sub_batch:
            try:
                # Convert record to proper format with standard fields
                formatted_record = {
                    'email': convert_to_string(get_value_from_record(record, ['email', 'mail', 'e-mail address', 'e-mail', 'email_address', 'emailaddress', 'email-address', 'email address', 'user_email', 'useremail', 'user-email', 'user email', 'Email'])),
                    'username': convert_to_string(get_value_from_record(record, ['username', 'user_name', 'user', 'login', 'Username'])),
                    'first_name': convert_to_string(get_value_from_record(record, ['first_name', 'first', 'fname', 'f_name', 'FirstName'])),
                    'last_name': convert_to_string(get_value_from_record(record, ['last_name', 'last', 'lname', 'l_name', 'LastName'])),
                    'phone_number': convert_to_string(get_value_from_record(record, ['phone_number', 'phone', 'telephone', 'tel', 'Phone'])),
                    'password': convert_to_string(get_value_from_record(record, ['password', 'pwd', 'pass'])),
                    'city': convert_to_string(get_value_from_record(record, ['city', 'town', 'location', 'City'])),
                    'state': convert_to_string(get_value_from_record(record, ['state', 'province', 'region', 'State'])),
                    'dob': convert_to_string(get_value_from_record(record, ['dob', 'date_of_birth', 'dateofbirth', 'birth_date', 'DOB'])),
                    'source': file_path,
                    'data': json.dumps(record)  # Store complete record as JSON
                }

                # Ensure email is present
                if not formatted_record['email']:
                    skipped_count += 1
                    continue

                # Add any new columns found in the record
                for key in record.keys():
                    if key not in existing_columns and key not in formatted_record:
                        sanitized_key = re.sub(r'[^a-zA-Z0-9_]', '_', key.lower())
                        try:
                            alter_query = f"ALTER TABLE user_data ADD {sanitized_key} text"
                            await asyncio.get_event_loop().run_in_executor(executor, scylla_app.session.execute, alter_query)
                            existing_columns.add(sanitized_key)
                            formatted_record[sanitized_key] = convert_to_string(record[key])
                        except Exception as e:
                            if "Invalid column name" not in str(e):
                                console.print(f"[red]Error adding column '{sanitized_key}': {e}[/red]")
                            continue

                # Prepare and execute the insert statement
                columns = ', '.join(formatted_record.keys())
                placeholders = ', '.join(['?'] * len(formatted_record))
                insert_stmt = scylla_app.session.prepare(
                    f"INSERT INTO user_data ({columns}) VALUES ({placeholders})"
                )

                batch_stmt.add(insert_stmt, tuple(formatted_record.values()))

            except Exception as e:
                console.print(f"[red]Error processing record: {e}[/red]")
                continue

        try:
            await asyncio.get_event_loop().run_in_executor(executor, scylla_app.session.execute, batch_stmt)
            pbar.update(len(sub_batch))
        except Exception as e:
            console.print(f"[red]Error executing batch: {e}[/red]")

    if skipped_count > 0:
        console.print(f"[yellow]Skipped {skipped_count} records due to missing email.[/yellow]")
       
MAX_WORKERS = 100000
async def insert_records_in_batches(records, file_path, scylla_app, batch_size=1000, executor=None):
    """Insert records in optimized batches with parallel processing"""
    try:
        chunks = [records[i:i + batch_size] for i in range(0, len(records), batch_size)]
        total_records = len(records)
        processed_records = 0
        
        with tqdm(total=total_records, desc=f"Processing {file_path}", unit="records") as pbar:
            # Process chunks concurrently in groups
            for i in range(0, len(chunks), 10):  # Process 10 chunks at a time
                current_chunks = chunks[i:i + 10]
                tasks = []
                
                for chunk in current_chunks:
                    task = asyncio.create_task(
                        process_chunk(
                            chunk=chunk,
                            file_path=file_path,
                            scylla_app=scylla_app,
                            executor=executor  # Pass executor here
                        )
                    )
                    tasks.append(task)
                
                # Wait for current group of chunks to complete
                results = await asyncio.gather(*tasks)
                
                # Update progress
                for processed_count in results:
                    if isinstance(processed_count, int):
                        processed_records += processed_count
                        pbar.update(processed_count)
                
    except Exception as e:
        console.print(f"[red]Error in batch insertion: {e}[/red]")
async def process_chunk(chunk, file_path, scylla_app, executor):
    """Process a single chunk of records using ThreadPoolExecutor"""
    try:
        batch_stmt = BatchStatement()
        processed_count = 0
        
        # Process records in parallel
        futures = []
        for record in chunk:
            future = executor.submit(
                format_and_add_record,
                record,
                file_path,
                scylla_app,
                batch_stmt
            )
            futures.append(future)
        
        # Wait for all record processing to complete
        for future in futures:
            result = future.result()
            if result:
                processed_count += 1
        
        # Execute batch
        await asyncio.get_event_loop().run_in_executor(executor, scylla_app.session.execute, batch_stmt)
        return processed_count
        
    except Exception as e:
        console.print(f"[red]Error processing chunk: {e}[/red]")
        return 0
def format_and_add_record(record, file_path, scylla_app, batch_stmt):
    """Format and add a single record to the batch statement"""
    try:
        formatted_record = {
            'email': convert_to_string(get_value_from_record(record, ['email', 'mail', 'e-mail address', 'e-mail', 'Email'])),
            'username': convert_to_string(get_value_from_record(record, ['username', 'user_name', 'user', 'login'])),
            'first_name': convert_to_string(get_value_from_record(record, ['first_name', 'firstname', 'fname'])),
            'last_name': convert_to_string(get_value_from_record(record, ['last_name', 'lastname', 'lname'])),
            'phone_number': convert_to_string(get_value_from_record(record, ['phone_number', 'phone', 'telephone'])),
            'password': convert_to_string(get_value_from_record(record, ['password', 'pwd', 'pass'])),
            'source': file_path,
            'data': json.dumps(record)
        }
        
        if not formatted_record['email']:
            return False
            
        columns = ', '.join(formatted_record.keys())
        placeholders = ', '.join(['?'] * len(formatted_record))
        insert_stmt = scylla_app.session.prepare(
            f"INSERT INTO user_data ({columns}) VALUES ({placeholders})"
        )
        
        batch_stmt.add(insert_stmt, tuple(formatted_record.values()))
        return True
        
    except Exception as e:
        console.print(f"[red]Error formatting record: {e}[/red]")
        return False
def optimize_batch_size(file_size):
    """Dynamically adjust batch size based on file size"""
    if file_size > 1_000_000_000:  # 1GB
        return 100000
    elif file_size > 100_000_000:  # 100MB
        return 200000
    else:
        return 5000

async def process_large_file(file_path, scylla_app, executor):
    """Process large files with optimized memory usage"""
    file_size = os.path.getsize(file_path)
    batch_size = optimize_batch_size(file_size)
    
    try:
        if file_path.endswith('.csv'):
            total_rows = sum(1 for _ in open(file_path)) - 1
            with tqdm(total=total_rows, desc=f"Processing {file_path}") as pbar:
                async for chunk in read_csv_chunks(file_path, chunksize=batch_size):
                    await insert_records_in_batches(chunk, file_path, scylla_app, batch_size, executor)
                    pbar.update(len(chunk))
    except Exception as e:
        console.print(f"[red]Error processing large file: {e}[/red]")
def format_record(record, file_path, existing_columns):
    formatted_record = {
        'email': convert_to_string(get_value_from_record(record, ['email', 'mail', 'e-mail address', 'e-mail', 'Email'])),
        'username': convert_to_string(get_value_from_record(record, ['username', 'user_name', 'user', 'login'])),
        'first_name': convert_to_string(get_value_from_record(record, ['first_name', 'firstname', 'fname'])),
        'last_name': convert_to_string(get_value_from_record(record, ['last_name', 'lastname', 'lname'])),
        'phone_number': convert_to_string(get_value_from_record(record, ['phone_number', 'phone', 'telephone'])),
        'city': convert_to_string(get_value_from_record(record, ['city', 'town', 'location'])),
        'state': convert_to_string(get_value_from_record(record, ['state', 'province', 'region'])),
        'dob': convert_to_string(get_value_from_record(record, ['dob', 'date_of_birth', 'birthdate'])),
        'source': file_path,
        'data': json.dumps(record)  # Store complete record as JSON
    }

    # Ensure email is present
    if not formatted_record['email']:
        return None

    # Add any new columns found in the record
    for key in record.keys():
        if key not in existing_columns and key not in formatted_record:
            sanitized_key = re.sub(r'[^a-zA-Z0-9_]', '_', key.lower())
            formatted_record[sanitized_key] = convert_to_string(record[key])

    return formatted_record
async def load_single_file(scylla_app, executor):
    root = Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(
        title="Select a file",
        filetypes=[("CSV Files", "*.csv"), ("Text Files", "*.txt")]
    )
    if file_path:
        file_size = os.path.getsize(file_path)
        if file_size > 100_000_000:  # 100MB
            await process_large_file(file_path, scylla_app, executor)
        else:
            await process_file(file_path, scylla_app, executor)
    else:
        console.print("[yellow]No file selected.[/yellow]")
    root.destroy()

async def update_table_schema(scylla_app, new_columns):
    """Update the table schema with new columns."""
    try:
        table_metadata = scylla_app.cluster.metadata.keyspaces[scylla_app.session.keyspace].tables['user_data']
        existing_columns = set(table_metadata.columns.keys())
        
        for column in new_columns:
            if column not in existing_columns:
                try:
                    await scylla_app.session.execute(f"ALTER TABLE user_data ADD {column} text")
                    console.print(f"[green]Added new column: {column}[/green]")
                except Exception as e:
                    if "Invalid column name" not in str(e):
                        console.print(f"[red]Error adding column {column}: {e}[/red]")
    except Exception as e:
        console.print(f"[red]Error updating schema: {e}[/red]")

async def read_csv_chunks(file_path, chunksize=10000):
    """Read CSV file in chunks asynchronously"""
    try:
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            yield chunk.to_dict(orient='records')
    except Exception as e:
        console.print(f"[red]Error reading CSV chunks: {e}[/red]")
async def process_chunk(chunk, file_path, scylla_app, executor):
    """Process a single chunk of records using ThreadPoolExecutor"""
    try:
        batch_stmt = BatchStatement()
        processed_count = 0
        
        # Process records in parallel
        futures = []
        for record in chunk:
            future = executor.submit(
                format_and_add_record,
                record,
                file_path,
                scylla_app,
                batch_stmt
            )
            futures.append(future)
        
        # Wait for all record processing to complete
        for future in futures:
            result = future.result()
            if result:
                processed_count += 1
        
        # Execute batch
        await asyncio.get_event_loop().run_in_executor(executor, scylla_app.session.execute, batch_stmt)
        return processed_count
        
    except Exception as e:
        console.print(f"[red]Error processing chunk: {e}[/red]")
        return 0
def read_malformed_csv(file_path, delimiter=',', chunksize=None):
    """Read CSV file with support for chunked reading and multiple encodings"""
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'utf-16', 'utf-32']
    
    for encoding in encodings:
        try:
            if chunksize:
                # Read in chunks using pandas
                return pd.read_csv(
                    file_path,
                    encoding=encoding,
                    chunksize=chunksize,
                    on_bad_lines='skip',
                    low_memory=False,
                    engine='python'
                )
            else:
                # Read entire file
                with open(file_path, 'r', encoding=encoding, errors='ignore') as file:
                    reader = csv.reader(file, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
                    headers = next(reader, None)
                    cleaned_data = [headers] if headers else []
                    for row in reader:
                        try:
                            cleaned_data.append(row)
                        except csv.Error as e:
                            console.print(f"[red]Error processing row: {e}[/red]")
                            continue
                return pd.DataFrame(cleaned_data[1:], columns=cleaned_data[0])
                
        except UnicodeDecodeError:
            console.print(f"[yellow]Failed to decode with {encoding}, trying next encoding...[/yellow]")
            continue
        except Exception as e:
            console.print(f"[red]Error reading file with {encoding}: {e}[/red]")
            continue
    
    raise UnicodeDecodeError(f"Unable to decode the file with any of these encodings: {encodings}")
async def search_scylla(search_input, scylla_app, max_results=None):
    await asyncio.to_thread(_search_scylla, search_input, scylla_app, max_results)

def _search_scylla(search_input, scylla_app, max_results=None):
    try:
        query_conditions = []
        
        # Define primary and alternative keys
        primary_and_alternative_keys = ["email", "first_name", "last_name", "phone_number", "username", "city", "state", "dob"]

        if ':' in search_input:
            field, value = search_input.split(':', 1)
            if field in primary_and_alternative_keys:
                # For email searches, we should be case-sensitive
                if field == "email":
                    query_conditions.append(f"{field} = '{value}'")
                else:
                    query_conditions.append(f"{field} = '{value}'")
                    query_conditions.append(f"{field} = '{value.lower()}'")
                    query_conditions.append(f"{field} = '{value.capitalize()}'")
                    query_conditions.append(f"{field} = '{value.upper()}'")

        # Construct and execute queries
        results = []
        for condition in query_conditions:
            query = f"SELECT * FROM user_data WHERE {condition}"
            if max_results:
                query += f" LIMIT {max_results}"
            
            try:
                console.print(f"[cyan]Executing query: {query}[/cyan]")
                rows = scylla_app.session.execute(query)
                
                # Process each row safely
                for row in rows:
                    try:
                        # Convert row to dictionary and handle None values
                        row_dict = {
                            'email': row.email,
                            'username': row.username if hasattr(row, 'username') else None,
                            'first_name': row.first_name if hasattr(row, 'first_name') else None,
                            'last_name': row.last_name if hasattr(row, 'last_name') else None,
                            'phone_number': row.phone_number if hasattr(row, 'phone_number') else None,
                            'city': row.city if hasattr(row, 'city') else None,
                            'state': row.state if hasattr(row, 'state') else None,
                            'dob': row.dob if hasattr(row, 'dob') else None,
                            'source': row.source if hasattr(row, 'source') else None
                        }
                        # Filter out None values
                        row_dict = {k: v for k, v in row_dict.items() if v is not None}
                        results.append(row_dict)
                    except Exception as e:
                        console.print(f"[yellow]Error processing row: {e}[/yellow]")
                        continue
                        
            except Exception as e:
                console.print(f"[red]Error executing query: {e}[/red]")
                continue

        if results:
            console.print(f"[bold green]Found {len(results)} results:[/bold green]")
            for i, result in enumerate(results, 1):
                table = Table(title=f"Result {i}", box=box.ROUNDED)
                for key, value in result.items():
                    table.add_row(str(key), str(value))
                console.print(table)
                console.print()
        else:
            console.print("[yellow]No matching records found.[/yellow]")
            
    except Exception as e:
        console.print(f"[red]Error searching ScyllaDB: {str(e)}[/red]")
        if 'query' in locals():
            console.print(f"[yellow]Query attempted: {query}[/yellow]")
    finally:
        gc.collect()


async def load_multiple_files(scylla_app, executor):
    root = Tk()
    root.withdraw()  # Hide the root window
    file_paths = filedialog.askopenfilenames(title="Select files", filetypes=[("CSV Files", "*.csv"), ("Text Files", "*.txt")])
    if file_paths:
        for file_path in file_paths:
            await process_file(file_path, scylla_app, executor)  # Pass executor here
    else:
        console.print("[yellow]No files selected.[/yellow]")
async def main():
    scylla_app = ScyllaApp(contact_points=['localhost'], port=9042, keyspace='user_data')
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    
    while True:
        console.print(Panel.fit(
            "[bold cyan]Select mode:[/bold cyan]\n"
            "1. Load a single file (CSV or TXT)\n"
            "2. Load all files in a directory (CSV or TXT)\n"
            "3. Load multiple selected files (CSV or TXT)\n"
            "4. Search ScyllaDB\n"
            "5. Exit",
            title="ScyllaDB Data Manager",
            border_style="bold green"
        ))
        
        mode = Prompt.ask("Enter mode", choices=["1", "2", "3", "4", "5", "6"])

        if mode == '1':
            await load_single_file(scylla_app, executor)  # Pass executor here
        elif mode == '2':
            await load_all_files(scylla_app, executor)  # Pass executor here
        elif mode == '3':
            await load_multiple_files(scylla_app, executor)  # Pass executor here
        elif mode == '4':
            search_input = input("Enter search terms (e.g., \"email:example@gmail.com\" or \"first_name:John\"): ")
            await search_scylla(search_input, scylla_app)
        elif mode == '5':
            console.print("[yellow]Exiting...[/yellow]")
            break
        else:
            console.print("[red]Invalid mode selected. Please try again.[/red]")

    scylla_app.close()

if __name__ == "__main__":
    asyncio.run(main())