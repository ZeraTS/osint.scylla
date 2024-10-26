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
import gc
from datetime import datetime
import json
import sqlparse

# Rich console setup
console = Console()

# ScyllaDB connection setup
class ScyllaApp:
    def __init__(self, contact_points=['localhost'], port=32768, keyspace='user_data'):
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM
        )
        self.cluster = Cluster(
            contact_points=contact_points,
            port=port,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile}
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
        self.create_materialized_views()
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
            self.session.execute("CREATE INDEX IF NOT EXISTS idx_first_name ON user_data (first_name)")
            self.session.execute("CREATE INDEX IF NOT EXISTS idx_last_name ON user_data (last_name)")
            self.session.execute("CREATE INDEX IF NOT EXISTS idx_phone_number ON user_data (phone_number)")
            self.session.execute("CREATE INDEX IF NOT EXISTS idx_username ON user_data (username)")
            self.session.execute("CREATE INDEX IF NOT EXISTS idx_source ON user_data (source)")
            console.print("[green]Secondary indexes created[/green]")
        except Exception as e:
            console.print(f"[red]Error creating indexes: {str(e)}[/red]")

    def create_materialized_views(self):
        try:
            self.session.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS user_data_by_first_name AS
                SELECT *
                FROM user_data
                WHERE first_name IS NOT NULL 
                    AND email IS NOT NULL 
                    AND username IS NOT NULL 
                    AND last_name IS NOT NULL 
                    AND phone_number IS NOT NULL
                    AND city IS NOT NULL
                    AND state IS NOT NULL
                    AND dob IS NOT NULL
                PRIMARY KEY (first_name, email, username, last_name, phone_number, city, state, dob)
            """)
            self.session.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS user_data_by_last_name AS
                SELECT *
                FROM user_data
                WHERE last_name IS NOT NULL 
                    AND email IS NOT NULL 
                    AND username IS NOT NULL 
                    AND first_name IS NOT NULL 
                    AND phone_number IS NOT NULL
                    AND city IS NOT NULL
                    AND state IS NOT NULL
                    AND dob IS NOT NULL
                PRIMARY KEY (last_name, email, username, first_name, phone_number, city, state, dob)
            """)
            self.session.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS user_data_by_phone_number AS
                SELECT *
                FROM user_data
                WHERE phone_number IS NOT NULL 
                    AND email IS NOT NULL 
                    AND username IS NOT NULL 
                    AND first_name IS NOT NULL 
                    AND last_name IS NOT NULL
                    AND city IS NOT NULL
                    AND state IS NOT NULL
                    AND dob IS NOT NULL
                PRIMARY KEY (phone_number, email, username, first_name, last_name, city, state, dob)
            """)
            console.print("[green]Materialized views created[/green]")
        except Exception as e:
            console.print(f"[red]Error creating materialized views: {str(e)}[/red]")

    def create_table_if_not_exists(self):
        try:
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS user_data (
                    email text,
                    username text,
                    first_name text,
                    last_name text,
                    phone_number text,
                    city text,
                    state text,
                    dob text,
                    source text,
                    data text,
                    PRIMARY KEY ((email), username, first_name, last_name, phone_number, city, state, dob)
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

    def insert_data_in_batches(self, data, batch_size=50):
        for i in range(0, len(data), batch_size):
            batch = BatchStatement()
            for record in data[i:i + batch_size]:
                email = record.get('email')
                username = record.get('username')
                first_name = record.get('first_name')
                last_name = record.get('last_name')
                phone_number = record.get('phone_number')

                # Ensure at least one key is present
                if not (email or username or (first_name and last_name) or phone_number):
                    console.print("[yellow]Skipping record due to missing keys[/yellow]")
                    continue

                primary_key = email or username or f"{first_name} {last_name}" or phone_number
                try:
                    batch.add(self.insert_stmt, (primary_key, record['data']))
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
        if phone_match:
            return phone_match.group(0)
    return None

def detect_email(cell_value):
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if isinstance(cell_value, str):
        email_match = re.search(email_pattern, cell_value)
        if email_match:
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

def read_malformed_csv(file_path, delimiter=','):
    cleaned_data = []
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'utf-16', 'utf-32']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding, errors='ignore') as file:
                reader = csv.reader(file, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
                headers = next(reader, None)  # Read the first row as headers
                if headers:
                    cleaned_data.append(headers)
                for row in reader:
                    try:
                        cleaned_data.append(row)
                    except csv.Error as e:
                        console.print(f"[red]Error processing row {reader.line_num}: {e}[/red]")
                        continue
            break
        except UnicodeDecodeError:
            console.print(f"[yellow]Failed to decode file with encoding: {encoding}. Trying next encoding...[/yellow]")
            continue
    else:
        raise UnicodeDecodeError(f"Unable to decode the file with any of the provided encodings: {encodings}")

    df = pd.DataFrame(cleaned_data[1:], columns=cleaned_data[0])
    return df
async def load_all_files(scylla_app):
    root = Tk()
    root.withdraw()  # Hide the root window
    directory = filedialog.askdirectory(title="Select a directory")
    if directory:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".csv") or file.endswith(".txt"):
                    file_path = os.path.join(root, file)
                    await process_file(file_path, scylla_app)
    else:
        console.print("[yellow]No directory selected.[/yellow]")
async def insert_batch(batch, file_path, scylla_app, pbar):
    skipped_count = 0
    batch_stmt = BatchStatement()
    for record in batch:
        record['source'] = file_path
        email = convert_to_string(get_value_from_record(record, ['email', 'mail', 'e-mail address', 'e-mail', 'email_address', 'emailaddress', 'email-address', 'email address', 'user_email', 'useremail', 'user-email', 'user email', 'user_id', 'userid', 'user-id', 'user id', 'account', 'acct', 'account_id', 'accountid', 'account-id', 'account id', 'account_number', 'accountnumber', 'account-number', 'account number', 'Email']))
        username = convert_to_string(get_value_from_record(record, ['username', 'user_name', 'user', 'login', 'user_id', 'userid', 'user-id', 'user id', 'account', 'acct', 'account_id', 'accountid', 'account-id', 'account id', 'account_number', 'accountnumber', 'account-number', 'account number']))
        
        # Add lowercase versions
        email_lower = email.lower() if email else None
        username_lower = username.lower() if username else None

        insert_stmt = scylla_app.session.prepare("""
            INSERT INTO user_data (email, username, email_lower, username_lower, data, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """)
        try:
            batch_stmt.add(insert_stmt, (email, username, email_lower, username_lower, json.dumps(record), file_path))
        except Exception as e:
            pbar.update(len(batch))
            console.print(f"[red]Error adding record to batch: {e}[/red]")
            console.print(f"[yellow]Problematic record: {record}[/yellow]")

    try:
        scylla_app.session.execute(batch_stmt)
        pbar.update(len(batch))  # Update the progress bar after each batch
    except Exception as e:
        console.print(f"[red]Error executing batch: {e}[/red]")

    if skipped_count > 0:
        console.print(f"[yellow]Skipped {skipped_count} records due to errors.[/yellow]")
async def insert_records_in_batches(records, file_path, scylla_app, batch_size=100):
    try:
        with tqdm(total=len(records), desc=f"Processing {file_path}") as pbar:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                try:
                    await insert_batch(batch, file_path, scylla_app, pbar)
                except Exception as e:
                    console.print(f"[red]Error inserting batch: {str(e)}[/red]")
                await asyncio.sleep(0.1)  # Prevent overwhelming the database
    except Exception as e:
        console.print(f"[red]Error in batch insertion: {str(e)}[/red]")
async def process_file(file_path, scylla_app):
    try:
        if file_path.endswith('.csv'):
            df = read_malformed_csv(file_path)
            records = df.to_dict(orient='records')
            await insert_records_in_batches(records, file_path, scylla_app)
        elif file_path.endswith('.txt'):
            with open(file_path, 'r', encoding='utf-8') as infile:
                records = [json.loads(line) for line in infile]
                await insert_records_in_batches(records, file_path, scylla_app)
        else:
            console.print(f"[red]Unsupported file type: {file_path}[/red]")
    except Exception as e:
        console.print(f"[red]Error processing file {file_path}: {e}[/red]")
async def process_file(file_path, scylla_app):
    try:
        if file_path.endswith('.csv'):
            df = read_malformed_csv(file_path)
            records = df.to_dict(orient='records')
            await insert_records_in_batches(records, file_path, scylla_app)
        elif file_path.endswith('.txt'):
            with open(file_path, 'r', encoding='utf-8') as infile:
                records = [json.loads(line) for line in infile]
                await insert_records_in_batches(records, file_path, scylla_app)
        else:
            console.print(f"[red]Unsupported file type: {file_path}[/red]")
    except Exception as e:
        console.print(f"[red]Error processing file {file_path}: {e}[/red]")
async def load_single_file(scylla_app):
    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(title="Select a file", filetypes=[("CSV Files", "*.csv"), ("Text Files", "*.txt")])
    if file_path:
        await process_file(file_path, scylla_app)
    else:
        console.print("[yellow]No file selected.[/yellow]")
async def load_multiple_files(scylla_app):
    root = Tk()
    root.withdraw()  # Hide the root window
    file_paths = filedialog.askopenfilenames(title="Select files", filetypes=[("CSV Files", "*.csv"), ("Text Files", "*.txt")])
    if file_paths:
        for file_path in file_paths:
            await process_file(file_path, scylla_app)
    else:
        console.print("[yellow]No files selected.[/yellow]")
        
def partition_csv(file_path, chunksize=500000):
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'utf-16', 'utf-32']
    for encoding in encodings:
        try:
            reader = pd.read_csv(file_path, chunksize=chunksize, encoding=encoding, on_bad_lines='skip')
            break
        except UnicodeDecodeError:
            console.print(f"[yellow]Failed to decode file with encoding: {encoding}. Trying next encoding...[/yellow]")
            continue
    else:
        raise UnicodeDecodeError(f"Unable to decode the file with any of the provided encodings: {encodings}")

    with console.status("[bold green]Partitioning file...") as status:
        for chunk in reader:
            chunk['source'] = file_path
            yield chunk
            status.update(f"[bold green]Processed {chunk.index[-1]} rows")

def save_results_to_file(results, file_name="search_results.json"):
    try:
        with open(file_name, 'w') as f:
            json.dump(results, f, indent=4)
        console.print(f"[green]Results saved to {file_name}[/green]")
    except Exception as e:
        console.print(f"[red]Error saving results to file: {str(e)}[/red]")

async def search_scylla(search_input, scylla_app, max_results=None):
    try:
        query_conditions = []
        params = []

        # Define primary and alternative keys
        primary_and_alternative_keys = ["email", "first_name", "last_name", "phone_number", "username", "city", "state", "dob"]

        if ':' in search_input:
            field, value = search_input.split(':', 1)
            if field in primary_and_alternative_keys:
                query_conditions.append(f"{field} = '{value.lower()}'")
                query_conditions.append(f"{field} = '{value.capitalize()}'")
                query_conditions.append(f"{field} = '{value.upper()}'")
            else:
                query_conditions.append(f"data CONTAINS KEY '{field}' AND data['{field}'] = '{value.lower()}'")
                query_conditions.append(f"data CONTAINS KEY '{field}' AND data['{field}'] = '{value.capitalize()}'")
                query_conditions.append(f"data CONTAINS KEY '{field}' AND data['{field}'] = '{value.upper()}'")
        else:
            query_conditions.append(f"data CONTAINS '{search_input.lower()}'")
            query_conditions.append(f"data CONTAINS '{search_input.capitalize()}'")
            query_conditions.append(f"data CONTAINS '{search_input.upper()}'")

        # Ensure preceding primary key columns are included
        if 'city' in [cond.split('=')[0].strip() for cond in query_conditions]:
            if 'username' not in [cond.split('=')[0].strip() for cond in query_conditions]:
                # Prompt the user to enter the username
                username = input("Enter username: ")
                query_conditions.append(f"username = '{username.lower()}'")
                query_conditions.append(f"username = '{username.capitalize()}'")
                query_conditions.append(f"username = '{username.upper()}'")

        # Construct the queries
        queries = []
        for condition in query_conditions:
            if any(search_input.startswith(f"{key}:") for key in primary_and_alternative_keys):
                query = f"SELECT email, data FROM user_data WHERE {condition}"
            else:
                query = f"SELECT email, data FROM user_data WHERE {condition} ALLOW FILTERING"
            if max_results:
                query += f" LIMIT {max_results}"
            queries.append(query)

        results = []
        for query in queries:
            console.print(f"[cyan]Executing query: {query}[/cyan]")
            rows = scylla_app.session.execute(query)
            results.extend([{'email': row.email, **json.loads(row.data)} for row in rows if 'G:/Fraud/Breaches/NPD/' not in row.data])

        if results:
            console.print(f"[bold green]Found {len(results)} results:[/bold green]")
            for i, result in enumerate(results, 1):
                table = Table(title=f"Result {i}", box=box.ROUNDED)
                for key, value in result.items():
                    table.add_row(str(key), str(value))
                console.print(table)
                console.print()  # Add a blank line between results
            
            save_results = Prompt.ask(
                "Do you want to save the search results to a file?",
                choices=["y", "n"],
                default="n"
            ) == "y"
            
            if save_results:
                save_results_to_file(results)
        else:
            console.print("[yellow]No matching records found.[/yellow]")
            
    except Exception as e:
        console.print(f"[red]Error searching ScyllaDB: {str(e)}[/red]")
        if 'query' in locals():
            console.print(f"[yellow]Query attempted: {query}[yellow]")

    finally:
        gc.collect()
async def search_npd_data(scylla_app, max_results=None):
    try:
        query_conditions = []
        params = []

        # Prompt the user for required parameters
        first_name = input("Enter first name (or leave blank): ").strip()
        last_name = input("Enter last name (or leave blank): ").strip()
        city = input("Enter city (or leave blank): ").strip()

        # Ensure at least one required parameter is provided
        if not first_name and not last_name and not city:
            console.print("[red]Error: At least one of 'first_name', 'last_name', or 'city' must be provided.[/red]")
            return

        # Add conditions based on provided parameters
        if first_name:
            query_conditions.append("first_name = ?")
            params.append(first_name)
        if last_name:
            query_conditions.append("last_name = ?")
            params.append(last_name)
        if city:
            query_conditions.append("city = ?")
            params.append(city)

        # Construct the query
        query = f"SELECT email, data FROM user_data WHERE {' AND '.join(query_conditions)} ALLOW FILTERING"

        if max_results:
            query += f" LIMIT {max_results}"

        console.print(f"[cyan]Executing query: {query}[/cyan]")
        console.print(f"[cyan]With parameters: {params}[/cyan]")

        prepared_stmt = scylla_app.session.prepare(query)
        rows = scylla_app.session.execute(prepared_stmt, params)

        results = [{'email': row.email, **json.loads(row.data)} for row in rows]

        if results:
            console.print(f"[bold green]Found {len(results)} results:[/bold green]")
            for i, result in enumerate(results, 1):
                table = Table(title=f"Result {i}", box=box.ROUNDED)
                for key, value in result.items():
                    table.add_row(str(key), str(value))
                console.print(table)
                console.print()  # Add a blank line between results
            
            save_results = Prompt.ask(
                "Do you want to save the search results to a file?",
                choices=["y", "n"],
                default="n"
            ) == "y"
            
            if save_results:
                save_results_to_file(results)
        else:
            console.print("[yellow]No matching records found.[/yellow]")
            
    except Exception as e:
        console.print(f"[red]Error searching ScyllaDB: {str(e)}[/red]")
        if 'query' in locals():
            console.print(f"[yellow]Query attempted: {query}[yellow]")
        if 'params' in locals():
            console.print(f"[yellow]Parameters: {params}[yellow]")

    finally:
        gc.collect()
async def main():
    scylla_app = ScyllaApp()
    while True:
        console.print(Panel.fit(
            "[bold cyan]Select mode:[/bold cyan]\n"
            "1. Load a single file (CSV or TXT)\n"
            "2. Load all files in a directory (CSV or TXT)\n"
            "3. Load multiple selected files (CSV or TXT)\n"
            "4. Search ScyllaDB\n"
            "5. Search NPD Data\n"
            "6. Exit",
            title="ScyllaDB Data Manager",
            border_style="bold green"
        ))
        
        mode = Prompt.ask("Enter mode", choices=["1", "2", "3", "4", "5", "6"])

        if mode == '1':
            await load_single_file(scylla_app)
        elif mode == '2':
            await load_all_files(scylla_app)
        elif mode == '3':
            await load_multiple_files(scylla_app)
        elif mode == '4':
            search_input = input("Enter search terms (e.g., \"email:example@gmail.com\" or \"first_name:John\"): ")
            await search_scylla(search_input, scylla_app)
        elif mode == '5':
            await search_npd_data(scylla_app)
        elif mode == '6':
            console.print("[yellow]Exiting...[/yellow]")
            break
        else:
            console.print("[red]Invalid mode selected. Please try again.[/red]")

    scylla_app.close()

if __name__ == "__main__":
    asyncio.run(main())