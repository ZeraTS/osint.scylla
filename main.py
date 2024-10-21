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
from tqdm import tqdm
from datetime import datetime

# Rich console setup
console = Console()

# ScyllaDB connection setup
class ScyllaApp:
    def __init__(self, contact_points=['localhost'], port=32770, keyspace='user_data'):
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
          CREATE MATERIALIZED VIEW IF NOT EXISTS user_data_by_username AS
          SELECT * FROM user_data
          WHERE username IS NOT NULL AND email IS NOT NULL
          PRIMARY KEY (username, email)
          """)
        self.session.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS user_data_by_first_name AS
            SELECT * FROM user_data
            WHERE first_name IS NOT NULL AND email IS NOT NULL
            PRIMARY KEY (first_name, email)
        """)
        self.session.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS user_data_by_last_name AS
            SELECT * FROM user_data
            WHERE last_name IS NOT NULL AND email IS NOT NULL
            PRIMARY KEY (last_name, email)
        """)
        console.print("[green]Materialized views created[/green]")
     except Exception as e:
      console.print(f"[red]Error creating materialized views: {str(e)}[/red]")

    def create_table_if_not_exists(self):
        try:
            self.session.execute("""
               CREATE TABLE IF NOT EXISTS user_data (
                 primary_key text,
                 email text,
                 username text,
                 first_name text,
                 last_name text,
                 phone_number text,
                 data map<text, text>,
                 PRIMARY KEY ((email), username)
                );
            """)

            console.print("[green]Table 'user_data' created or already exists[/green]")
            console.print("[green]Indexes created[/green]")
        except Exception as e:
            console.print(f"[red]Error creating table or indexes: {str(e)}[/red]")

    def prepare_statements(self):
        try:
            self.insert_stmt = self.session.prepare("""
                INSERT INTO user_data (email, username, first_name, last_name, phone_number, data) VALUES (?, ?, ?, ?, ?, ?)
            """)
            self.select_stmt = self.session.prepare("SELECT * FROM user_data WHERE email = ?")
            console.print("[green]Prepared statements created[/green]")
        except Exception as e:
            console.print(f"[red]Error preparing statements: {str(e)}[/red]")
            raise

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

    def close(self):
        self.cluster.shutdown()
        console.print("[yellow]ScyllaDB connection closed[/yellow]")

# Initialize ScyllaApp
scylla_app = ScyllaApp()

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

async def search_scylla(search_input, max_results=None):
    try:
        query_conditions = []
        params = []

        # Define primary and alternative keys
        primary_and_alternative_keys = ["email", "first_name", "last_name", "phone_number", "username"]

        if ':' in search_input:
            field, value = search_input.split(':', 1)
            if field in primary_and_alternative_keys:
                query_conditions.append(f"{field} = ?")
                params.append(value)
            else:
                query_conditions.append(f"data CONTAINS KEY '{field}' AND data['{field}'] = ?")
                params.append(value)
        else:
            query_conditions.append("data CONTAINS ?")
            params.append(search_input)

        # Construct the query
        if any(search_input.startswith(f"{key}:") for key in primary_and_alternative_keys):
            query = f"SELECT email, data FROM user_data WHERE {' AND '.join(query_conditions)}"
        else:
            query = f"SELECT email, data FROM user_data WHERE {' AND '.join(query_conditions)} ALLOW FILTERING"

        if max_results:
            query += f" LIMIT {max_results}"

        console.print(f"[cyan]{datetime.now()} - Executing query: {query}[/cyan]")
        console.print(f"[cyan]{datetime.now()} - With parameters: {params}[/cyan]")

        prepared_stmt = scylla_app.session.prepare(query)
        rows = scylla_app.session.execute(prepared_stmt, params)

        results = [{'email': row.email, **row.data} for row in rows]

        if results:
            console.print(f"[bold green]{datetime.now()} - Found {len(results)} results:[/bold green]")
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
            console.print(f"[yellow]{datetime.now()} - No matching records found.[/yellow]")
            
    except Exception as e:
        console.print(f"[red]{datetime.now()} - Error searching ScyllaDB: {str(e)}[/red]")
        if 'query' in locals():
            console.print(f"[yellow]Query attempted: {query}[yellow]")
        if 'params' in locals():
            console.print(f"[yellow]Parameters: {params}[yellow]")

    finally:
        gc.collect()
async def insert_records_in_batches(records, file_path, batch_size=50):
    try:
        tasks = []
        total_batches = (len(records) + batch_size - 1) // batch_size
        with tqdm(total=total_batches, desc="Inserting records", unit="batch") as pbar:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                tasks.append(insert_batch(batch, file_path, pbar))
            await asyncio.gather(*tasks)
    except Exception as e:
        console.print(f"[red]Error inserting records in batches: {e}[/red]")

async def insert_data_to_scylla(file_path, batch_size=50):  # Reduced batch size to 50
    try:
        if isinstance(file_path, str) and file_path.endswith('.txt'):
            for chunk_df in read_combolist_txt(file_path):
                records = chunk_df.to_dict(orient='records')
                await insert_records_in_batches(records, file_path, batch_size)
        elif isinstance(file_path, str) and file_path.endswith('.csv'):
            df = read_malformed_csv(file_path)
            records = df.to_dict(orient='records')
            await insert_records_in_batches(records, file_path, batch_size)
        else:
            raise ValueError("Invalid file path or unsupported file type")
        console.print(f"[green]Data inserted successfully from: {file_path}[/green]")
    except Exception as e:
        console.print(f"[red]Error inserting data into ScyllaDB: {e}[/red]")
        console.print(f"[yellow]Error details: {str(e)}[/yellow]")
    finally:
        gc.collect()
async def insert_batch(batch, file_path, pbar):
    skipped_count = 0
    batch_stmt = BatchStatement()
    for record in batch:
        record['source'] = file_path
        email = convert_to_string(get_value_from_record(record, ['email', 'mail', 'e-mail address', 'e-mail', 'email_address', 'emailaddress', 'email-address', 'email address', 'user_email', 'useremail', 'user-email', 'user email', 'user_id', 'userid', 'user-id', 'user id', 'account', 'acct', 'account_id', 'accountid', 'account-id', 'account id', 'account_number', 'accountnumber', 'account-number', 'account number'])) 
        username = convert_to_string(get_value_from_record(record, ['username', 'user_name', 'user', 'login', 'user_id', 'userid', 'user-id', 'user id', 'account', 'acct', 'account_id', 'accountid', 'account-id', 'account id', 'account_number', 'accountnumber', 'account-number', 'account number']))
        first_name = convert_to_string(get_value_from_record(record, ['first_name', 'first name' , 'first', 'fname', 'f_name', 'given_name', 'given', 'gname', 'g_name', 'forename', 'fore_name', 'fore name', 'name', 'name_first', 'namefirst', 'name first', 'name_given', 'namegiven', 'name given']))
        last_name = convert_to_string(get_value_from_record(record, ['last_name', 'last name', 'last', 'lname', 'l_name', 'family_name', 'family', 'sname', 's_name', 'surname', 'sur_name']))
        phone_number = convert_to_string(get_value_from_record(record, ['phone_number', 'phone number', 'call', 'phone', 'telephone', 'contact', 'number', 'cell', 'mobile', 'cellphone', 'cellular']))

       
        # Use email, username, first_name, last_name, or phone_number as the primary key
        primary_keys = []
        if email:
            primary_keys.append(email)
        if username:
            primary_keys.append(username)
        if first_name and last_name:
            primary_keys.append(f"{first_name} {last_name}")
        if phone_number:
            primary_keys.append(phone_number)

        if not primary_keys:
            skipped_count += 1
            continue

        # Convert all values to strings and remove empty values
        data = {k: convert_to_string(v) for k, v in record.items()}
        data = {k: v for k, v in data.items() if v}

        try:
            for primary_key in primary_keys:
                batch_stmt.add(scylla_app.insert_stmt, (email, data))
        except Exception as e:
            console.print(f"[red]Error adding record to batch: {e}[/red]")
            console.print(f"[yellow]Problematic record: {record}[/yellow]")

    try:
        scylla_app.session.execute(batch_stmt)
        pbar.update(1)  # Update progress bar
    except Exception as e:
        console.print(f"[red]Error executing batch: {e}[/red]")

    if skipped_count > 0:
        console.print(f"[yellow]Skipped {skipped_count} records due to missing primary key.[/yellow]")
def save_results_to_file(results):
    Tk().withdraw()
    
    file_path = filedialog.asksaveasfilename(
        title="Save Results",
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
    )
    
    if file_path:
        df = pd.DataFrame(results)
        df.to_csv(file_path, index=False)
        console.print(f"[green]Results saved to {file_path}[/green]")
    
def save_results_to_file(results):
    Tk().withdraw()
    
    file_path = filedialog.asksaveasfilename(
        title="Save Results",
        defaultextension=".csv",
        filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
    )
    
    if file_path:
        df = pd.DataFrame(results)
        df.to_csv(file_path, index=False)
        console.print(f"[green]Results saved to {file_path}[/green]")
    else:
        console.print("[yellow]Save operation cancelled.[/yellow]")

def read_combolist_txt(file_path, chunk_size=10000):
    def chunk_generator(file, chunk_size):
        chunk = []
        for line in file:
            chunk.append(line.strip())
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        for chunk in chunk_generator(file, chunk_size):
            chunk_data = []
            for line in chunk:
                try:
                    # Handle the {email},{password} format
                    email, password = line.split(',', 1)
                    email = email.strip().strip('"')
                    password = password.strip().strip('"')
                    chunk_data.append({'email': email, 'hashed_password': password})
                except ValueError:
                    console.print(f"[yellow]Skipping malformed line: {line}[/yellow]")
                    continue
            yield pd.DataFrame(chunk_data)
            
def partition_and_insert_txt(file_path, chunk_size=10000):
    for chunk_df in read_combolist_txt(file_path, chunk_size):
        records = chunk_df.to_dict(orient='records')
        asyncio.run(insert_records_in_batches(records, file_path))
def load_single_file():
    Tk().withdraw()
    file_path = filedialog.askopenfilename(
        title="Select a File",
        filetypes=[("CSV Files", "*.csv"), ("Text Files", "*.txt")]
    )
    if file_path:
        if file_path.endswith('.txt'):
            partition_and_insert_txt(file_path)
        elif file_path.endswith('.csv'):
            asyncio.run(insert_data_to_scylla(file_path))
        else:
            console.print("[red]Unsupported file type selected.[/red]")
    else:
        console.print("[yellow]No file selected.[/yellow]")
def load_all_files():
    Tk().withdraw()
    directory = filedialog.askdirectory(title="Select Directory Containing Files")
    if directory:
        for file_name in os.listdir(directory):
            if file_name.endswith(".csv") or file_name.endswith(".txt"):
                file_path = os.path.join(directory, file_name)
                if file_path.endswith('.txt'):
                    partition_and_insert_txt(file_path)
                elif file_path.endswith('.csv'):
                    asyncio.run(insert_data_to_scylla(file_path))
                else:
                    console.print(f"[red]Unsupported file type: {file_path}[/red]")
    else:
        console.print("[yellow]No directory selected.[/yellow]")
def load_multiple_files():
    Tk().withdraw()
    file_paths = filedialog.askopenfilenames(
        title="Select Files",
        filetypes=[("CSV Files", "*.csv"), ("Text Files", "*.txt")]
    )
    if file_paths:
        for file_path in file_paths:
            if file_path.endswith('.txt'):
                partition_and_insert_txt(file_path)
            elif file_path.endswith('.csv'):
                asyncio.run(insert_data_to_scylla(file_path))
            else:
                console.print(f"[red]Unsupported file type: {file_path}[/red]")
    else:
        console.print("[yellow]No files selected.[/yellow]")
def main():
    try:
        console.print("[cyan]Initializing ScyllaApp...[/cyan]")
        scylla_app = ScyllaApp()
        console.print("[green]ScyllaApp initialized successfully[/green]")

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
            
            mode = Prompt.ask("Enter mode", choices=["1", "2", "3", "4", "5"])

            if mode == '1':
                load_single_file()
            elif mode == '2':
                load_all_files()
            elif mode == '3':
                load_multiple_files()
            elif mode == '4':
                 search_input = input("Enter search terms (e.g., \"email:example@gmail.com\" or \"first_name:John\"): ")
                 asyncio.run(search_scylla(search_input))
            elif mode == '5':
                console.print("[yellow]Exiting...[/yellow]")
                break
            else:
                console.print("[red]Invalid mode selected. Please try again.[/red]")
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
    finally:
        scylla_app.close()

if __name__ == "__main__":
    main()
