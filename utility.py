import pandas as pd
import sqlite3
import random
import tkinter as tk
from tkinter import filedialog, simpledialog, messagebox
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import os
import io
import csv

def read_random_line(file_path, encoding='utf-8'):
    try:
        with open(file_path, 'rb') as f:
            content = f.read().decode(encoding, errors='replace')
        df = pd.read_csv(io.StringIO(content))
    except UnicodeDecodeError:
        encodings = ['latin1', 'iso-8859-1', 'cp1252']
        for enc in encodings:
            try:
                with open(file_path, 'rb') as f:
                    content = f.read().decode(enc, errors='replace')
                df = pd.read_csv(io.StringIO(content))
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError("Unable to decode file with available encodings.")
    
    headers = df.columns.tolist()
    random_row = df.sample().iloc[0].tolist()
    return headers, random_row

def modify_header(file_path, encoding='utf-8'):
    try:
        df = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip')
    except pd.errors.ParserError:
        cleaned_data = []
        with open(file_path, 'r', encoding=encoding, errors='replace') as file:
            for line in file:
                cleaned_data.append(line.strip().split(','))
        df = pd.DataFrame(cleaned_data[1:], columns=cleaned_data[0])
    
    root = tk.Tk()
    root.withdraw()
    
    new_headers = []
    for col in df.columns:
        new_header = simpledialog.askstring("Input", f"Enter new header for column '{col}':", parent=root)
        if new_header is None:
            new_header = col  
        new_headers.append(new_header)
    
    df.columns = new_headers
    df.to_csv(file_path, index=False, encoding=encoding)
    messagebox.showinfo("Success", "Headers modified successfully.")
    root.destroy()

def is_valid_sqlite(file_path):
    try:
        conn = sqlite3.connect(file_path)
        conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        conn.close()
        return True
    except sqlite3.DatabaseError as e:
        print(f"Database error: {e}")
        return False

def convert_to_csv(file_path, output_path, encoding='utf-8'):
    chunk_size = 100000  # Adjust the chunk size as needed
    if file_path.endswith('.sql'):
        if is_valid_sqlite(file_path):
            try:
                conn = sqlite3.connect(file_path)
                query = "SELECT * FROM table_name"
                total_rows = pd.read_sql_query("SELECT COUNT(*) FROM table_name", conn).iloc[0, 0]
                with tqdm(total=total_rows, desc="Converting SQL to CSV") as pbar:
                    for chunk in pd.read_sql_query(query, conn, chunksize=chunk_size):
                        chunk.to_csv(output_path, mode='a', index=False, encoding=encoding, header=not os.path.exists(output_path))
                        pbar.update(len(chunk))
                conn.close()
            except Exception as e:
                print(f"Error converting SQL to CSV: {e}")
        else:
            raise ValueError("The file is not a valid SQLite database.")
    elif file_path.endswith('.xlsx'):
        total_rows = pd.read_excel(file_path, nrows=0).shape[0]
        with tqdm(total=total_rows, desc="Converting Excel to CSV") as pbar:
            for chunk in pd.read_excel(file_path, chunksize=chunk_size):
                chunk.to_csv(output_path, mode='a', index=False, encoding=encoding, header=not os.path.exists(output_path))
                pbar.update(len(chunk))
    elif file_path.endswith('.mybbsql'):
        import mysql.connector
        conn = mysql.connector.connect(user='username', password='password', host='localhost', database='database_name')
        raise ValueError(".mybbsql is not a supported file format")

def convert_text_to_csv(file_path, output_path, encoding='utf-8'):
    with open(file_path, 'r', encoding=encoding) as file:
        lines = file.readlines()
    
    data = [line.strip().split(':') for line in lines if ':' in line]
    df = pd.DataFrame(data, columns=['email', 'password'])
    df.to_csv(output_path, index=False, encoding=encoding)
    messagebox.showinfo("Success", "Text file converted to CSV successfully.")

def partition_csv(file_path, rows_per_file, encoding='utf-8'):
    base_name, ext = os.path.splitext(file_path)
    chunk_size = rows_per_file
    if ext == '.csv':
        with pd.read_csv(file_path, encoding=encoding, chunksize=chunk_size) as reader:
            for i, chunk in enumerate(reader):
                chunk.to_csv(f"{base_name}_part{i+1}{ext}", index=False, encoding=encoding)
    elif ext == '.txt':
        with open(file_path, 'r', encoding=encoding) as file:
            lines = file.readlines()
        header = "email,password\n"
        for i in range(0, len(lines), chunk_size):
            chunk_lines = lines[i:i + chunk_size]
            chunk_data = [line.strip().split(':') for line in chunk_lines if ':' in line]
            chunk_df = pd.DataFrame(chunk_data, columns=['email', 'password'])
            chunk_df.to_csv(f"{base_name}_part{i//chunk_size + 1}.csv", index=False, encoding=encoding)
    messagebox.showinfo("Success", f"File partitioned into chunks of {rows_per_file} rows each.")

def convert_messed_up_csv(file_path, output_path, encoding='utf-8'):
    with open(file_path, 'r', encoding=encoding) as file:
        lines = file.readlines()
    
    data = [line.strip().strip('[]').split(':') for line in lines if ':' in line]
    max_columns = max(len(row) for row in data)
    columns = [f'column{i+1}' for i in range(max_columns)]
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(output_path, index=False, encoding=encoding)
    messagebox.showinfo("Success", "Messed up CSV file converted successfully.")

def open_file_dialog():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    return file_path

def save_file_dialog():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
    return file_path

def main_menu():
    while True:
        print("1. Read a random line from a file")
        print("2. Modify header column names")
        print("3. Convert file to CSV")
        print("4. Convert text file to CSV (email:password)")
        print("5. Partition CSV or text file")
        print("6. Convert messed up CSV")
        print("7. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            file_path = open_file_dialog()
            encoding = input("Enter file encoding (default is utf-8): ") or 'utf-8'
            headers, random_line = read_random_line(file_path, encoding)
            print("Headers:", headers)
            print("Random line:", random_line)
        elif choice == '2':
            file_path = open_file_dialog()
            encoding = input("Enter file encoding (default is utf-8): ") or 'utf-8'
            modify_header(file_path, encoding)
        elif choice == '3':
            file_path = open_file_dialog()
            output_path = save_file_dialog()
            encoding = input("Enter file encoding (default is utf-8): ") or 'utf-8'
            try:
                with ThreadPoolExecutor() as executor:
                    future = executor.submit(convert_to_csv, file_path, output_path, encoding)
                    future.result()
                print("File converted to CSV successfully.")
            except ValueError as e:
                print(e)
        elif choice == '4':
            file_path = open_file_dialog()
            output_path = save_file_dialog()
            encoding = input("Enter file encoding (default is utf-8): ") or 'utf-8'
            convert_text_to_csv(file_path, output_path, encoding)
        elif choice == '5':
            file_path = open_file_dialog()
            rows_per_file = int(input("Enter the number of rows per file: "))
            encoding = input("Enter file encoding (default is utf-8): ") or 'utf-8'
            partition_csv(file_path, rows_per_file, encoding)
        elif choice == '6':
            file_path = open_file_dialog()
            output_path = save_file_dialog()
            encoding = input("Enter file encoding (default is utf-8): ") or 'utf-8'
            convert_messed_up_csv(file_path, output_path, encoding)
        elif choice == '7':
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main_menu()

#credit is given to Claude for assisting with code generation for this utilty script for conversion / editing / modification. 
