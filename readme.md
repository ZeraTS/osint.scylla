# Introduction ✍️

Welcome to the **osint.scylla** repository! This project leverages ScyllaDB to store large amounts of data efficiently. Initially, I experimented with different databases like MongoDB and MySQL for search queries yet felt dis-satisfied, and ultimately discovered this resource. My main goal was to understand how sites like IntelX and Snusbase handle breached information at massive scales with billions of lines. Feel free to explore the source code. While it's not my prettiest work, with some effort and tinkering, you could develop a REST API using the provided resource.

## Features ✨

- **Concurrent Processing:** Efficient multi-threading with `ThreadPoolExecutor`.
- **Data Handling:** Powerful data manipulation using `pandas` and CSV operations.
- **User Interface:** Simple GUI interactions with `tkinter`.
- **Progress Tracking:** Real-time progress bars with `tqdm`.
- **Rich Text Formatting:** Enhanced console output using `rich`.
- **Database Interaction:** Seamless integration with Cassandra using `cassandra-driver`.
- **Asynchronous I/O:** Non-blocking file operations with `asyncio` and `aiofiles`.
- **Memory Management:** Effective garbage collection with `gc`.
- **Multiprocessing:** Parallel processing capabilities with `multiprocessing`.

## Installation 🛠️

To get started with this project, follow these steps:

1. **Clone the repository:**

    ```bash
    git clone https://github.com/ZeraTS/osint.scylla
    cd OSINT.db
    ```

2. **Create a virtual environment:**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. **Install the dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

## Usage 🚀

To run the project, follow these steps:

1. **Run the main script:**

    ```bash
    python main.py
    ```

2. **Follow the on-screen instructions to [brief description of what the user should do].**
## Frequently Asked Questions 💭

<details>
<summary>What is ScyllaDB and why use it?</summary>

ScyllaDB is a high-performance NoSQL database that's compatible with Apache Cassandra but offers better performance and scalability. We use it because:
- Handles massive datasets efficiently
- Offers excellent read/write performance
- Supports concurrent operations effectively
- Compatible with Cassandra's ecosystem
</details>

<details>
<summary>How do I configure ScyllaDB for this project?</summary>

1. Install ScyllaDB on your system
2. Default configuration uses:
   - Host: localhost
   - Port: 9042
   - Keyspace: user_data
3. Modify these settings in the `ScyllaApp` class constructor if needed
</details>

<details>
<summary>What file formats are supported?</summary>

Currently supported formats:
- CSV files (*.csv)
- Text files (*.txt) (username:password) format

Data fields should contain at least one of these fields:
- email
- username
- first_name
- last_name
- phone_number
</details>

<details>
<summary>How do I optimize performance for large files?</summary>

The system automatically:
- Adjusts batch sizes based on file size
- Uses concurrent processing for large files
- Implements memory-efficient chunk processing

For best results:
- Keep files under 1GB per batch
- Ensure proper indexing in ScyllaDB
- Use SSD storage for the database
</details>

## Contributing 🤝

We welcome contributions! Please follow these steps to contribute:

1. **Fork the repository.**
2. **Create a new branch:**

    ```bash
    git checkout -b my-feature-branch
    ```

3. **Make your changes and commit them:**

    ```bash
    git commit -m 'Add some feature'
    ```

4. **Push to the branch:**

    ```bash
    git push origin my-feature-branch
    ```

5. **Create a pull request.**

## Contact 📬

If you have any questions, feel free to reach out to [sarasamrin55@gmail.com].

---

Made with ❤️ by Paul
