==========================================
  UnknownDatabase Scanner - User Guide
==========================================

What is this?
-------------
A local search website for your database (.txt / NDJSON files).
Works completely offline on your own computer.
Fast, low memory usage. Search by name, phone, email, IBAN,
postal code, street or customer ID. Click a row to open the
full customer record including activity log and warnings.


STEP 1: Install Python (once)
------------------------------
Python is already installed on your system.
If you ever need to reinstall it:
1. Go to: https://www.python.org/downloads/
2. Download the Windows installer
3. Check: "Add Python to PATH"


STEP 2: Import a database (once per file)
------------------------------------------
  - Double-click menu.bat
  - Select option 2 (Import database)
  - Enter the full path to your .txt file
  - Give the database a name (e.g. "Odido")

NOTE: For large files (millions of records) this can take
      10-60 minutes. Keep the window open.
      This only needs to be done ONCE per file!

You can import multiple databases and search across all of them.


STEP 2b: Migrate an existing database (once)
---------------------------------------------
If you already have a database.db from an older version:
  - Double-click menu.bat
  - Select option 4 (Migrate database)
  - This adds the IBAN column and extracts phone numbers
    that were previously missed from the activity log

This is only needed if your database.db existed before the update.


STEP 3: Start the scanner
--------------------------
  - Double-click menu.bat
  - Select option 1 (Start scanner)
  - Your browser opens automatically at http://localhost:3000
  - Type a name, phone number, email, IBAN, postal code or
    customer ID to search


Search options
--------------
  All fields     - Search simultaneously across name, phone, email,
                   IBAN, postal code and street
  Name           - Search by first or last name
  Phone          - Search by (part of) a phone number
  Email          - Search by (part of) an email address
  IBAN           - Search by account number (e.g. "NL77RABO")
  City           - Search by city name
  Postal code    - Search by postal code (e.g. "1504" or "1504 HN")
  Street         - Search by street name
  Customer ID    - Search by Salesforce ID (exact match)
  Notes / Log    - Search in the full activity log and notes
                   (e.g. debt management, aggressive, deceased)


Customer record (detail view)
------------------------------
Click a row in the search results to open the full customer record.
You will see:

  - WARNING banner (red) if Flash_Message__c is set
    (e.g. "CUSTOMER IS AGGRESSIVE", "DECEASED")
  - Summary with name, phone, email, IBAN and address
  - All available fields grouped by category
  - Activity log as a timeline, each entry showing:
      * Date and time
      * Direction (Inbound / Outbound)
      * Agent ID
      * Action text
    Red entries = warning keywords found
    Blue entries = SMS sent


Tips
----
  - Search works on partial matches
    ("jan" finds "Jansen", "Jan de Boer")
  - Click a row to open the full customer record
  - Export results to CSV via the "Export CSV" button
  - The server runs locally, no internet connection needed
  - Press Escape or click outside the window to close the record


Files
-----
  menu.bat       - Main menu (double-click to start)
  server.py      - The web server
  import.py      - The import script
  migrate.py     - The migration script
  manage.py      - Database management (delete/list)
  index.html     - The search page (opened in browser)
  databases.json - List of all imported databases
  *.db           - SQLite databases (created after import)


System requirements
-------------------
  - Windows 10 or 11
  - Python 3.10+ (already installed on your system)
  - ~2x the size of your .txt file in free disk space
  - Low memory usage (<200 MB during use)


Troubleshooting
---------------
  "No databases found"   -> Run option 2 (Import) in menu.bat first
  "Port 3000 in use"     -> Close the other menu.bat window
  Import takes long      -> Normal for large files, let it run
  No IBAN column         -> Run option 4 (Migrate) in menu.bat
  Python not found       -> See https://www.python.org/downloads/

==========================================
