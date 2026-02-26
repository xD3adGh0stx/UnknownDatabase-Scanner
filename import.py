"""
UnknownDatabase Scanner - Import Script
Converts an NDJSON (.txt) file into a SQLite database.
No extra packages required - uses only built-in Python modules.
"""
import sqlite3
import json
import re
import sys
import time
from pathlib import Path

# ─── Email / phone extraction ──────────────────────────────────────────────
_EMAIL_RE      = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
# Patterns for extracting phone from activity log ("SMS verstuurd: +31618486761")
_SMS_RE        = re.compile(r'SMS\s+verstuurd[:\s]+([+\d][\d\s\-]{6,20})', re.IGNORECASE)
_INTL_PHONE_RE = re.compile(r'\+31\d{9}')
_LOCAL_PHONE_RE = re.compile(r'(?<!\d)0[0-9]{9}(?!\d)')

# Known phone field names in order of preference
_PHONE_FIELDS = (
    'Phone', 'Phone__c', 'MobilePhone', 'HomePhone', 'OtherPhone',
    'Mobile__c', 'PhoneNumber__c', 'Mobile', 'Telephone__c',
    'TelephoneNumber__c', 'ContactPhone__c', 'tel', 'telephone',
)

# Known email field names in order of preference
_EMAIL_FIELDS = (
    'vlocity_cmt__BillingEmailAddress__c', 'Email', 'Email__c',
    'PersonEmail', 'npe01__AlternateEmail__c', 'Personal_Email__c',
    'Contact_Email__c', 'BillingEmail', 'email', 'EmailAddress',
    'EmailAddress__c', 'ContactEmail__c', 'klant_email', 'e_mail',
)

# Known IBAN field names
_IBAN_FIELDS = (
    'Bank_Account_Number__c', 'IBAN__c', 'BankAccountNumber__c',
    'Bank_Account__c', 'IBAN', 'Iban__c',
)


def _is_real_phone(v):
    """Return True only if v looks like an actual phone number."""
    v = v.strip()
    if not v or len(v) > 25:
        return False
    # Must not contain letters — rules out Salesforce IDs like "0014H00001gbtcx"
    if any(c.isalpha() for c in v):
        return False
    # Must not be a decimal/float like "0.0", "1.0", "2935.0" — check for decimal point
    if '.' in v:
        return False
    # Must have at least 7 digits
    digits = sum(c.isdigit() for c in v)
    if digits < 7:
        return False
    # Must not look like a date ("2024-07-20")
    if re.match(r'^\d{4}-\d{2}-\d{2}', v):
        return False
    return True


def _extract_phone(obj):
    """Return the first genuine phone number, checking known fields then SObjectLog__c."""
    for key in _PHONE_FIELDS:
        v = obj.get(key, '')
        if v and isinstance(v, str) and _is_real_phone(v):
            return v.strip()
    # Parse SObjectLog__c for SMS entries: "SMS verstuurd: +31618486761"
    log = obj.get('SObjectLog__c', '')
    if log and isinstance(log, str):
        m = _SMS_RE.search(log)
        if m:
            v = re.sub(r'[\s]', '', m.group(1)).strip()
            if _is_real_phone(v):
                return v
        # Fallback: any international Dutch number in the log
        m = _INTL_PHONE_RE.search(log)
        if m and _is_real_phone(m.group(0)):
            return m.group(0)
        # Fallback: any local Dutch number in the log
        m = _LOCAL_PHONE_RE.search(log)
        if m and _is_real_phone(m.group(0)):
            return m.group(0)
    return ''


def _extract_email(obj):
    """Return the first email address found anywhere in the record."""
    # Check known email field names first
    for key in _EMAIL_FIELDS:
        v = obj.get(key, '')
        if v and isinstance(v, str) and '@' in v:
            m = _EMAIL_RE.search(v)
            if m:
                return m.group(0)
    # Explicitly parse SObjectLog__c (activity log — can be very large)
    log = obj.get('SObjectLog__c', '')
    if log and isinstance(log, str) and '@' in log:
        m = _EMAIL_RE.search(log)
        if m:
            return m.group(0)
    # Scan other short string values as fallback
    for k, v in obj.items():
        if k == 'SObjectLog__c':
            continue
        if isinstance(v, str) and '@' in v and 6 < len(v) < 500:
            # Skip Salesforce PhotoUrl fields
            if '/services/' in v or 'http' in v:
                continue
            m = _EMAIL_RE.search(v)
            if m:
                return m.group(0)
    return ''


def _extract_iban(obj):
    """Return the first IBAN / bank account number found."""
    for key in _IBAN_FIELDS:
        v = obj.get(key, '')
        if v and isinstance(v, str) and len(v) >= 10:
            return v.strip()
    return ''


BASE_DIR      = Path(__file__).parent
MANIFEST_PATH = BASE_DIR / 'databases.json'

# ─── Argument check ────────────────────────────────────────────────────────
if len(sys.argv) < 2:
    print('\nUsage: python import.py "path\\to\\file.txt" "Database naam"')
    print('Example: python import.py "C:\\Users\\alexd\\Downloads\\data.txt" "Odido"\n')
    input('Press Enter to exit...')
    sys.exit(1)

input_file = Path(sys.argv[1].strip('"'))
if not input_file.exists():
    print(f'\n[ERROR] File not found:\n  {input_file}\n')
    input('Press Enter to exit...')
    sys.exit(1)

file_size_mb = input_file.stat().st_size / 1024 / 1024

# ─── Database name ─────────────────────────────────────────────────────────
db_name = sys.argv[2].strip() if len(sys.argv) >= 3 else ''
if not db_name:
    print(f'\n  Bestand  : {input_file.name}')
    print(f'  Grootte  : {file_size_mb:.1f} MB\n')
    db_name = input('  Naam voor deze database (bijv. "Odido"): ').strip()
    if not db_name:
        db_name = 'Database 1'

safe_file = re.sub(r'[^\w\-]', '_', db_name) + '.db'
DB_PATH   = BASE_DIR / safe_file

# ─── Create database ───────────────────────────────────────────────────────
if DB_PATH.exists():
    DB_PATH.unlink()
    print(f'Bestaande {safe_file} verwijderd.')

conn = sqlite3.connect(str(DB_PATH))
c    = conn.cursor()

# Performance settings for bulk import
c.execute("PRAGMA journal_mode = WAL")
c.execute("PRAGMA synchronous  = OFF")
c.execute("PRAGMA cache_size   = 200000")
c.execute("PRAGMA temp_store   = MEMORY")
c.execute("PRAGMA mmap_size    = 536870912")

c.execute("""
    CREATE TABLE accounts (
        rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
        id        TEXT UNIQUE,
        name      TEXT,
        phone     TEXT,
        email     TEXT,
        iban      TEXT,
        street    TEXT,
        city      TEXT,
        postal    TEXT,
        country   TEXT,
        status    TEXT,
        segment   TEXT,
        is_active TEXT,
        brand     TEXT,
        created   TEXT,
        data      TEXT
    )
""")
conn.commit()

INSERT_SQL = """
    INSERT OR IGNORE INTO accounts
        (id, name, phone, email, iban, street, city, postal, country,
         status, segment, is_active, brand, created, data)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

# ─── Import ────────────────────────────────────────────────────────────────
count  = 0
errors = 0
batch  = []
BATCH  = 2000
start  = time.time()

print(f'\n{"═"*44}')
print(f'  UnknownDatabase Scanner – Import')
print(f'{"═"*44}')
print(f'\n  File    : {input_file.name}')
print(f'  Size    : {file_size_mb:.1f} MB')
print(f'\n  Importing records...\n')


def flush():
    global count
    if batch:
        c.executemany(INSERT_SQL, batch)
        conn.commit()
        count += len(batch)
        batch.clear()


def parse_row(obj):
    street = ' '.join(filter(None, [
        obj.get('BillingStreet') or obj.get('Street_Address__c', ''),
        str(obj['House_Number__c']) if obj.get('House_Number__c') else '',
        obj.get('House_Number_Extension__c', ''),
    ])).strip()

    # Store all non-empty fields
    compact = {k: v for k, v in obj.items() if v not in ('', None)}
    compact['Id']   = obj.get('Id', '')
    compact['Name'] = obj.get('Name', '')

    return (
        obj.get('Id', ''),
        obj.get('Name', ''),
        _extract_phone(obj),
        _extract_email(obj),
        _extract_iban(obj),
        street,
        obj.get('BillingCity') or obj.get('City__c', ''),
        obj.get('BillingPostalCode') or obj.get('Postal_Code__c', ''),
        obj.get('BillingCountry', ''),
        obj.get('vlocity_cmt__Status__c', ''),
        obj.get('Segment__c') or obj.get('Segment_Indicator__c', ''),
        obj.get('IsActive', ''),
        obj.get('Brand_Type__c', ''),
        obj.get('CreatedDate', ''),
        json.dumps(compact, ensure_ascii=False),
    )


with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            batch.append(parse_row(obj))
            if len(batch) >= BATCH:
                flush()
                elapsed = time.time() - start
                rate    = int(count / elapsed) if elapsed > 0 else 0
                print(f'\r  Processed: {count:>10,} records | {rate:>7,}/s | {elapsed:.0f}s elapsed',
                      end='', flush=True)
        except Exception:
            errors += 1

flush()  # Final batch
elapsed = time.time() - start
print(f'\r  Processed: {count:>10,} records | done in {elapsed:.1f}s                        ')

# ─── Create indexes ────────────────────────────────────────────────────────
print('\n  Creating indexes...')
for sql in [
    "CREATE INDEX IF NOT EXISTS idx_name   ON accounts(name   COLLATE NOCASE)",
    "CREATE INDEX IF NOT EXISTS idx_phone  ON accounts(phone)",
    "CREATE INDEX IF NOT EXISTS idx_city   ON accounts(city   COLLATE NOCASE)",
    "CREATE INDEX IF NOT EXISTS idx_postal ON accounts(postal)",
    "CREATE INDEX IF NOT EXISTS idx_id     ON accounts(id)",
    "CREATE INDEX IF NOT EXISTS idx_email  ON accounts(email  COLLATE NOCASE)",
    "CREATE INDEX IF NOT EXISTS idx_iban   ON accounts(iban   COLLATE NOCASE)",
]:
    c.execute(sql)
conn.commit()

# ─── Full-text search index ────────────────────────────────────────────────
print('  Building full-text search index (may take a few minutes)...')
try:
    c.execute("""
        CREATE VIRTUAL TABLE accounts_fts USING fts5(
            id, name, phone, email, iban, street, city, postal,
            content       = accounts,
            content_rowid = rowid,
            tokenize      = 'unicode61 remove_diacritics 1'
        )
    """)
    c.execute("INSERT INTO accounts_fts(accounts_fts) VALUES('rebuild')")
    conn.commit()
    print('  FTS5 index created successfully.')
except Exception as e:
    print(f'  Warning: FTS5 not available ({e}).')
    print('  LIKE-based search will still work.')

# ─── Finalize ──────────────────────────────────────────────────────────────
c.execute("PRAGMA synchronous = NORMAL")
c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
conn.close()

db_size_mb = DB_PATH.stat().st_size / 1024 / 1024

print(f'\n{"═"*44}')
print(f'  Import complete!')
print(f'{"═"*44}')
print(f'  Records imported : {count:,}')
if errors:
    print(f'  Skipped (errors) : {errors:,}')
print(f'  Database size    : {db_size_mb:.0f} MB')
print(f'  Time elapsed     : {elapsed:.1f}s')
# ─── Update databases.json manifest ───────────────────────────────────────
try:
    manifest = []
    if MANIFEST_PATH.exists():
        manifest = json.loads(MANIFEST_PATH.read_text('utf-8'))
    # Remove old entry with same name (re-import)
    manifest = [e for e in manifest if e['name'] != db_name]
    manifest.append({'name': db_name, 'file': safe_file})
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), 'utf-8')
    print(f'  databases.json bijgewerkt  ({len(manifest)} database(s) totaal)')
except Exception as e:
    print(f'  [WAARSCHUWING] databases.json niet bijgewerkt: {e}')

print(f'\n  Dubbelklik op start.bat om de scanner te starten!\n')
input('Press Enter to exit...')
