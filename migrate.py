"""
UnknownDatabase Scanner - Migration Script
Fixes the existing database.db:
  1. Adds iban column and populates it from stored JSON
  2. Extracts phone numbers from activity logs (fixes _is_real_phone bug)
  3. Creates / updates indexes
No extra packages required - uses only built-in Python modules.
"""
import sqlite3
import json
import re
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).parent

# Accept optional file argument (relative or absolute path)
if len(sys.argv) > 1:
    arg = sys.argv[1]
    DB_PATH = Path(arg) if Path(arg).is_absolute() else BASE_DIR / arg
else:
    DB_PATH = BASE_DIR / 'database.db'

if not DB_PATH.exists():
    print(f'\n[ERROR] File not found: {DB_PATH}')
    print('Run an import first.\n')
    input('Press Enter to exit...')
    exit(1)

# ─── Phone helpers (same fixed logic as import.py) ────────────────────────
_SMS_RE        = re.compile(r'SMS\s+verstuurd[:\s]+([+\d][\d\s\-]{6,20})', re.IGNORECASE)
_INTL_PHONE_RE = re.compile(r'\+31\d{9}')
_LOCAL_PHONE_RE = re.compile(r'(?<!\d)0[0-9]{9}(?!\d)')

def _is_real_phone(v):
    v = v.strip()
    if not v or len(v) > 25:
        return False
    if any(c.isalpha() for c in v):
        return False
    if '.' in v:       # reject decimals like "0.0", "2935.0"
        return False
    digits = sum(c.isdigit() for c in v)
    if digits < 7:
        return False
    if re.match(r'^\d{4}-\d{2}-\d{2}', v):   # reject dates
        return False
    return True

def _extract_phone_from_log(log):
    """Extract phone from SObjectLog__c activity log."""
    if not log or not isinstance(log, str):
        return ''
    m = _SMS_RE.search(log)
    if m:
        v = re.sub(r'[\s]', '', m.group(1)).strip()
        if _is_real_phone(v):
            return v
    m = _INTL_PHONE_RE.search(log)
    if m and _is_real_phone(m.group(0)):
        return m.group(0)
    m = _LOCAL_PHONE_RE.search(log)
    if m and _is_real_phone(m.group(0)):
        return m.group(0)
    return ''

# ─── Connect ───────────────────────────────────────────────────────────────
print(f'\n{"═"*44}')
print(f'  UnknownDatabase Scanner – Migration')
print(f'{"═"*44}\n')

conn = sqlite3.connect(str(DB_PATH))
c    = conn.cursor()
c.execute("PRAGMA journal_mode = WAL")
c.execute("PRAGMA synchronous  = OFF")
c.execute("PRAGMA cache_size   = 200000")
c.execute("PRAGMA temp_store   = MEMORY")

# ─── Step 1: Add iban column ───────────────────────────────────────────────
print('Step 1: Adding IBAN column...')
existing_cols = {row[1] for row in c.execute("PRAGMA table_info(accounts)").fetchall()}

if 'iban' not in existing_cols:
    c.execute("ALTER TABLE accounts ADD COLUMN iban TEXT DEFAULT ''")
    conn.commit()
    print('  Column added.')
else:
    print('  Column already exists.')

# ─── Step 2: Populate iban from JSON ──────────────────────────────────────
print('Step 2: Populating IBAN from JSON...')
start = time.time()

# Use SQLite json_extract for fast bulk update
try:
    c.execute("""
        UPDATE accounts
        SET    iban = TRIM(COALESCE(json_extract(data, '$.Bank_Account_Number__c'), ''))
        WHERE  (iban IS NULL OR iban = '')
          AND  data LIKE '%Bank_Account_Number%'
    """)
    iban_updated = c.rowcount
    conn.commit()
    print(f'  {iban_updated:,} IBAN records updated in {time.time()-start:.1f}s')
except Exception as e:
    print(f'  json_extract not available ({e}), Python fallback...')
    rows = c.execute(
        "SELECT rowid, data FROM accounts WHERE (iban IS NULL OR iban = '') "
        "AND data LIKE '%Bank_Account_Number%'"
    ).fetchall()
    batch = []
    for rowid, data_str in rows:
        try:
            data = json.loads(data_str)
            iban = (data.get('Bank_Account_Number__c') or '').strip()
            if not iban:
                for key in ('IBAN__c','BankAccountNumber__c','Bank_Account__c','IBAN','Iban__c'):
                    iban = (data.get(key) or '').strip()
                    if iban:
                        break
            if iban:
                batch.append((iban, rowid))
        except Exception:
            pass
    if batch:
        conn.executemany("UPDATE accounts SET iban = ? WHERE rowid = ?", batch)
        conn.commit()
    print(f'  {len(batch):,} IBAN records updated in {time.time()-start:.1f}s')

# ─── Step 3: Extract phone from activity logs ─────────────────────────────
print('Step 3: Extracting phone numbers from activity logs...')
print('  (This may take a few minutes...)')
start = time.time()

rows = c.execute(
    "SELECT rowid, data FROM accounts "
    "WHERE (phone IS NULL OR phone = '') AND data LIKE '%SObjectLog%'"
).fetchall()
print(f'  {len(rows):,} records to check...')

total_phone = 0
batch       = []

for rowid, data_str in rows:
    try:
        data  = json.loads(data_str)
        phone = ''

        # Check direct phone fields first (might be present in some records)
        for key in ('Phone','MobilePhone','HomePhone','OtherPhone','Mobile__c',
                    'PhoneNumber__c','Mobile','Telephone__c'):
            v = (data.get(key) or '').strip()
            if v and _is_real_phone(v):
                phone = v
                break

        # Fall back to SObjectLog
        if not phone:
            phone = _extract_phone_from_log(data.get('SObjectLog__c', ''))

        if phone:
            batch.append((phone, rowid))

        if len(batch) >= 5000:
            conn.executemany("UPDATE accounts SET phone = ? WHERE rowid = ?", batch)
            conn.commit()
            total_phone += len(batch)
            batch = []
            elapsed = time.time() - start
            print(f'\r  ...{total_phone:,} phone numbers updated ({elapsed:.0f}s)',
                  end='', flush=True)
    except Exception:
        pass

if batch:
    conn.executemany("UPDATE accounts SET phone = ? WHERE rowid = ?", batch)
    conn.commit()
    total_phone += len(batch)

elapsed = time.time() - start
print(f'\r  {total_phone:,} phone numbers updated in {elapsed:.1f}s            ')

# ─── Step 4: Update / create indexes ──────────────────────────────────────
print('Step 4: Creating indexes...')
for sql in [
    "CREATE INDEX IF NOT EXISTS idx_iban  ON accounts(iban  COLLATE NOCASE)",
    "CREATE INDEX IF NOT EXISTS idx_phone ON accounts(phone)",
    "CREATE INDEX IF NOT EXISTS idx_name  ON accounts(name  COLLATE NOCASE)",
    "CREATE INDEX IF NOT EXISTS idx_email ON accounts(email COLLATE NOCASE)",
]:
    c.execute(sql)
conn.commit()
print('  Done.')

# ─── Step 5: Summary ──────────────────────────────────────────────────────
c.execute("PRAGMA synchronous = NORMAL")
c.execute("PRAGMA wal_checkpoint(TRUNCATE)")

iban_count  = c.execute("SELECT COUNT(*) FROM accounts WHERE iban  <> ''").fetchone()[0]
phone_count = c.execute("SELECT COUNT(*) FROM accounts WHERE phone <> ''").fetchone()[0]
total       = c.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
conn.close()

print(f'\n{"═"*44}')
print(f'  Migration complete!')
print(f'{"═"*44}')
print(f'  Total records    : {total:,}')
print(f'  Records with IBAN: {iban_count:,}')
print(f'  Records with phone: {phone_count:,}')
print(f'\n  Run menu.bat and select option 1 to start searching!\n')
input('Press Enter to exit...')
