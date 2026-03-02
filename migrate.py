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

BASE_DIR          = Path(__file__).parent
CURRENT_VERSION   = 4  # After migration, set to this version

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
    v = str(v).strip()
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

# ─── Step 5: Add flag columns for fast filtering ───────────────────────────
print('Step 5: Adding filter flag columns for fast browse...')
existing_cols = {row[1] for row in c.execute("PRAGMA table_info(accounts)").fetchall()}

flag_cols = ['f_notes', 'f_kvk', 'f_password', 'f_pincode', 'f_id_doc', 'f_summons', 'f_deceased']
for col in flag_cols:
    if col not in existing_cols:
        c.execute(f"ALTER TABLE accounts ADD COLUMN {col} INTEGER DEFAULT 0")
conn.commit()

# Populate flag columns from existing data
start = time.time()
print('  Populating f_notes...')
c.execute("UPDATE accounts SET f_notes = 1 WHERE (data LIKE '%SObjectLog__c%' OR data LIKE '%Flash_Message__c%') AND f_notes = 0")
conn.commit()
print(f'    {c.rowcount:,} records ({time.time()-start:.0f}s)')

start = time.time()
print('  Populating f_kvk...')
c.execute("""UPDATE accounts SET f_kvk = 1
    WHERE (data LIKE '%"Chamber_Of_Commerce_Number__c": "_%'
        OR data LIKE '%"KvK_Number__c": "_%') AND f_kvk = 0""")
conn.commit()
print(f'    {c.rowcount:,} records ({time.time()-start:.0f}s)')

start = time.time()
print('  Populating f_password...')
c.execute("""UPDATE accounts SET f_password = 1
    WHERE (data LIKE '%"Password__c": "_%'
        OR data LIKE '%"Portal_Password__c": "_%'
        OR data LIKE '%"Wachtwoord__c": "_%') AND f_password = 0""")
conn.commit()
print(f'    {c.rowcount:,} records ({time.time()-start:.0f}s)')

start = time.time()
print('  Populating f_pincode...')
c.execute("""UPDATE accounts SET f_pincode = 1
    WHERE (data LIKE '%"Pin__c": "_%'
        OR data LIKE '%"Pincode__c": "_%') AND f_pincode = 0""")
conn.commit()
print(f'    {c.rowcount:,} records ({time.time()-start:.0f}s)')

start = time.time()
print('  Populating f_id_doc (driver license/passport in logs)...')
c.execute("""UPDATE accounts SET f_id_doc = 1
    WHERE f_notes = 1 AND f_id_doc = 0
      AND (data LIKE '%rijbewijs%' OR data LIKE '%paspoort%'
        OR data LIKE '%identiteitsbewijs%' OR data LIKE '%identiteitskaart%'
        OR data LIKE '%ID-kaart%' OR data LIKE '%ID kaart%')""")
conn.commit()
print(f'    {c.rowcount:,} records ({time.time()-start:.0f}s)')

start = time.time()
print('  Populating f_summons (summons/collection in logs)...')
c.execute("""UPDATE accounts SET f_summons = 1
    WHERE (data LIKE '%aanmaning%' OR data LIKE '%sommatie%'
        OR data LIKE '%incasso%' OR data LIKE '%deurwaarder%'
        OR data LIKE '%ingebrekestell%') AND f_summons = 0""")
conn.commit()
print(f'    {c.rowcount:,} records ({time.time()-start:.0f}s)')

start = time.time()
print('  Populating f_deceased (deceased mentions in logs)...')
c.execute("""UPDATE accounts SET f_deceased = 1
    WHERE f_notes = 1 AND f_deceased = 0
      AND (data LIKE '%overled%' OR data LIKE '%verlijden%'
        OR data LIKE '%nabestaand%' OR data LIKE '%gestorven%'
        OR data LIKE '%overlijden%')""")
conn.commit()
print(f'    {c.rowcount:,} records ({time.time()-start:.0f}s)')

# Create indexes on flag columns + status index
print('  Creating flag indexes...')
c.execute("CREATE INDEX IF NOT EXISTS idx_status ON accounts(status)")
for col in flag_cols:
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_{col} ON accounts({col})")
conn.commit()
print('  Flag columns done.')

# ─── Step 5b: Add v4 identity columns ─────────────────────────────────────
print('Step 5b: Adding identity columns (id_number, id_type, etc.)...')
existing_cols = {row[1] for row in c.execute("PRAGMA table_info(accounts)").fetchall()}

id_cols = ['id_number', 'id_type', 'id_valid', 'birthdate', 'nationality', 'gender']
for col in id_cols:
    if col not in existing_cols:
        c.execute(f"ALTER TABLE accounts ADD COLUMN {col} TEXT DEFAULT ''")
conn.commit()

# Populate identity columns from stored JSON using json_extract
start = time.time()
print('  Extracting ID numbers from JSON...')
try:
    c.execute("""
        UPDATE accounts SET
            id_number   = COALESCE(TRIM(json_extract(data, '$.ID_number__c')), ''),
            id_type     = COALESCE(TRIM(json_extract(data, '$.ID_type__c')), ''),
            id_valid    = COALESCE(TRIM(json_extract(data, '$.ID_valid__c')), ''),
            birthdate   = COALESCE(TRIM(COALESCE(json_extract(data, '$.Birthdate'), json_extract(data, '$.BirthDate__c'))), ''),
            nationality = COALESCE(TRIM(json_extract(data, '$.Nationality__c')), ''),
            gender      = COALESCE(TRIM(COALESCE(json_extract(data, '$.Gender__c'), json_extract(data, '$.vlocity_cmt__Gender__c'))), '')
        WHERE (id_number IS NULL OR id_number = '')
          AND (data LIKE '%ID_number__c%' OR data LIKE '%Birthdate%' OR data LIKE '%Nationality__c%')
    """)
    id_updated = c.rowcount
    conn.commit()
    print(f'  {id_updated:,} records updated in {time.time()-start:.1f}s')
except Exception as e:
    print(f'  json_extract failed ({e}), using Python fallback...')
    rows = c.execute(
        "SELECT rowid, data FROM accounts WHERE (id_number IS NULL OR id_number = '') "
        "AND (data LIKE '%ID_number__c%' OR data LIKE '%Birthdate%' OR data LIKE '%Nationality__c%')"
    ).fetchall()
    batch = []
    for rowid, data_str in rows:
        try:
            data = json.loads(data_str)
            id_num  = (data.get('ID_number__c') or '').strip()
            id_tp   = (data.get('ID_type__c') or '').strip()
            id_vl   = (data.get('ID_valid__c') or '').strip()
            bdate   = (data.get('Birthdate') or data.get('BirthDate__c') or '').strip()
            nat     = (data.get('Nationality__c') or '').strip()
            gen     = (data.get('Gender__c') or data.get('vlocity_cmt__Gender__c') or '').strip()
            if id_num or bdate or nat:
                batch.append((id_num, id_tp, id_vl, bdate, nat, gen, rowid))
        except Exception:
            pass
    if batch:
        conn.executemany(
            "UPDATE accounts SET id_number=?, id_type=?, id_valid=?, birthdate=?, nationality=?, gender=? WHERE rowid=?",
            batch)
        conn.commit()
    print(f'  {len(batch):,} records updated in {time.time()-start:.1f}s')

# Also update f_id_doc for records with id_number
start = time.time()
print('  Updating f_id_doc for records with ID numbers...')
c.execute("UPDATE accounts SET f_id_doc = 1 WHERE id_number != '' AND id_number IS NOT NULL AND f_id_doc = 0")
conn.commit()
print(f'  {c.rowcount:,} records ({time.time()-start:.0f}s)')

# Create index on id_number
c.execute("CREATE INDEX IF NOT EXISTS idx_id_number ON accounts(id_number COLLATE NOCASE)")
conn.commit()
print('  Identity columns done.')

# ─── Step 6: Compute filter counts ────────────────────────────────────────
print('Step 6: Computing filter counts...')
filter_counts = {}
try:
    filter_counts['has_iban']     = c.execute("SELECT COUNT(*) FROM accounts WHERE iban != '' AND iban IS NOT NULL").fetchone()[0]
    filter_counts['has_notes']    = c.execute("SELECT COUNT(*) FROM accounts WHERE f_notes = 1").fetchone()[0]
    filter_counts['has_password'] = c.execute("SELECT COUNT(*) FROM accounts WHERE f_password = 1").fetchone()[0]
    filter_counts['has_pincode']  = c.execute("SELECT COUNT(*) FROM accounts WHERE f_pincode = 1").fetchone()[0]
    filter_counts['has_kvk']      = c.execute("SELECT COUNT(*) FROM accounts WHERE f_kvk = 1").fetchone()[0]
    filter_counts['has_summons']  = c.execute("SELECT COUNT(*) FROM accounts WHERE f_summons = 1").fetchone()[0]
    filter_counts['has_id_doc']   = c.execute("SELECT COUNT(*) FROM accounts WHERE f_id_doc = 1 OR (id_number != '' AND id_number IS NOT NULL)").fetchone()[0]
    filter_counts['has_bsn']      = c.execute("SELECT COUNT(*) FROM accounts WHERE data LIKE '%BSN%' OR data LIKE '%burgerservicenummer%'").fetchone()[0]
    filter_counts['is_deceased']  = c.execute("SELECT COUNT(*) FROM accounts WHERE f_deceased = 1").fetchone()[0]
    filter_counts['is_active']    = c.execute("SELECT COUNT(*) FROM accounts WHERE status = 'Active'").fetchone()[0]
    filter_counts['is_inactive']  = c.execute("SELECT COUNT(*) FROM accounts WHERE status = 'Inactive'").fetchone()[0]
    print(f'  Done. IBAN={filter_counts["has_iban"]:,}  Notes={filter_counts["has_notes"]:,}  KvK={filter_counts["has_kvk"]:,}')
except Exception as e:
    print(f'  Warning: {e}')
    filter_counts = {}

# ─── Step 7: Summary ──────────────────────────────────────────────────────
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

# ─── Update manifest version ──────────────────────────────────────────────────
MANIFEST_PATH = BASE_DIR / 'databases.json'
try:
    if MANIFEST_PATH.exists():
        manifest = json.loads(MANIFEST_PATH.read_text('utf-8'))
        db_file_name = DB_PATH.name
        for entry in manifest:
            if entry['file'] == db_file_name:
                entry['version'] = CURRENT_VERSION
                entry['counts']  = filter_counts
                break
        MANIFEST_PATH.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), 'utf-8')
        print(f'  Version updated to v{CURRENT_VERSION} in databases.json')
except Exception as e:
    print(f'  [WARNING] Could not update version: {e}')

print(f'\n  Run menu.bat and select option 1 to start searching!\n')
input('Press Enter to exit...')
