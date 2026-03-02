"""
UnknownDatabase Scanner - Recount Filter Statistics
Computes and caches filter counts (IBAN, notes, summons, etc.) in databases.json.
Run this once to make the Overzicht filter badges instant on every server start.
"""
import sqlite3
import json
import time
from pathlib import Path

BASE_DIR      = Path(__file__).parent
MANIFEST_PATH = BASE_DIR / 'databases.json'

print(f'\n{"═"*44}')
print(f'  UnknownDatabase Scanner – Recount')
print(f'{"═"*44}\n')

if not MANIFEST_PATH.exists():
    print('  [ERROR] databases.json not found. Import a database first.')
    input('  Press Enter to exit...')
    exit(1)

manifest = json.loads(MANIFEST_PATH.read_text('utf-8'))
if not manifest:
    print('  [ERROR] No databases found in databases.json.')
    input('  Press Enter to exit...')
    exit(1)

any_updated = False

for entry in manifest:
    name    = entry.get('name', '?')
    db_file = BASE_DIR / entry.get('file', '')

    if not db_file.exists():
        print(f'  [SKIP] "{name}" — file not found: {db_file.name}')
        continue

    print(f'  Database : {name}  ({db_file.name})')
    start = time.time()

    conn = sqlite3.connect(str(db_file))
    conn.execute("PRAGMA query_only = 1")
    conn.execute("PRAGMA cache_size = 100000")
    conn.execute("PRAGMA mmap_size  = 536870912")

    # Detect columns
    cols = {row[1] for row in conn.execute("PRAGMA table_info(accounts)").fetchall()}
    has_iban     = 'iban'       in cols
    has_flags    = 'f_notes'   in cols   # v3+ flag columns available
    has_deceased = 'f_deceased' in cols  # added later, check separately
    has_id_num   = 'id_number' in cols  # v4+ identity columns

    # Determine whether to trust flag columns.
    # v3+ databases have flags set during import, so trust them even if all = 0
    # (Contact databases legitimately have 0 notes/kvk/password).
    db_version = entry.get('version', 1)

    if db_version >= 3:
        use_flags    = has_flags
        use_deceased = has_deceased
    else:
        # Old DB — check if flags are actually populated
        use_flags = False
        if has_flags:
            n = conn.execute("SELECT COUNT(*) FROM accounts WHERE f_notes = 1").fetchone()[0]
            use_flags = n > 0
            if not use_flags:
                print(f'  [WARNING] Flag columns exist but are NOT populated (all = 0).')
                print(f'  Run option 4 (migrate database) on {db_file.name} for correct results!')
                print(f'  Recount is now using LIKE-queries as a temporary fallback.\n')
        use_deceased = False
        if has_deceased:
            n = conn.execute("SELECT COUNT(*) FROM accounts WHERE f_deceased = 1").fetchone()[0]
            use_deceased = n > 0

    counts = {}
    steps = [
        ('has_iban',     "SELECT COUNT(*) FROM accounts WHERE iban != '' AND iban IS NOT NULL"
                         if has_iban else None),
        ('has_notes',    "SELECT COUNT(*) FROM accounts WHERE f_notes = 1"
                         if use_flags else "SELECT COUNT(*) FROM accounts WHERE data LIKE '%SObjectLog__c%' OR data LIKE '%Flash_Message__c%'"),
        ('has_password', "SELECT COUNT(*) FROM accounts WHERE f_password = 1"
                         if use_flags else 'SELECT COUNT(*) FROM accounts WHERE data LIKE \'%"Password__c": "_%\' OR data LIKE \'%"Portal_Password__c": "_%\' OR data LIKE \'%"Wachtwoord__c": "_%\''),
        ('has_pincode',  "SELECT COUNT(*) FROM accounts WHERE f_pincode = 1"
                         if use_flags else 'SELECT COUNT(*) FROM accounts WHERE data LIKE \'%"Pin__c": "_%\' OR data LIKE \'%"Pincode__c": "_%\''),
        ('has_kvk',      "SELECT COUNT(*) FROM accounts WHERE f_kvk = 1"
                         if use_flags else 'SELECT COUNT(*) FROM accounts WHERE data LIKE \'%"Chamber_Of_Commerce_Number__c": "_%\' OR data LIKE \'%"KvK_Number__c": "_%\''),
        ('has_summons',  "SELECT COUNT(*) FROM accounts WHERE f_summons = 1"
                         if use_flags else "SELECT COUNT(*) FROM accounts WHERE data LIKE '%aanmaning%' OR data LIKE '%sommatie%' OR data LIKE '%incasso%'"),
        ('has_id_doc',   "SELECT COUNT(*) FROM accounts WHERE f_id_doc = 1 OR (id_number != '' AND id_number IS NOT NULL)"
                         if has_id_num else ("SELECT COUNT(*) FROM accounts WHERE f_id_doc = 1"
                         if use_flags else "SELECT COUNT(*) FROM accounts WHERE data LIKE '%rijbewijs%' OR data LIKE '%paspoort%' OR data LIKE '%identiteitsbewijs%'")),
        ('has_bsn',      "SELECT COUNT(*) FROM accounts WHERE data LIKE '%BSN%' OR data LIKE '%burgerservicenummer%'"),
        ('is_deceased',  "SELECT COUNT(*) FROM accounts WHERE f_deceased = 1"
                         if use_deceased else "SELECT COUNT(*) FROM accounts WHERE data LIKE '%overled%' OR data LIKE '%verlijden%' OR data LIKE '%nabestaand%' OR data LIKE '%gestorven%'"),
        ('is_active',    "SELECT COUNT(*) FROM accounts WHERE status = 'Active'"),
        ('is_inactive',  "SELECT COUNT(*) FROM accounts WHERE status = 'Inactive'"),
    ]

    for key, sql in steps:
        if sql is None:
            counts[key] = 0
            print(f'    {key:<15} = 0  (column missing, migrate first)')
            continue
        print(f'    {key:<15} ... ', end='', flush=True)
        n = conn.execute(sql).fetchone()[0]
        counts[key] = n
        print(f'{n:,}')

    conn.close()
    elapsed = time.time() - start
    print(f'  Done in {elapsed:.1f}s\n')

    entry['counts'] = counts
    any_updated = True

if any_updated:
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), 'utf-8')
    print(f'  databases.json updated with filter counts.')
    print(f'  Counts will be visible immediately next time the scanner starts.\n')
else:
    print('  Nothing updated.\n')

input('  Press Enter to exit...')
