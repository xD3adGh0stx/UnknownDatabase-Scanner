"""
UnknownDatabase Scanner - Import Script
Converts an NDJSON (.txt) file into a SQLite database.
Uses multiprocessing: each worker reads its own file segment and writes
parsed rows to a flat binary file (pickle).  The main process then bulk-
inserts from those files into the final SQLite DB.
Automatically adapts to the host machine's CPU and RAM.
"""
import sqlite3
import json
import re
import sys
import time
import os
import shutil
import tempfile
import pickle
import multiprocessing as mp
from pathlib import Path

# orjson is ~5x faster than stdlib json — use it when available
try:
    import orjson
    _json_loads = orjson.loads          # accepts bytes directly (no decode needed)
    def _json_dumps(obj):
        return orjson.dumps(obj).decode('utf-8')
except ImportError:
    _json_loads = json.loads
    def _json_dumps(obj):
        return json.dumps(obj, ensure_ascii=False)


# ─── System resource detection ──────────────────────────────────────────────

def _get_total_ram_mb():
    """Return total physical RAM in MB.  Works on Windows, Linux, macOS."""
    try:
        if sys.platform == 'win32':
            import ctypes
            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ('dwLength',                ctypes.c_ulong),
                    ('dwMemoryLoad',            ctypes.c_ulong),
                    ('ullTotalPhys',            ctypes.c_ulonglong),
                    ('ullAvailPhys',            ctypes.c_ulonglong),
                    ('ullTotalPageFile',        ctypes.c_ulonglong),
                    ('ullAvailPageFile',        ctypes.c_ulonglong),
                    ('ullTotalVirtual',         ctypes.c_ulonglong),
                    ('ullAvailVirtual',         ctypes.c_ulonglong),
                    ('ullAvailExtendedVirtual', ctypes.c_ulonglong),
                ]
            stat = MEMORYSTATUSEX(dwLength=ctypes.sizeof(MEMORYSTATUSEX))
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            return stat.ullTotalPhys // (1024 * 1024)
        else:
            # Linux / macOS
            import resource
            mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            return mem_bytes // (1024 * 1024)
    except Exception:
        return 4096          # safe fallback: assume 4 GB


def _compute_settings():
    """Return tuned settings based on available CPU and RAM."""
    ram_mb   = _get_total_ram_mb()
    num_cpus = os.cpu_count() or 2

    # Workers: leave 1 core free on machines with <=4 cores
    if num_cpus <= 2:
        workers = 1
    elif num_cpus <= 4:
        workers = num_cpus - 1
    else:
        workers = num_cpus

    # Memory budget: use at most 50% of RAM for the import
    budget_mb = max(512, ram_mb // 2)

    # Read buffer per worker (capped between 1 MB and 16 MB)
    read_buf = min(16, max(1, budget_mb // (workers * 8))) * 1024 * 1024

    # Pickle batch size per worker (capped between 5K and 50K)
    flush_size = min(50_000, max(5_000, budget_mb * 30))

    # SQLite page cache (25% of budget, in negative-KB notation)
    cache_kb = max(64_000, (budget_mb * 1024) // 4)
    cache_pragma = -cache_kb

    # SQLite mmap (capped at budget or 4 GB, whichever is smaller)
    mmap_bytes = min(4 * 1024**3, budget_mb * 1024 * 1024)

    # Commit interval scales with RAM
    commit_every = min(1_000_000, max(100_000, budget_mb * 500))

    return {
        'workers':      workers,
        'read_buf':     read_buf,
        'flush_size':   flush_size,
        'cache_pragma': cache_pragma,
        'mmap_bytes':   mmap_bytes,
        'commit_every': commit_every,
        'ram_mb':       ram_mb,
    }

# ─── Email / phone extraction ──────────────────────────────────────────────
_EMAIL_RE       = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
_SMS_RE         = re.compile(r'SMS\s+verstuurd[:\s]+([+\d][\d\s\-]{6,20})', re.IGNORECASE)
_INTL_PHONE_RE  = re.compile(r'\+31\d{9}')
_LOCAL_PHONE_RE = re.compile(r'(?<!\d)0[0-9]{9}(?!\d)')
_HAS_ALPHA_RE   = re.compile(r'[a-zA-Z]')
_DATE_PREFIX_RE = re.compile(r'^\d{4}-\d{2}-\d{2}')
_WHITESPACE_RE  = re.compile(r'[\s]')
_SUMMONS_RE     = re.compile(r'aanmaning|sommatie|incasso|deurwaarder|ingebrekestell', re.I)
_IDDOC_RE       = re.compile(r'rijbewijs|paspoort|identiteitsbewijs|identiteitskaart|ID-kaart|ID kaart', re.I)
_DECEASED_RE    = re.compile(r'overled|verlijden|nabestaand|gestorven|overlijden', re.I)
_EMPTY_VALS     = frozenset(('', None))

_PHONE_FIELDS = (
    'Phone', 'Phone__c', 'MobilePhone', 'HomePhone', 'OtherPhone',
    'Mobile__c', 'PhoneNumber__c', 'Mobile', 'Telephone__c',
    'TelephoneNumber__c', 'ContactPhone__c', 'tel', 'telephone',
)
_EMAIL_FIELDS = (
    'vlocity_cmt__BillingEmailAddress__c', 'Email', 'Email__c',
    'PersonEmail', 'npe01__AlternateEmail__c', 'Personal_Email__c',
    'Contact_Email__c', 'BillingEmail', 'email', 'EmailAddress',
    'EmailAddress__c', 'ContactEmail__c', 'klant_email', 'e_mail',
)
_IBAN_FIELDS = (
    'Bank_Account_Number__c', 'IBAN__c', 'BankAccountNumber__c',
    'Bank_Account__c', 'IBAN', 'Iban__c',
)

_COL_LIST = (
    'id, name, phone, email, iban, street, city, postal, country, '
    'status, segment, is_active, brand, created, data, '
    'id_number, id_type, id_valid, birthdate, nationality, gender, '
    'f_notes, f_kvk, f_password, f_pincode, f_id_doc, f_summons, f_deceased'
)
_COL_PLACEHOLDERS = ','.join('?' * 28)


# ─── Field extraction helpers ─────────────────────────────────────────────

def _is_real_phone(v):
    v = str(v).strip()
    if not v or len(v) > 25:
        return False
    if _HAS_ALPHA_RE.search(v):
        return False
    if '.' in v:
        return False
    if sum(c.isdigit() for c in v) < 7:
        return False
    if _DATE_PREFIX_RE.match(v):
        return False
    return True


def _extract_phone(obj):
    for key in _PHONE_FIELDS:
        v = obj.get(key, '')
        if v is not None and v != '' and _is_real_phone(v):
            return str(v).strip()
    log = obj.get('SObjectLog__c', '')
    if log and isinstance(log, str):
        m = _SMS_RE.search(log)
        if m:
            v = _WHITESPACE_RE.sub('', m.group(1)).strip()
            if _is_real_phone(v):
                return v
        m = _INTL_PHONE_RE.search(log)
        if m and _is_real_phone(m.group(0)):
            return m.group(0)
        m = _LOCAL_PHONE_RE.search(log)
        if m and _is_real_phone(m.group(0)):
            return m.group(0)
    return ''


def _extract_email(obj):
    for key in _EMAIL_FIELDS:
        v = obj.get(key, '')
        if v and isinstance(v, str) and '@' in v:
            m = _EMAIL_RE.search(v)
            if m:
                return m.group(0)
    log = obj.get('SObjectLog__c', '')
    if log and isinstance(log, str) and '@' in log:
        m = _EMAIL_RE.search(log)
        if m:
            return m.group(0)
    for k, v in obj.items():
        if k == 'SObjectLog__c':
            continue
        if isinstance(v, str) and '@' in v and 6 < len(v) < 500:
            if '/services/' in v or 'http' in v:
                continue
            m = _EMAIL_RE.search(v)
            if m:
                return m.group(0)
    return ''


def _extract_iban(obj):
    for key in _IBAN_FIELDS:
        v = obj.get(key, '')
        if v and isinstance(v, str) and len(v) >= 10:
            return v.strip()
    return ''


def _parse_row(obj):
    """Convert a parsed JSON object into a row tuple."""
    house_num = obj.get('House_Number__c')
    street = ' '.join(filter(None, [
        obj.get('BillingStreet') or obj.get('Street_Address__c', ''),
        str(house_num) if house_num is not None and house_num != '' else '',
        obj.get('House_Number_Extension__c', ''),
    ])).strip()

    compact = {k: v for k, v in obj.items() if v not in _EMPTY_VALS}
    compact['Id']   = obj.get('Id', '')
    compact['Name'] = obj.get('Name', '')

    log_text = str(obj.get('SObjectLog__c') or '') + str(obj.get('Flash_Message__c') or '')

    id_number   = (obj.get('ID_number__c') or '').strip()
    id_type     = (obj.get('ID_type__c') or '').strip()
    id_valid    = (obj.get('ID_valid__c') or '').strip()
    birthdate   = (obj.get('Birthdate') or obj.get('BirthDate__c') or '').strip()
    nationality = (obj.get('Nationality__c') or '').strip()
    gender      = (obj.get('Gender__c') or obj.get('vlocity_cmt__Gender__c') or '').strip()
    brand       = (obj.get('Brand_Type__c') or obj.get('Brand__c') or '').strip()
    segment     = (obj.get('Segment__c') or obj.get('Segment_Indicator__c')
                   or obj.get('Account_Segment_Indicator__c') or '').strip()
    has_id_doc  = 1 if (id_number or _IDDOC_RE.search(log_text)) else 0

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
        segment,
        obj.get('IsActive', ''),
        brand,
        obj.get('CreatedDate', ''),
        _json_dumps(compact),
        id_number, id_type, id_valid,
        birthdate, nationality, gender,
        1 if log_text else 0,
        1 if (obj.get('Chamber_Of_Commerce_Number__c') or obj.get('KvK_Number__c')) else 0,
        1 if (obj.get('Password__c') or obj.get('Portal_Password__c') or obj.get('Wachtwoord__c')) else 0,
        1 if (obj.get('Pin__c') or obj.get('Pincode__c')) else 0,
        has_id_doc,
        1 if _SUMMONS_RE.search(log_text + str(obj.get('Description') or '')) else 0,
        1 if _DECEASED_RE.search(log_text) else 0,
    )


# ─── Worker shared state (set by pool initializer) ───────────────────────
_shared_progress = None
_shared_errors   = None
_worker_cfg      = {}

def _init_worker(progress, errors, cfg):
    global _shared_progress, _shared_errors, _worker_cfg
    _shared_progress = progress
    _shared_errors   = errors
    _worker_cfg      = cfg


def _compute_chunks(filepath, num_chunks):
    """Divide file into byte ranges aligned to line boundaries."""
    file_size = os.path.getsize(filepath)
    if num_chunks <= 1 or file_size == 0:
        return [(0, file_size)]

    chunk_size = file_size // num_chunks
    chunks = []
    with open(filepath, 'rb') as f:
        start = 0
        for _ in range(num_chunks - 1):
            target = start + chunk_size
            if target >= file_size:
                break
            f.seek(target)
            f.readline()
            end = f.tell()
            if end >= file_size:
                break
            chunks.append((start, end))
            start = end
        chunks.append((start, file_size))

    return [(s, e) for s, e in chunks if s < e]


def _worker_parse(filepath, start_byte, end_byte, output_path):
    """
    Worker process: read a byte-range of the source file, parse every
    JSON line, and write batches of row tuples to a flat pickle file.
    Pure sequential I/O — no SQLite, no B-tree, no degradation.
    Returns (output_path, record_count, error_count).
    """
    global _shared_progress, _shared_errors, _worker_cfg

    count = 0
    errs  = 0
    batch = []
    FLUSH = _worker_cfg.get('flush_size', 50_000)
    READ_BUF = _worker_cfg.get('read_buf', 16 * 1024 * 1024)

    loads = _json_loads                       # local ref for tight loop
    parse = _parse_row
    bappend = batch.append

    with open(output_path, 'wb') as out, \
         open(filepath, 'rb', buffering=READ_BUF) as f:
        f.seek(start_byte)
        if start_byte > 0:
            f.readline()                      # skip partial first line

        while f.tell() < end_byte:
            raw = f.readline()
            if not raw:
                break
            try:
                raw = raw.strip()
                if not raw:
                    continue
                obj = loads(raw)              # orjson accepts bytes directly
                bappend(parse(obj))
                if len(batch) >= FLUSH:
                    pickle.dump(batch, out, protocol=pickle.HIGHEST_PROTOCOL)
                    count += len(batch)
                    batch.clear()
                    if _shared_progress is not None:
                        with _shared_progress.get_lock():
                            _shared_progress.value += FLUSH
            except Exception:
                errs += 1

        # Final flush — still inside the 'with' block so 'out' is open
        if batch:
            pickle.dump(batch, out, protocol=pickle.HIGHEST_PROTOCOL)
            count += len(batch)
            if _shared_progress is not None:
                with _shared_progress.get_lock():
                    _shared_progress.value += len(batch)

    if _shared_errors is not None:
        with _shared_errors.get_lock():
            _shared_errors.value += errs

    return output_path, count, errs


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    BASE_DIR      = Path(__file__).parent
    MANIFEST_PATH = BASE_DIR / 'databases.json'

    # ─── Argument check ───────────────────────────────────────────────────
    if len(sys.argv) < 2:
        print('\nUsage: python import.py "path\\to\\file.txt" "Database name"')
        print('Example: python import.py "C:\\Users\\alexd\\Downloads\\data.txt" "Odido"\n')
        input('Press Enter to exit...')
        sys.exit(1)

    input_file = Path(sys.argv[1].strip('"'))
    if not input_file.exists():
        print(f'\n[ERROR] File not found:\n  {input_file}\n')
        input('Press Enter to exit...')
        sys.exit(1)

    file_size_mb = input_file.stat().st_size / 1024 / 1024

    # ─── Database name ────────────────────────────────────────────────────
    db_name = sys.argv[2].strip() if len(sys.argv) >= 3 else ''
    if not db_name:
        print(f'\n  File : {input_file.name}')
        print(f'  Size : {file_size_mb:.1f} MB\n')
        db_name = input('  Name for this database (e.g. "Odido"): ').strip()
        if not db_name:
            db_name = 'Database 1'

    safe_file = re.sub(r'[^\w\-]', '_', db_name) + '.db'
    DB_PATH   = BASE_DIR / safe_file

    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f'Removed existing {safe_file}.')

    # ─── Detect system resources and compute settings ────────────────────
    cfg = _compute_settings()
    NUM_WORKERS = cfg['workers']
    chunks = _compute_chunks(str(input_file), NUM_WORKERS)
    actual_workers = len(chunks)

    start_time = time.time()

    print(f'\n{"═"*50}')
    print(f'  UnknownDatabase Scanner – Import')
    print(f'{"═"*50}')
    print(f'\n  File    : {input_file.name}')
    print(f'  Size    : {file_size_mb:.1f} MB')
    print(f'  RAM     : {cfg["ram_mb"]:,} MB')
    print(f'  Workers : {actual_workers}')

    # ══════════════════════════════════════════════════════════════════════
    #  Phase 1 — parallel parse  (workers → flat pickle files)
    # ══════════════════════════════════════════════════════════════════════
    print(f'\n  Phase 1 — Parsing records (parallel)...\n')

    temp_dir  = tempfile.mkdtemp(dir=str(BASE_DIR), prefix='.import_tmp_')
    progress  = mp.Value('l', 0)
    err_count = mp.Value('l', 0)

    try:
        pool = mp.Pool(actual_workers, initializer=_init_worker,
                       initargs=(progress, err_count, cfg))

        async_results = []
        for i, (sb, eb) in enumerate(chunks):
            tpath = os.path.join(temp_dir, f'w{i}.bin')
            async_results.append(
                pool.apply_async(_worker_parse,
                                 (str(input_file), sb, eb, tpath)))

        # Live progress while workers run
        while not all(r.ready() for r in async_results):
            time.sleep(0.4)
            elapsed = time.time() - start_time
            total   = progress.value
            rate    = int(total / elapsed) if elapsed > 0 else 0
            print(f'\r  Parsed: {total:>10,} records | {rate:>7,}/s | '
                  f'{elapsed:.0f}s elapsed', end='', flush=True)

        worker_results = [r.get() for r in async_results]
        pool.close()
        pool.join()

        parse_time   = time.time() - start_time
        total_parsed = sum(wc for _, wc, _ in worker_results)
        total_errors = sum(we for _, _, we in worker_results)
        parse_rate   = int(total_parsed / parse_time) if parse_time > 0 else 0
        print(f'\r  Parsed: {total_parsed:>10,} records | {parse_rate:>7,}/s | '
              f'{parse_time:.0f}s elapsed        ')

        # ══════════════════════════════════════════════════════════════════
        #  Phase 2 — bulk insert into final SQLite database
        # ══════════════════════════════════════════════════════════════════
        print(f'\n  Phase 2 — Inserting into database...\n')
        phase2_start = time.time()

        conn = sqlite3.connect(str(DB_PATH))
        c    = conn.cursor()
        c.execute("PRAGMA page_size      = 65536")
        c.execute("PRAGMA journal_mode   = OFF")
        c.execute("PRAGMA synchronous    = OFF")
        c.execute(f"PRAGMA cache_size    = {cfg['cache_pragma']}")
        c.execute("PRAGMA temp_store     = MEMORY")
        c.execute(f"PRAGMA mmap_size     = {cfg['mmap_bytes']}")
        c.execute("PRAGMA locking_mode   = EXCLUSIVE")

        # No UNIQUE on id yet — plain inserts are much faster.
        # We add the UNIQUE index in Phase 3.
        c.execute("""
            CREATE TABLE accounts (
                rowid       INTEGER PRIMARY KEY,
                id          TEXT,
                name        TEXT,
                phone       TEXT,
                email       TEXT,
                iban        TEXT,
                street      TEXT,
                city        TEXT,
                postal      TEXT,
                country     TEXT,
                status      TEXT,
                segment     TEXT,
                is_active   TEXT,
                brand       TEXT,
                created     TEXT,
                data        TEXT,
                id_number   TEXT,
                id_type     TEXT,
                id_valid    TEXT,
                birthdate   TEXT,
                nationality TEXT,
                gender      TEXT,
                f_notes     INTEGER DEFAULT 0,
                f_kvk       INTEGER DEFAULT 0,
                f_password  INTEGER DEFAULT 0,
                f_pincode   INTEGER DEFAULT 0,
                f_id_doc    INTEGER DEFAULT 0,
                f_summons   INTEGER DEFAULT 0,
                f_deceased  INTEGER DEFAULT 0
            )
        """)
        conn.commit()

        insert_sql = f"INSERT INTO accounts ({_COL_LIST}) VALUES ({_COL_PLACEHOLDERS})"

        count = 0
        commit_pending = 0
        COMMIT_EVERY = cfg['commit_every']

        for i, (output_path, wcount, werrs) in enumerate(worker_results):
            with open(output_path, 'rb') as f:
                while True:
                    try:
                        batch = pickle.load(f)
                        c.executemany(insert_sql, batch)
                        count += len(batch)
                        commit_pending += len(batch)
                        if commit_pending >= COMMIT_EVERY:
                            conn.commit()
                            commit_pending = 0
                    except EOFError:
                        break
            try:
                os.unlink(output_path)
            except OSError:
                pass

            elapsed = time.time() - phase2_start
            rate = int(count / elapsed) if elapsed > 0 else 0
            print(f'\r  Inserted: {count:>10,} records | {rate:>7,}/s | '
                  f'file {i+1}/{len(worker_results)}',
                  end='', flush=True)

        conn.commit()
        phase2_time = time.time() - phase2_start
        insert_rate = int(count / phase2_time) if phase2_time > 0 else 0
        print(f'\r  Inserted: {count:>10,} records | {insert_rate:>7,}/s | '
              f'{phase2_time:.1f}s                       ')

        # ══════════════════════════════════════════════════════════════════
        #  Phase 3 — indexes
        # ══════════════════════════════════════════════════════════════════
        print(f'\n  Phase 3 — Creating indexes...')
        idx_start = time.time()

        # UNIQUE index on id  (also deduplicates if needed)
        try:
            c.execute("CREATE UNIQUE INDEX idx_id ON accounts(id)")
        except sqlite3.IntegrityError:
            print('  Removing duplicate IDs...')
            c.execute("CREATE INDEX _tmp_idx_id ON accounts(id)")
            c.execute("""
                DELETE FROM accounts WHERE rowid NOT IN (
                    SELECT MIN(rowid) FROM accounts GROUP BY id)
            """)
            removed = c.execute("SELECT changes()").fetchone()[0]
            c.execute("DROP INDEX _tmp_idx_id")
            c.execute("CREATE UNIQUE INDEX idx_id ON accounts(id)")
            count -= removed
            print(f'  Removed {removed:,} duplicates.')
        conn.commit()

        for sql in [
            "CREATE INDEX IF NOT EXISTS idx_name       ON accounts(name       COLLATE NOCASE)",
            "CREATE INDEX IF NOT EXISTS idx_phone      ON accounts(phone)",
            "CREATE INDEX IF NOT EXISTS idx_city       ON accounts(city       COLLATE NOCASE)",
            "CREATE INDEX IF NOT EXISTS idx_postal     ON accounts(postal)",
            "CREATE INDEX IF NOT EXISTS idx_email      ON accounts(email      COLLATE NOCASE)",
            "CREATE INDEX IF NOT EXISTS idx_iban       ON accounts(iban       COLLATE NOCASE)",
            "CREATE INDEX IF NOT EXISTS idx_id_number  ON accounts(id_number  COLLATE NOCASE)",
            "CREATE INDEX IF NOT EXISTS idx_status     ON accounts(status)",
            "CREATE INDEX IF NOT EXISTS idx_f_notes    ON accounts(f_notes)",
            "CREATE INDEX IF NOT EXISTS idx_f_kvk      ON accounts(f_kvk)",
            "CREATE INDEX IF NOT EXISTS idx_f_password ON accounts(f_password)",
            "CREATE INDEX IF NOT EXISTS idx_f_pincode  ON accounts(f_pincode)",
            "CREATE INDEX IF NOT EXISTS idx_f_id_doc   ON accounts(f_id_doc)",
            "CREATE INDEX IF NOT EXISTS idx_f_summons  ON accounts(f_summons)",
            "CREATE INDEX IF NOT EXISTS idx_f_deceased ON accounts(f_deceased)",
        ]:
            c.execute(sql)
        conn.commit()
        print(f'  Indexes created in {time.time() - idx_start:.1f}s')

        # ─── Full-text search index ───────────────────────────────────────
        print('  Building full-text search index...')
        fts_start = time.time()
        try:
            c.execute("""
                CREATE VIRTUAL TABLE accounts_fts USING fts5(
                    id, name, phone, email, iban, street, city, postal,
                    id_number,
                    content       = accounts,
                    content_rowid = rowid,
                    tokenize      = 'unicode61 remove_diacritics 1'
                )
            """)
            c.execute("INSERT INTO accounts_fts(accounts_fts) VALUES('rebuild')")
            conn.commit()
            print(f'  FTS5 index created in {time.time() - fts_start:.1f}s')
        except Exception as e:
            print(f'  Warning: FTS5 not available ({e}).')
            print('  LIKE-based search will still work.')

        # ─── Compute filter counts ────────────────────────────────────────
        print('  Computing filter counts...')
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
            print(f'  Counts: IBAN={filter_counts["has_iban"]:,}  '
                  f'Log/Flits={filter_counts["has_notes"]:,}  '
                  f'KvK={filter_counts["has_kvk"]:,}')
        except Exception as e:
            print(f'  Warning: Could not compute counts: {e}')
            filter_counts = {}

        # ─── Finalize ─────────────────────────────────────────────────────
        c.execute("PRAGMA locking_mode  = NORMAL")
        c.execute("PRAGMA synchronous   = NORMAL")
        conn.close()

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    # ─── Summary ──────────────────────────────────────────────────────────
    total_time = time.time() - start_time
    db_size_mb = DB_PATH.stat().st_size / 1024 / 1024

    print(f'\n{"═"*50}')
    print(f'  Import complete!')
    print(f'{"═"*50}')
    print(f'  Records imported : {count:,}')
    if total_errors:
        print(f'  Skipped (errors) : {total_errors:,}')
    print(f'  Database size    : {db_size_mb:.0f} MB')
    print(f'  Total time       : {total_time:.1f}s')

    # ─── Update databases.json manifest ───────────────────────────────────
    CURRENT_VERSION = 4
    try:
        manifest = []
        if MANIFEST_PATH.exists():
            manifest = json.loads(MANIFEST_PATH.read_text('utf-8'))
        manifest = [e for e in manifest if e['name'] != db_name]
        manifest.append({
            'name': db_name, 'file': safe_file,
            'version': CURRENT_VERSION, 'counts': filter_counts,
        })
        MANIFEST_PATH.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), 'utf-8')
        print(f'  databases.json updated ({len(manifest)} database(s) total)')
    except Exception as e:
        print(f'  [WARNING] databases.json not updated: {e}')

    print(f'\n  Run menu.bat and select option 1 to start searching!\n')
    input('Press Enter to exit...')


if __name__ == '__main__':
    mp.freeze_support()
    main()
