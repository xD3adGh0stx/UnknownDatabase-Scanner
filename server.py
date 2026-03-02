"""
UnknownDatabase Scanner - Web Server
Supports multiple named databases simultaneously.
No extra packages required - uses only built-in Python modules.
"""
import sqlite3
import json
import sys
import threading
import time
import webbrowser
import socketserver
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from pathlib import Path

BASE_DIR          = Path(__file__).parent
MANIFEST_PATH     = BASE_DIR / 'databases.json'
PORT              = 3000
CURRENT_VERSION   = 4  # Version migration level

# ─── Manifest helpers ──────────────────────────────────────────────────────
def load_manifest():
    """Returns list of {name, file, version} dicts from databases.json."""
    if MANIFEST_PATH.exists():
        try:
            data = json.loads(MANIFEST_PATH.read_text('utf-8'))
            if isinstance(data, list) and data:
                # Add default version=1 for old databases
                for entry in data:
                    if 'version' not in entry:
                        entry['version'] = 1
                return data
        except Exception:
            pass
    # Auto-detect legacy database.db and create manifest
    legacy = BASE_DIR / 'database.db'
    if legacy.exists():
        manifest = [{'name': 'Database 1', 'file': 'database.db', 'version': 1}]
        MANIFEST_PATH.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), 'utf-8')
        print('  [INFO] databases.json created from existing database.db')
        return manifest
    return []

def save_manifest(manifest):
    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), 'utf-8')

# ─── Startup ───────────────────────────────────────────────────────────────
DATABASES = load_manifest()

if not DATABASES:
    print('\n[ERROR] No databases found.')
    print('Run menu.bat and choose option 2 to import a database.\n')
    input('Press Enter to exit...')
    sys.exit(1)

# ─── Per-database column detection ────────────────────────────────────────
def _detect_columns(db_path):
    conn = sqlite3.connect(str(db_path))
    cols = {row[1] for row in conn.execute("PRAGMA table_info(accounts)").fetchall()}
    conn.close()
    return cols

def _check_flags_populated(db_path, has_flags, has_deceased):
    """Return (flags_populated, deceased_populated) — columns may exist but be empty if migrate never ran."""
    if not has_flags and not has_deceased:
        return False, False
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA query_only = 1")
    fp = dp = False
    if has_flags:
        fp = conn.execute("SELECT COUNT(*) FROM accounts WHERE f_notes = 1").fetchone()[0] > 0
    if has_deceased:
        dp = conn.execute("SELECT COUNT(*) FROM accounts WHERE f_deceased = 1").fetchone()[0] > 0
    conn.close()
    return fp, dp

_BASE_COLS = 'id, name, phone, email, street, city, postal, country, status, segment, is_active, brand'
_FULL_COLS = 'id, name, phone, email, iban, street, city, postal, country, status, segment, is_active, brand'
_V4_COLS   = 'id, name, phone, email, iban, street, city, postal, country, status, segment, is_active, brand, id_number, id_type, id_valid, birthdate, nationality, gender'

# Build per-database info at startup
_DB_INFO  = {}   # name -> {path, cols, has_iban, has_flags}
_DB_ORDER = []   # ordered list of db names

def _fmt(n):
    """Format number with thousand separators."""
    return f'{n:,}'.replace(',', '.')

def _size_mb(path):
    """Get file size in MB."""
    try:
        return path.stat().st_size / (1024 * 1024)
    except Exception:
        return 0

print()
print('  +==========================================+')
print('  |     UnknownDatabase Scanner  v1.0        |')
print('  +==========================================+')
print()

# Check for old database versions
_outdated_dbs = [e for e in DATABASES if e.get('version', 1) < CURRENT_VERSION]
if _outdated_dbs:
    print(f'  [!] {len(_outdated_dbs)} database(s) need migration!')
    for _e in _outdated_dbs:
        print(f'     - "{_e["name"]}" (v{_e.get("version", 1)} → v{CURRENT_VERSION})')
    print(f'     Use menu.bat option 4 to migrate!\n')

_total_records = 0

for _i, _entry in enumerate(DATABASES):
    _name = _entry['name']
    _path = BASE_DIR / _entry['file']
    if not _path.exists():
        print(f'  [!] File not found: {_path}')
        continue
    _cols_set = _detect_columns(_path)
    _has_iban     = 'iban'       in _cols_set
    _has_flags    = 'f_notes'   in _cols_set  # v3+ flag columns
    _has_deceased = 'f_deceased' in _cols_set  # added later, check separately
    _has_id_num   = 'id_number' in _cols_set  # v4+ identity columns
    _db_version = _entry.get('version', 1)
    # For v3+ databases, flags were populated during import — trust them even if all 0
    # (e.g. Contact records have no SObjectLog so f_notes is correctly 0 for all rows)
    if _db_version >= 3:
        _flags_ok    = _has_flags
        _deceased_ok = _has_deceased
    else:
        _flags_ok, _deceased_ok = _check_flags_populated(_path, _has_flags, _has_deceased)
        if _has_flags and not _flags_ok:
            print(f'  [!] "{_name}": flags not populated - run option 4 (migrate)!')
    # Choose column set based on available columns
    if _has_id_num:
        _col_str = _V4_COLS
    elif _has_iban:
        _col_str = _FULL_COLS
    else:
        _col_str = _BASE_COLS
    _DB_INFO[_name] = {
        'path':            _path,
        'cols':            _col_str,
        'has_iban':        _has_iban,
        'has_flags':       _has_flags and _flags_ok,   # only True if actually populated
        'has_deceased':    _has_deceased and _deceased_ok,
        'has_id_num':      _has_id_num,
    }
    _DB_ORDER.append(_name)

    # Collect stats for display
    _counts = _entry.get('counts', {})
    _active   = _counts.get('is_active', 0)
    _inactive = _counts.get('is_inactive', 0)
    _rec_count = _active + _inactive
    _total_records += _rec_count
    _mb = _size_mb(_path)

    # Feature tags
    _tags = []
    if _has_iban:    _tags.append('IBAN')
    if _has_id_num:  _tags.append('ID-doc')
    if _has_flags:   _tags.append('Filters')

    print(f'  +-- {_name}')
    print(f'  |   {_entry["file"]}  ({_mb:,.0f} MB)  -  {_fmt(_rec_count)} records')
    _tag_str = '  -  '.join(_tags) if _tags else 'basic'
    print(f'  |   {_tag_str}')
    if _i < len(DATABASES) - 1:
        print(f'  |')
    else:
        print(f'  +--')

if not _DB_INFO:
    print('\n[ERROR] No usable databases found.')
    input('Press Enter to exit...')
    sys.exit(1)

# ─── Thread-local connections per database ─────────────────────────────────
_local      = threading.local()
_all_conns  = {}          # name -> list of all sqlite3 connections ever opened
_conns_lock = threading.Lock()

def get_db(name):
    """Returns a thread-local SQLite connection for the named database."""
    if not hasattr(_local, 'conns'):
        _local.conns = {}
    if name not in _local.conns:
        info = _DB_INFO.get(name)
        if not info:
            return None
        conn = sqlite3.connect(str(info['path']), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA cache_size   = 10000")
        # conn.execute("PRAGMA temp_store   = MEMORY") # Removed to prevent OOM
        conn.execute("PRAGMA journal_mode = WAL")
        _local.conns[name] = conn
        # Register in global registry so delete can close all threads' connections
        with _conns_lock:
            _all_conns.setdefault(name, []).append(conn)
    return _local.conns[name]

def _close_all_db_conns(name):
    """Close every open connection to the named database across all threads."""
    with _conns_lock:
        conns = _all_conns.pop(name, [])
    for conn in conns:
        try:
            conn.close()
        except Exception:
            pass
    # Also clear thread-local reference if this thread has one
    if hasattr(_local, 'conns'):
        _local.conns.pop(name, None)

# ─── FTS5 query helper ─────────────────────────────────────────────────────
def escape_fts(q):
    words = [w for w in q.strip().split() if w]
    return ' '.join(f'"{w.replace(chr(34), " ")}"*' for w in words)

# ─── Filter counts cache ───────────────────────────────────────────────────
_FILTER_COUNTS  = {}   # db_name -> {has_iban, has_notes, ...}
_counts_ready   = False
_counts_lock_fc = threading.Lock()

def _compute_counts_for_db(db_name, conn):
    """Compute all filter counts for one DB using the given connection."""
    info  = _DB_INFO.get(db_name, {})
    flags = info.get('has_flags', False)   # v3+ flag columns available
    c = {}
    # IBAN — fast (indexed column if migrated, else 0)
    if info.get('has_iban'):
        c['has_iban'] = conn.execute(
            "SELECT COUNT(*) FROM accounts WHERE iban != '' AND iban IS NOT NULL"
        ).fetchone()[0]
    else:
        c['has_iban'] = 0
    # Notes: use flag column if available, else LIKE scan
    c['has_notes'] = conn.execute(
        "SELECT COUNT(*) FROM accounts WHERE f_notes = 1" if flags else
        "SELECT COUNT(*) FROM accounts WHERE data LIKE '%SObjectLog__c%' OR data LIKE '%Flash_Message__c%'"
    ).fetchone()[0]
    # Account password (not pincode)
    c['has_password'] = conn.execute(
        "SELECT COUNT(*) FROM accounts WHERE f_password = 1" if flags else
        'SELECT COUNT(*) FROM accounts WHERE data LIKE \'%"Password__c": "_%\''
        ' OR data LIKE \'%"Portal_Password__c": "_%\' OR data LIKE \'%"Wachtwoord__c": "_%\''
    ).fetchone()[0]
    # App pincode (separate from account password)
    c['has_pincode'] = conn.execute(
        "SELECT COUNT(*) FROM accounts WHERE f_pincode = 1" if flags else
        'SELECT COUNT(*) FROM accounts WHERE data LIKE \'%"Pin__c": "_%\''
        ' OR data LIKE \'%"Pincode__c": "_%\''
    ).fetchone()[0]
    # KvK (Chamber of Commerce number)
    c['has_kvk'] = conn.execute(
        "SELECT COUNT(*) FROM accounts WHERE f_kvk = 1" if flags else
        'SELECT COUNT(*) FROM accounts WHERE data LIKE \'%"Chamber_Of_Commerce_Number__c": "_%\''
        ' OR data LIKE \'%"KvK_Number__c": "_%\''
    ).fetchone()[0]
    # Summons / debt notices
    c['has_summons'] = conn.execute(
        "SELECT COUNT(*) FROM accounts WHERE f_summons = 1" if flags else
        "SELECT COUNT(*) FROM accounts WHERE data LIKE '%aanmaning%' OR data LIKE '%sommatie%' OR data LIKE '%incasso%'"
    ).fetchone()[0]
    # ID documents (rijbewijs/paspoort in log text, or id_number column)
    has_id_col = info.get('has_id_num', False)
    if has_id_col:
        c['has_id_doc'] = conn.execute(
            "SELECT COUNT(*) FROM accounts WHERE f_id_doc = 1 OR (id_number != '' AND id_number IS NOT NULL)"
        ).fetchone()[0]
    elif flags:
        c['has_id_doc'] = conn.execute(
            "SELECT COUNT(*) FROM accounts WHERE f_id_doc = 1"
        ).fetchone()[0]
    else:
        c['has_id_doc'] = conn.execute(
            "SELECT COUNT(*) FROM accounts WHERE data LIKE '%rijbewijs%' OR data LIKE '%paspoort%' OR data LIKE '%identiteitsbewijs%'"
        ).fetchone()[0]
    # BSN mentions in log text
    c['has_bsn'] = conn.execute(
        "SELECT COUNT(*) FROM accounts WHERE data LIKE '%BSN%' OR data LIKE '%burgerservicenummer%'"
    ).fetchone()[0]
    # Deceased (use flag if available — check f_deceased separately, added after f_notes)
    deceased_col = info.get('has_deceased', False)
    c['is_deceased'] = conn.execute(
        "SELECT COUNT(*) FROM accounts WHERE f_deceased = 1" if deceased_col else
        "SELECT COUNT(*) FROM accounts WHERE data LIKE '%overled%' OR data LIKE '%verlijden%'"
        " OR data LIKE '%nabestaand%' OR data LIKE '%gestorven%'"
    ).fetchone()[0]
    # Active / Inactive (indexed status column)
    c['is_active']   = conn.execute(
        "SELECT COUNT(*) FROM accounts WHERE status = 'Active'"
    ).fetchone()[0]
    c['is_inactive'] = conn.execute(
        "SELECT COUNT(*) FROM accounts WHERE status = 'Inactive'"
    ).fetchone()[0]
    return c

def _compute_filter_counts():
    """Background thread: load cached counts from manifest, compute missing ones."""
    global _FILTER_COUNTS, _counts_ready

    # 1. Load any counts already stored in databases.json (set during import/migrate)
    try:
        manifest_data = json.loads(MANIFEST_PATH.read_text('utf-8')) if MANIFEST_PATH.exists() else []
        for entry in manifest_data:
            name = entry.get('name', '')
            if 'counts' in entry and name in _DB_ORDER:
                with _counts_lock_fc:
                    _FILTER_COUNTS[name] = dict(entry['counts'])
    except Exception:
        pass

    # 2. Compute counts for any DB that didn't have them cached
    needs_compute = [n for n in _DB_ORDER if n not in _FILTER_COUNTS]
    for db_name in needs_compute:
        info = _DB_INFO.get(db_name)
        if not info:
            continue
        try:
            conn = sqlite3.connect(str(info['path']), check_same_thread=False)
            conn.execute("PRAGMA query_only = 1")
            conn.execute("PRAGMA cache_size = 50000")
            conn.execute("PRAGMA mmap_size  = 268435456")
            counts = _compute_counts_for_db(db_name, conn)
            conn.close()
            with _counts_lock_fc:
                _FILTER_COUNTS[db_name] = counts
            # Cache to manifest so next server restart is instant
            try:
                manifest = json.loads(MANIFEST_PATH.read_text('utf-8'))
                for entry in manifest:
                    if entry.get('name') == db_name:
                        entry['counts'] = counts
                        break
                MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), 'utf-8')
            except Exception:
                pass
        except Exception:
            pass

    _counts_ready = True

# Start background count computation (instant if counts cached in manifest)
threading.Thread(target=_compute_filter_counts, daemon=True).start()

# ─── Search one database, return set of matching IDs ──────────────────────
_MAX_IDS = 5000   # Hard cap per database

def _search_ids(db, q, field, has_iban, has_id_num=False):
    lq = f'%{q}%'

    if field == 'all':
        # 1. FTS5 — very fast, use it exclusively when it returns results
        try:
            fts_q = escape_fts(q)
            rows  = db.execute("""
                SELECT a.id FROM accounts_fts f
                JOIN   accounts a ON a.rowid = f.rowid
                WHERE  accounts_fts MATCH ?
                LIMIT  ?
            """, (fts_q, _MAX_IDS)).fetchall()
            if rows:
                return {r[0] for r in rows}
        except Exception:
            pass

        # 2. LIKE on indexed columns only (no full data scan)
        cols = [('name','NOCASE'), ('phone',''), ('email','NOCASE'),
                ('city','NOCASE'), ('street','NOCASE'), ('id','')]
        if has_iban:
            cols.insert(0, ('iban','NOCASE'))
        if has_id_num:
            cols.append(('id_number', 'NOCASE'))
        ids = set()
        for col, collate in cols:
            if len(ids) >= _MAX_IDS:
                break
            coll = f' COLLATE {collate}' if collate else ''
            rows = db.execute(
                f"SELECT id FROM accounts WHERE {col} LIKE ?{coll} LIMIT ?",
                (lq, _MAX_IDS - len(ids))
            ).fetchall()
            ids |= {r[0] for r in rows}
        # Postal: strip spaces from both sides so "8251PD" matches "8251 PD"
        if len(ids) < _MAX_IDS:
            q_postal = f'%{q.replace(" ", "")}%'
            rows = db.execute(
                "SELECT id FROM accounts WHERE REPLACE(postal, ' ', '') LIKE ? LIMIT ?",
                (q_postal, _MAX_IDS - len(ids))
            ).fetchall()
            ids |= {r[0] for r in rows}
        return ids

    elif field == 'iban':
        if has_iban:
            rows = db.execute(
                "SELECT id FROM accounts WHERE iban LIKE ? COLLATE NOCASE LIMIT ?",
                (lq, _MAX_IDS)
            ).fetchall()
            return {r[0] for r in rows}
        # Fallback for old DBs without iban column
        rows = db.execute(
            "SELECT id FROM accounts WHERE data LIKE ? LIMIT ?", (lq, _MAX_IDS)
        ).fetchall()
        return {r[0] for r in rows}

    elif field == 'notes':
        # Must scan data column — limited to avoid hanging
        rows = db.execute(
            "SELECT id FROM accounts WHERE data LIKE ? LIMIT ?", (lq, _MAX_IDS)
        ).fetchall()
        return {r[0] for r in rows}

    elif field == 'phone':
        # Try prefix match first ("+316..." uses index)
        stripped = q.replace(' ', '').replace('-', '')
        rows = db.execute(
            "SELECT id FROM accounts WHERE phone LIKE ? LIMIT ?",
            (f'{stripped}%', _MAX_IDS)
        ).fetchall()
        if rows:
            return {r[0] for r in rows}
        # Try FTS5 for exact/near matches (fast)
        try:
            fts_q = escape_fts(stripped)
            rows = db.execute("""
                SELECT a.id FROM accounts_fts f
                JOIN   accounts a ON a.rowid = f.rowid
                WHERE  accounts_fts MATCH 'phone: ' || ?
                LIMIT  ?
            """, (fts_q, _MAX_IDS)).fetchall()
            if rows:
                return {r[0] for r in rows}
        except Exception:
            pass
        # Fallback: full LIKE scan (slow but catches partial matches)
        rows = db.execute(
            "SELECT id FROM accounts WHERE phone LIKE ? LIMIT ?", (lq, _MAX_IDS)
        ).fetchall()
        return {r[0] for r in rows}

    elif field == 'email':
        rows = db.execute(
            "SELECT id FROM accounts WHERE email LIKE ? COLLATE NOCASE LIMIT ?",
            (lq, _MAX_IDS)
        ).fetchall()
        return {r[0] for r in rows}

    elif field == 'postal':
        # Strip spaces from both stored value and query: "8251PD" matches "8251 PD"
        q_postal = f'%{q.replace(" ", "")}%'
        rows = db.execute(
            "SELECT id FROM accounts WHERE REPLACE(postal, ' ', '') LIKE ? LIMIT ?",
            (q_postal, _MAX_IDS)
        ).fetchall()
        return {r[0] for r in rows}

    elif field == 'id_number':
        if has_id_num:
            # Try exact match first (instant with index)
            rows = db.execute(
                "SELECT id FROM accounts WHERE id_number = ? COLLATE NOCASE LIMIT ?",
                (q, _MAX_IDS)
            ).fetchall()
            if rows:
                return {r[0] for r in rows}
            # Try prefix match (uses index)
            rows = db.execute(
                "SELECT id FROM accounts WHERE id_number LIKE ? COLLATE NOCASE LIMIT ?",
                (f'{q}%', _MAX_IDS)
            ).fetchall()
            if rows:
                return {r[0] for r in rows}
            # Fallback: full LIKE scan
            rows = db.execute(
                "SELECT id FROM accounts WHERE id_number LIKE ? COLLATE NOCASE LIMIT ?",
                (lq, _MAX_IDS)
            ).fetchall()
            return {r[0] for r in rows}
        return set()

    else:
        field_map = {
            'name':   ('name',   'NOCASE'),
            'city':   ('city',   'NOCASE'),
            'street': ('street', 'NOCASE'),
            'id':     ('id',     ''),
        }
        if has_iban:
            field_map['iban'] = ('iban', 'NOCASE')
        if has_id_num:
            field_map['id_number'] = ('id_number', 'NOCASE')
        col, collate = field_map.get(field, ('name', 'NOCASE'))
        q_val = q if field == 'id' else lq
        coll  = f' COLLATE {collate}' if collate else ''
        rows  = db.execute(
            f"SELECT id FROM accounts WHERE {col} LIKE ?{coll} LIMIT ?",
            (q_val, _MAX_IDS)
        ).fetchall()
        return {r[0] for r in rows}

# ─── HTTP request handler ──────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass  # Suppress default request logs

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path
        qs     = parse_qs(parsed.query, keep_blank_values=True)

        def p(key, default=''):
            vals = qs.get(key)
            return vals[0] if vals else default

        try:
            if   path == '/api/search':            self.handle_search(p('q'), p('field', 'all'), p('page', '1'), p('dbs', ''))
            elif path == '/api/record':            self.handle_record(p('id'), p('db'))
            elif path == '/api/stats':             self.handle_stats()
            elif path == '/api/databases':         self.handle_databases()
            elif path == '/api/databases/rename':  self.handle_db_rename(p('old'), p('new'))
            elif path == '/api/databases/delete':  self.handle_db_delete(p('name'))
            elif path == '/api/browse':            self.handle_browse(p('page','1'), p('dbs',''), p('has_iban','0'), p('has_password','0'), p('has_notes','0'), p('has_summons','0'), p('is_deceased','0'), p('city',''), p('sort','city'), p('is_active','0'), p('is_inactive','0'), p('has_kvk','0'), p('has_pincode','0'), p('has_id_doc','0'), p('has_bsn','0'))
            elif path == '/api/cities':            self.handle_cities(p('dbs',''))
            elif path == '/api/filter_counts':     self.handle_filter_counts(p('dbs',''))
            elif path in ('/', '/index.html'):     self.serve_html()
            else:
                self.send_error(404, 'Not found')
        except BrokenPipeError:
            pass
        except Exception as e:
            import traceback
            print(f"\n[SERVER ERROR] {e}")
            traceback.print_exc()
            try:
                self.send_json({'error': str(e)}, 500)
            except Exception:
                pass

    # ── /api/search ──────────────────────────────────────────────────────
    def handle_search(self, q, field, page_str, dbs_str):
        q = q.strip()
        try:
            page = max(1, int(page_str or '1'))
        except ValueError:
            page = 1

        if not q:
            return self.send_json({'results': [], 'total': 0, 'page': 1, 'pages': 0})

        # Which databases to query (empty = all)
        selected = set(dbs_str.split(',')) - {''} if dbs_str else set()
        db_names = [n for n in _DB_ORDER if (not selected or n in selected)]

        limit    = 50
        all_rows = []

        for db_name in db_names:
            info = _DB_INFO.get(db_name)
            if not info:
                continue
            db = get_db(db_name)
            if not db:
                continue

            ids = _search_ids(db, q, field, info['has_iban'], info.get('has_id_num', False))
            if not ids:
                continue

            id_list      = list(ids)[:5000]
            placeholders = ','.join('?' * len(id_list))
            rows = db.execute(
                f"SELECT {info['cols']} FROM accounts WHERE id IN ({placeholders})",
                id_list
            ).fetchall()

            for r in rows:
                d = dict(r)
                if not info['has_iban']:
                    d['iban'] = ''
                if not info.get('has_id_num'):
                    d['id_number'] = ''
                    d['id_type'] = ''
                    d['id_valid'] = ''
                    d['birthdate'] = ''
                    d['nationality'] = ''
                    d['gender'] = ''
                d['_db'] = db_name
                all_rows.append(d)

        # Sort combined results by name
        all_rows.sort(key=lambda r: (r.get('name') or '').lower())

        total  = len(all_rows)
        pages  = (total + limit - 1) // limit
        offset = (page - 1) * limit

        self.send_json({
            'results': all_rows[offset:offset + limit],
            'total':   total,
            'page':    page,
            'pages':   pages,
            'hasIban': any(_DB_INFO[n]['has_iban'] for n in db_names if n in _DB_INFO),
        })

    # ── /api/record ───────────────────────────────────────────────────────
    def handle_record(self, record_id, db_name):
        if not record_id:
            return self.send_json({'error': 'No ID provided'}, 400)

        # Try the specified database first
        if db_name and db_name in _DB_INFO:
            db  = get_db(db_name)
            row = db.execute("SELECT data FROM accounts WHERE id = ?", (record_id,)).fetchone()
            if row:
                return self.send_json({'record': json.loads(row[0]), 'db': db_name})

        # Fallback: search all databases
        for name in _DB_ORDER:
            db = get_db(name)
            if not db:
                continue
            row = db.execute("SELECT data FROM accounts WHERE id = ?", (record_id,)).fetchone()
            if row:
                return self.send_json({'record': json.loads(row[0]), 'db': name})

        self.send_json({'error': 'Record not found'}, 404)

    # ── /api/databases ────────────────────────────────────────────────────
    def handle_databases(self):
        result   = []
        manifest = load_manifest()
        for entry in manifest:
            name = entry['name']
            info = _DB_INFO.get(name)
            if not info:
                result.append({'name': name, 'file': entry['file'], 'missing': True,
                                'sizeMB': '0', 'records': 0})
                continue
            path    = info['path']
            size_mb = path.stat().st_size / 1024 / 1024 if path.exists() else 0
            db      = get_db(name)
            count   = db.execute("SELECT COUNT(*) FROM accounts").fetchone()[0] if db else 0
            result.append({
                'name':    name,
                'file':    entry['file'],
                'sizeMB':  f'{size_mb:.1f}',
                'records': count,
                'hasIban': info['has_iban'],
                'hasIdNum': info.get('has_id_num', False),
            })
        self.send_json(result)

    # ── /api/databases/rename ─────────────────────────────────────────────
    def handle_db_rename(self, old_name, new_name):
        global DATABASES, _DB_ORDER
        old_name = old_name.strip()
        new_name = new_name.strip()
        if not old_name or not new_name:
            return self.send_json({'error': 'Name missing'}, 400)
        if new_name == old_name:
            return self.send_json({'ok': True})
        if new_name in _DB_INFO:
            return self.send_json({'error': 'That name already exists'}, 409)

        manifest = load_manifest()
        found    = False
        for entry in manifest:
            if entry['name'] == old_name:
                entry['name'] = new_name
                found = True
                break
        if not found:
            return self.send_json({'error': 'Database not found'}, 404)

        save_manifest(manifest)
        DATABASES = manifest

        if old_name in _DB_INFO:
            _DB_INFO[new_name] = _DB_INFO.pop(old_name)
        if old_name in _DB_ORDER:
            _DB_ORDER[_DB_ORDER.index(old_name)] = new_name

        self.send_json({'ok': True})

    # ── /api/databases/delete ─────────────────────────────────────────────
    def handle_db_delete(self, name):
        global DATABASES, _DB_ORDER
        name = name.strip()
        if not name:
            return self.send_json({'error': 'Name missing'}, 400)

        manifest = load_manifest()
        entry    = next((e for e in manifest if e['name'] == name), None)
        if not entry:
            return self.send_json({'error': 'Database not found'}, 404)

        db_path = BASE_DIR / entry['file']

        # Remove from in-memory registry first so no new connections are opened
        _DB_INFO.pop(name, None)
        if name in _DB_ORDER:
            _DB_ORDER.remove(name)

        # Close all open connections across all threads
        _close_all_db_conns(name)

        # Try to delete the file (retry a few times; WAL checkpoint may take a moment)
        if db_path.exists():
            last_err = None
            for attempt in range(6):
                try:
                    # Also delete WAL/SHM sidecar files if present
                    for suffix in ('', '-wal', '-shm'):
                        sidecar = db_path.parent / (db_path.name + suffix)
                        if sidecar.exists():
                            sidecar.unlink()
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    time.sleep(0.5)
            if last_err:
                return self.send_json({'error': f'Could not delete file: {last_err}'}, 500)

        manifest = [e for e in manifest if e['name'] != name]
        save_manifest(manifest)
        DATABASES = manifest

        self.send_json({'ok': True})

    # ── /api/browse ───────────────────────────────────────────────────────
    def handle_browse(self, page_str, dbs_str, has_iban_f, has_pass_f,
                      has_notes_f, has_summons_f, is_deceased_f, city_f, sort_f,
                      is_active_f='0', is_inactive_f='0',
                      has_kvk_f='0', has_pincode_f='0',
                      has_id_doc_f='0', has_bsn_f='0'):
        try:
            page = max(1, int(page_str or '1'))
        except ValueError:
            page = 1

        selected = set(dbs_str.split(',')) - {''} if dbs_str else set()
        db_names = [n for n in _DB_ORDER if (not selected or n in selected)]
        limit    = 50

        # If no filters at all, return empty hint
        no_filter = (has_iban_f != '1' and has_pass_f != '1' and
                     has_notes_f != '1' and has_summons_f != '1' and
                     is_deceased_f != '1' and is_active_f != '1' and
                     is_inactive_f != '1' and has_kvk_f != '1' and
                     has_pincode_f != '1' and has_id_doc_f != '1' and
                     has_bsn_f != '1' and not city_f.strip())
        if no_filter:
            return self.send_json({'results': [], 'total': 0, 'page': 1,
                                   'pages': 0, 'empty_hint': True})

        # Sort order
        order_sql = ("name COLLATE NOCASE" if sort_f == 'name'
                     else "city COLLATE NOCASE, name COLLATE NOCASE")

        def build_where(info):
            """Build (where_clause, params) for given db info.
            Uses indexed flag columns (f_*) when available (v3+), else LIKE fallback."""
            conds, params = [], []
            flags    = info.get('has_flags', False)
            deceased = info.get('has_deceased', False)

            if has_iban_f == '1':
                if info['has_iban']:
                    conds.append("iban != '' AND iban IS NOT NULL")
                else:
                    conds.append('data LIKE \'%"Bank_Account_Number__c": "_%\'')
            if has_notes_f == '1':
                conds.append("f_notes = 1" if flags else
                    "(data LIKE '%SObjectLog__c%' OR data LIKE '%Flash_Message__c%')")
            if has_pass_f == '1':
                conds.append("f_password = 1" if flags else
                    '(data LIKE \'%"Password__c": "_%\' OR data LIKE \'%"Portal_Password__c": "_%\''
                    ' OR data LIKE \'%"Wachtwoord__c": "_%\')')
            if has_pincode_f == '1':
                conds.append("f_pincode = 1" if flags else
                    '(data LIKE \'%"Pin__c": "_%\' OR data LIKE \'%"Pincode__c": "_%\')')
            if has_kvk_f == '1':
                conds.append("f_kvk = 1" if flags else
                    '(data LIKE \'%"Chamber_Of_Commerce_Number__c": "_%\''
                    ' OR data LIKE \'%"KvK_Number__c": "_%\')')
            if has_summons_f == '1':
                conds.append("f_summons = 1" if flags else
                    "(data LIKE '%aanmaning%' OR data LIKE '%sommatie%'"
                    " OR data LIKE '%incasso%' OR data LIKE '%deurwaarder%'"
                    " OR data LIKE '%ingebrekestell%')")
            if has_id_doc_f == '1':
                has_id_col = info.get('has_id_num', False)
                if has_id_col:
                    conds.append("(f_id_doc = 1 OR (id_number != '' AND id_number IS NOT NULL))")
                elif flags:
                    conds.append("f_id_doc = 1")
                else:
                    conds.append(
                        "(data LIKE '%rijbewijs%' OR data LIKE '%paspoort%'"
                        " OR data LIKE '%identiteitsbewijs%' OR data LIKE '%identiteitskaart%'"
                        " OR data LIKE '%ID-kaart%')"
                    )
            if has_bsn_f == '1':
                conds.append(
                    "(data LIKE '%BSN%' OR data LIKE '%burgerservicenummer%')"
                )
            if is_deceased_f == '1':
                conds.append("f_deceased = 1" if deceased else
                    "(data LIKE '%overled%' OR data LIKE '%verlijden%'"
                    " OR data LIKE '%nabestaand%' OR data LIKE '%gestorven%')")
            if is_active_f == '1':
                conds.append("status = 'Active'")
            if is_inactive_f == '1':
                conds.append("status = 'Inactive'")
            if city_f.strip():
                conds.append("city LIKE ? COLLATE NOCASE")
                params.append(f'%{city_f.strip()}%')
            return (' AND '.join(conds) if conds else '1=1'), params

        # ── Single DB: proper SQL COUNT + OFFSET/LIMIT ─────────────────────
        if len(db_names) == 1:
            db_name = db_names[0]
            info    = _DB_INFO.get(db_name)
            if not info:
                return self.send_json({'results': [], 'total': 0, 'page': 1, 'pages': 0})
            db = get_db(db_name)
            if not db:
                return self.send_json({'results': [], 'total': 0, 'page': 1, 'pages': 0})

            where, params = build_where(info)
            total  = db.execute(f"SELECT COUNT(*) FROM accounts WHERE {where}", params).fetchone()[0]
            pages  = max(1, (total + limit - 1) // limit)
            offset = (page - 1) * limit
            rows   = db.execute(
                f"SELECT {info['cols']} FROM accounts WHERE {where}"
                f" ORDER BY {order_sql} LIMIT ? OFFSET ?",
                params + [limit, offset]
            ).fetchall()
            results = []
            for r in rows:
                d = dict(r)
                if not info['has_iban']:
                    d['iban'] = ''
                if not info.get('has_id_num'):
                    d['id_number'] = ''
                    d['id_type'] = ''
                    d['id_valid'] = ''
                    d['birthdate'] = ''
                    d['nationality'] = ''
                    d['gender'] = ''
                d['_db'] = db_name
                results.append(d)
            return self.send_json({'results': results, 'total': total, 'page': page, 'pages': pages})

        # ── Multi-DB: per-DB COUNT, then proportional page pull ────────────
        db_segments = []   # (db_name, info, db, total, where, params)
        grand_total = 0
        for db_name in db_names:
            info = _DB_INFO.get(db_name)
            if not info:
                continue
            db = get_db(db_name)
            if not db:
                continue
            where, params = build_where(info)
            total_db = db.execute(f"SELECT COUNT(*) FROM accounts WHERE {where}", params).fetchone()[0]
            db_segments.append((db_name, info, db, total_db, where, params))
            grand_total += total_db

        pages         = max(1, (grand_total + limit - 1) // limit)
        global_offset = (page - 1) * limit
        results       = []
        remaining     = limit
        cumulative    = 0

        for db_name, info, db, total_db, where, params in db_segments:
            if remaining <= 0:
                break
            local_start = global_offset - cumulative
            if local_start >= total_db:
                cumulative += total_db
                continue
            local_start = max(0, local_start)
            take = min(remaining, total_db - local_start)
            rows = db.execute(
                f"SELECT {info['cols']} FROM accounts WHERE {where}"
                f" ORDER BY {order_sql} LIMIT ? OFFSET ?",
                params + [take, local_start]
            ).fetchall()
            for r in rows:
                d = dict(r)
                if not info['has_iban']:
                    d['iban'] = ''
                if not info.get('has_id_num'):
                    d['id_number'] = ''
                    d['id_type'] = ''
                    d['id_valid'] = ''
                    d['birthdate'] = ''
                    d['nationality'] = ''
                    d['gender'] = ''
                d['_db'] = db_name
                results.append(d)
            remaining  -= len(rows)
            cumulative += total_db

        # Re-sort combined slice for display consistency
        if sort_f == 'name':
            results.sort(key=lambda r: (r.get('name') or '').lower())
        else:
            results.sort(key=lambda r: ((r.get('city') or '').lower(),
                                        (r.get('name') or '').lower()))

        self.send_json({'results': results, 'total': grand_total, 'page': page, 'pages': pages})

    # ── /api/filter_counts ────────────────────────────────────────────────
    def handle_filter_counts(self, dbs_str):
        selected = set(dbs_str.split(',')) - {''} if dbs_str else set()
        db_names = [n for n in _DB_ORDER if (not selected or n in selected)]
        keys = ('has_iban', 'has_notes', 'has_password', 'has_pincode', 'has_kvk',
                'has_summons', 'has_id_doc', 'has_bsn', 'is_deceased',
                'is_active', 'is_inactive')
        totals = {k: 0 for k in keys}
        totals['ready'] = _counts_ready
        with _counts_lock_fc:
            for db_name in db_names:
                c = _FILTER_COUNTS.get(db_name, {})
                for key in keys:
                    totals[key] += c.get(key, 0)
        self.send_json(totals)

    # ── /api/cities ───────────────────────────────────────────────────────
    def handle_cities(self, dbs_str):
        selected = set(dbs_str.split(',')) - {''} if dbs_str else set()
        db_names = [n for n in _DB_ORDER if (not selected or n in selected)]

        all_cities = set()
        for db_name in db_names:
            db = get_db(db_name)
            if not db:
                continue
            rows = db.execute(
                "SELECT DISTINCT city FROM accounts "
                "WHERE city != '' AND city IS NOT NULL "
                "ORDER BY city COLLATE NOCASE"
            ).fetchall()
            all_cities.update(r[0] for r in rows if r[0] and r[0].strip())

        self.send_json({'cities': sorted(all_cities, key=str.lower)})

    # ── /api/stats ────────────────────────────────────────────────────────
    def handle_stats(self):
        total_records = 0
        total_size_mb = 0.0
        per_db = []

        for name in _DB_ORDER:
            info = _DB_INFO.get(name)
            if not info:
                continue
            db = get_db(name)
            if not db:
                continue
            count   = db.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
            size_mb = info['path'].stat().st_size / 1024 / 1024
            total_records += count
            total_size_mb += size_mb
            entry = {'name': name, 'records': count, 'sizeMB': f'{size_mb:.1f}'}
            if info['has_iban']:
                entry['withIban']  = db.execute("SELECT COUNT(*) FROM accounts WHERE iban  <> ''").fetchone()[0]
                entry['withPhone'] = db.execute("SELECT COUNT(*) FROM accounts WHERE phone <> ''").fetchone()[0]
                entry['withEmail'] = db.execute("SELECT COUNT(*) FROM accounts WHERE email <> ''").fetchone()[0]
            per_db.append(entry)

        self.send_json({
            'total':     total_records,
            'dbSizeMB':  f'{total_size_mb:.1f}',
            'hasIban':   any(_DB_INFO[n]['has_iban'] for n in _DB_ORDER if n in _DB_INFO),
            'databases': per_db,
        })

    # ── Serve HTML ────────────────────────────────────────────────────────
    def serve_html(self):
        html_path = BASE_DIR / 'index.html'
        try:
            data = html_path.read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except FileNotFoundError:
            self.send_error(404, 'index.html not found')

    # ── JSON helper ───────────────────────────────────────────────────────
    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type',   'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)


# ─── Multi-threaded server ─────────────────────────────────────────────────
class ThreadedHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads      = True
    allow_reuse_address = True


# ─── Entry point ──────────────────────────────────────────────────────────
def main():
    addr = f'http://localhost:{PORT}'
    print()
    print('  +==========================================+')
    print(f'  |  >> Server actief                        |')
    print('  +==========================================+')
    print()
    print(f'  Adres      :  {addr}')
    print(f'  Databases  :  {len(_DB_ORDER)}')
    print(f'  Records    :  {_fmt(_total_records)}')
    print()
    print(f'  Druk op Ctrl+C om te stoppen.')
    print()

    threading.Timer(1.0, lambda: webbrowser.open(addr)).start()

    server = ThreadedHTTPServer(('127.0.0.1', PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n\nServer stopped. Goodbye!')


if __name__ == '__main__':
    main()
