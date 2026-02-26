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

BASE_DIR      = Path(__file__).parent
MANIFEST_PATH = BASE_DIR / 'databases.json'
PORT          = 3000

# ─── Manifest helpers ──────────────────────────────────────────────────────
def load_manifest():
    """Returns list of {name, file} dicts from databases.json."""
    if MANIFEST_PATH.exists():
        try:
            data = json.loads(MANIFEST_PATH.read_text('utf-8'))
            if isinstance(data, list) and data:
                return data
        except Exception:
            pass
    # Auto-detect legacy database.db and create manifest
    legacy = BASE_DIR / 'database.db'
    if legacy.exists():
        manifest = [{'name': 'Database 1', 'file': 'database.db'}]
        MANIFEST_PATH.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), 'utf-8')
        print('  [INFO] databases.json aangemaakt van bestaande database.db')
        return manifest
    return []

def save_manifest(manifest):
    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), 'utf-8')

# ─── Startup ───────────────────────────────────────────────────────────────
DATABASES = load_manifest()

if not DATABASES:
    print('\n[ERROR] Geen databases gevonden.')
    print('Voer import.bat uit om een database aan te maken.\n')
    input('Press Enter to exit...')
    sys.exit(1)

# ─── Per-database column detection ────────────────────────────────────────
def _detect_columns(db_path):
    conn = sqlite3.connect(str(db_path))
    cols = {row[1] for row in conn.execute("PRAGMA table_info(accounts)").fetchall()}
    conn.close()
    return cols

_BASE_COLS = 'id, name, phone, email, street, city, postal, country, status, segment, is_active, brand'
_FULL_COLS = 'id, name, phone, email, iban, street, city, postal, country, status, segment, is_active, brand'

# Build per-database info at startup
_DB_INFO  = {}   # name -> {path, cols, has_iban}
_DB_ORDER = []   # ordered list of db names

print(f'\n{"==="*15}')
print(f'  UnknownDatabase Scanner - Databases')
print(f'{"==="*15}')

for _entry in DATABASES:
    _name = _entry['name']
    _path = BASE_DIR / _entry['file']
    if not _path.exists():
        print(f'  [WAARSCHUWING] Bestand niet gevonden: {_path}')
        continue
    _cols_set = _detect_columns(_path)
    _has_iban = 'iban' in _cols_set
    _DB_INFO[_name] = {
        'path':     _path,
        'cols':     _FULL_COLS if _has_iban else _BASE_COLS,
        'has_iban': _has_iban,
    }
    _DB_ORDER.append(_name)
    print(f'  "{_name}"  ({_entry["file"]})  IBAN: {"ja" if _has_iban else "nee"}')

if not _DB_INFO:
    print('\n[ERROR] Geen bruikbare databases gevonden.')
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
        conn.execute("PRAGMA temp_store   = MEMORY")
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

# ─── Search one database, return set of matching IDs ──────────────────────
def _search_ids(db, q, field, has_iban):
    lq = f'%{q}%'

    if field == 'all':
        fts_ids = set()
        try:
            fts_q = escape_fts(q)
            rows  = db.execute("""
                SELECT a.id FROM accounts_fts f
                JOIN   accounts a ON a.rowid = f.rowid
                WHERE  accounts_fts MATCH ?
            """, (fts_q,)).fetchall()
            fts_ids = {r[0] for r in rows}
        except Exception:
            pass

        if has_iban:
            like_rows = db.execute("""
                SELECT id FROM accounts
                WHERE  name   LIKE ? COLLATE NOCASE
                   OR  phone  LIKE ?
                   OR  email  LIKE ? COLLATE NOCASE
                   OR  iban   LIKE ? COLLATE NOCASE
                   OR  street LIKE ? COLLATE NOCASE
                   OR  city   LIKE ? COLLATE NOCASE
                   OR  postal LIKE ?
                   OR  id     LIKE ?
            """, (lq,)*8).fetchall()
        else:
            like_rows = db.execute("""
                SELECT id FROM accounts
                WHERE  name   LIKE ? COLLATE NOCASE
                   OR  phone  LIKE ?
                   OR  email  LIKE ? COLLATE NOCASE
                   OR  street LIKE ? COLLATE NOCASE
                   OR  city   LIKE ? COLLATE NOCASE
                   OR  postal LIKE ?
                   OR  id     LIKE ?
            """, (lq,)*7).fetchall()

        like_ids = {r[0] for r in like_rows}
        all_ids  = fts_ids | like_ids

        if not all_ids:
            deep = db.execute(
                "SELECT id FROM accounts WHERE data LIKE ? COLLATE NOCASE", (lq,)
            ).fetchall()
            all_ids = {r[0] for r in deep}

        return all_ids

    elif field == 'iban':
        if has_iban:
            rows = db.execute(
                "SELECT id FROM accounts WHERE iban LIKE ? COLLATE NOCASE", (lq,)
            ).fetchall()
            ids = {r[0] for r in rows}
        else:
            ids = set()
        if not ids:
            deep = db.execute(
                "SELECT id FROM accounts WHERE data LIKE ? COLLATE NOCASE", (lq,)
            ).fetchall()
            ids = {r[0] for r in deep}
        return ids

    elif field == 'notes':
        deep = db.execute(
            "SELECT id FROM accounts WHERE data LIKE ? COLLATE NOCASE", (lq,)
        ).fetchall()
        return {r[0] for r in deep}

    elif field == 'phone':
        rows = db.execute(
            "SELECT id FROM accounts WHERE phone LIKE ?", (lq,)
        ).fetchall()
        ids = {r[0] for r in rows}
        if not ids:
            deep = db.execute(
                "SELECT id FROM accounts WHERE data LIKE ? COLLATE NOCASE", (lq,)
            ).fetchall()
            ids = {r[0] for r in deep}
        return ids

    elif field == 'email':
        rows = db.execute(
            "SELECT id FROM accounts WHERE email LIKE ? COLLATE NOCASE", (lq,)
        ).fetchall()
        ids = {r[0] for r in rows}
        if not ids:
            deep = db.execute(
                "SELECT id FROM accounts WHERE data LIKE ? COLLATE NOCASE", (lq,)
            ).fetchall()
            ids = {r[0] for r in deep}
        return ids

    else:
        field_map = {
            'name':   'name',
            'phone':  'phone',
            'email':  'email',
            'city':   'city',
            'postal': 'postal',
            'street': 'street',
            'id':     'id',
        }
        if has_iban:
            field_map['iban'] = 'iban'
        col   = field_map.get(field, 'name')
        q_val = q if field == 'id' else lq
        rows  = db.execute(
            f"SELECT id FROM accounts WHERE {col} LIKE ? COLLATE NOCASE", (q_val,)
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
            elif path in ('/', '/index.html'):     self.serve_html()
            else:
                self.send_error(404, 'Not found')
        except BrokenPipeError:
            pass
        except Exception as e:
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

            ids = _search_ids(db, q, field, info['has_iban'])
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
            })
        self.send_json(result)

    # ── /api/databases/rename ─────────────────────────────────────────────
    def handle_db_rename(self, old_name, new_name):
        global DATABASES, _DB_ORDER
        old_name = old_name.strip()
        new_name = new_name.strip()
        if not old_name or not new_name:
            return self.send_json({'error': 'Naam ontbreekt'}, 400)
        if new_name == old_name:
            return self.send_json({'ok': True})
        if new_name in _DB_INFO:
            return self.send_json({'error': 'Die naam bestaat al'}, 409)

        manifest = load_manifest()
        found    = False
        for entry in manifest:
            if entry['name'] == old_name:
                entry['name'] = new_name
                found = True
                break
        if not found:
            return self.send_json({'error': 'Database niet gevonden'}, 404)

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
            return self.send_json({'error': 'Naam ontbreekt'}, 400)

        manifest = load_manifest()
        entry    = next((e for e in manifest if e['name'] == name), None)
        if not entry:
            return self.send_json({'error': 'Database niet gevonden'}, 404)

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
                return self.send_json({'error': f'Kan bestand niet verwijderen: {last_err}'}, 500)

        manifest = [e for e in manifest if e['name'] != name]
        save_manifest(manifest)
        DATABASES = manifest

        self.send_json({'ok': True})

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
    print(f'\n{"="*44}')
    print(f'  UnknownDatabase Scanner - Running')
    print(f'{"="*44}')
    print(f'\n  Adres    : {addr}')
    print(f'  Databases: {len(_DB_ORDER)}')
    print(f'  Press Ctrl+C to stop.\n')

    threading.Timer(1.0, lambda: webbrowser.open(addr)).start()

    server = ThreadedHTTPServer(('127.0.0.1', PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n\nServer gestopt. Tot ziens!')


if __name__ == '__main__':
    main()
