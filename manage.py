"""
UnknownDatabase Scanner - Database Manager
CLI tool for listing and deleting databases.
Used by menu.bat; runs without starting the web server.
"""
import json
import sys
import time
from pathlib import Path

BASE_DIR      = Path(__file__).parent
MANIFEST_PATH = BASE_DIR / 'databases.json'

# ANSI colours (work in Windows Terminal / modern cmd)
C_RESET  = '\033[0m'
C_CYAN   = '\033[96m'
C_GREEN  = '\033[92m'
C_RED    = '\033[91m'
C_YELLOW = '\033[93m'
C_BOLD   = '\033[1m'


def load_manifest():
    if MANIFEST_PATH.exists():
        try:
            data = json.loads(MANIFEST_PATH.read_text('utf-8'))
            if isinstance(data, list):
                return data
        except Exception:
            pass
    legacy = BASE_DIR / 'database.db'
    if legacy.exists():
        manifest = [{'name': 'Database 1', 'file': 'database.db'}]
        MANIFEST_PATH.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), 'utf-8')
        return manifest
    return []


def save_manifest(manifest):
    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), 'utf-8')


def list_databases():
    manifest = load_manifest()
    if not manifest:
        print(f'{C_YELLOW}  No databases found.{C_RESET}')
        print(f'  Run option 2 (Import database) first.\n')
        return manifest

    print(f'\n  {C_BOLD}{"#":<4} {"Name":<30} {"File":<35} {"Size":>10}{C_RESET}')
    print(f'  {"─"*80}')
    for i, entry in enumerate(manifest, 1):
        path = BASE_DIR / entry['file']
        if path.exists():
            size_mb = path.stat().st_size / 1024 / 1024
            size_str = f'{size_mb:.1f} MB'
            status = C_GREEN + '✓' + C_RESET
        else:
            size_str = 'file missing'
            status = C_RED + '✗' + C_RESET
        print(f'  {status} {i:<3} {entry["name"]:<30} {entry["file"]:<35} {size_str:>10}')
    print()
    return manifest


def delete_database():
    manifest = list_databases()
    if not manifest:
        return

    try:
        choice = input(f'  Which number do you want to delete? (0 = cancel): ').strip()
    except (KeyboardInterrupt, EOFError):
        print()
        return

    if choice == '0' or choice == '':
        print('  Cancelled.\n')
        return

    try:
        idx = int(choice) - 1
        if idx < 0 or idx >= len(manifest):
            raise ValueError
    except ValueError:
        print(f'{C_RED}  Invalid number.{C_RESET}\n')
        return

    entry = manifest[idx]
    name  = entry['name']
    fpath = BASE_DIR / entry['file']

    print(f'\n  Database : {C_CYAN}{name}{C_RESET}')
    print(f'  File     : {entry["file"]}')
    if fpath.exists():
        size_mb = fpath.stat().st_size / 1024 / 1024
        print(f'  Size     : {size_mb:.1f} MB')
    print()

    try:
        confirm = input(f'  {C_RED}Are you sure? This cannot be undone. (yes/no): {C_RESET}').strip().lower()
    except (KeyboardInterrupt, EOFError):
        print()
        return

    if confirm not in ('yes', 'y'):
        print('  Cancelled.\n')
        return

    # Delete the file and WAL/SHM sidecars
    deleted = False
    if fpath.exists():
        last_err = None
        for attempt in range(6):
            try:
                for suffix in ('', '-wal', '-shm'):
                    sidecar = fpath.parent / (fpath.name + suffix)
                    if sidecar.exists():
                        sidecar.unlink()
                deleted = True
                last_err = None
                break
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        if last_err:
            print(f'{C_RED}  [ERROR] Could not delete file: {last_err}{C_RESET}')
            print(f'  Make sure the scanner is not running and try again.\n')
            return
    else:
        deleted = True  # File already gone, just clean up manifest

    # Update manifest
    new_manifest = [e for e in manifest if e['name'] != name]
    save_manifest(new_manifest)

    if deleted:
        print(f'{C_GREEN}  Database "{name}" has been deleted.{C_RESET}\n')
    else:
        print(f'{C_YELLOW}  Database "{name}" removed from list (file was already gone).{C_RESET}\n')


def main():
    action = sys.argv[1] if len(sys.argv) > 1 else ''

    if action == 'list':
        list_databases()
    elif action == 'delete':
        delete_database()
    else:
        print(f'Usage: python manage.py list | delete')
        sys.exit(1)


if __name__ == '__main__':
    main()
