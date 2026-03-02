"""
UnknownDatabase Scanner - Interactive Migration Menu
Shows a list of databases and lets the user pick one to migrate.
Writes the chosen .db filename to stdout for menu.bat to capture.
"""
import json
import subprocess
import sys
from pathlib import Path

BASE_DIR        = Path(__file__).parent
MANIFEST_PATH   = BASE_DIR / 'databases.json'
CURRENT_VERSION = 4

# ANSI colours
C_RESET  = '\033[0m'
C_CYAN   = '\033[96m'
C_GREEN  = '\033[92m'
C_RED    = '\033[91m'
C_YELLOW = '\033[93m'
C_BOLD   = '\033[1m'

def load_manifest():
    if not MANIFEST_PATH.exists():
        return []
    try:
        data = json.loads(MANIFEST_PATH.read_text('utf-8'))
        if isinstance(data, list):
            for e in data:
                if 'version' not in e:
                    e['version'] = 1
            return data
    except Exception:
        pass
    return []


def main():
    manifest = load_manifest()

    if not manifest:
        print(f'{C_RED}  No databases found. Import one first.{C_RESET}')
        input('  Press Enter to continue...')
        sys.exit(1)

    # Separate outdated vs up-to-date
    outdated   = [e for e in manifest if e.get('version', 1) < CURRENT_VERSION]
    up_to_date = [e for e in manifest if e.get('version', 1) >= CURRENT_VERSION]

    print(f'\n  {C_BOLD}Databases available for migration:{C_RESET}')
    print(f'  {"─"*60}')

    all_entries = []  # combined list for numbering

    if outdated:
        print(f'  {C_YELLOW}Update required:{C_RESET}')
        for e in outdated:
            path = BASE_DIR / e['file']
            size = f'{path.stat().st_size / 1024 / 1024:.0f} MB' if path.exists() else 'file missing!'
            old_v = e.get('version', 1)
            i = len(all_entries) + 1
            all_entries.append(e)
            print(f'  {C_YELLOW}{i}{C_RESET}. {C_CYAN}{e["name"]}{C_RESET}')
            print(f'     File    : {e["file"]}  ({size})')
            print(f'     Status  : v{old_v} → v{CURRENT_VERSION}  {C_YELLOW}(update needed){C_RESET}')
            print()

    if up_to_date:
        print(f'  {C_GREEN}Already up-to-date (force re-migration possible):{C_RESET}')
        for e in up_to_date:
            path = BASE_DIR / e['file']
            size = f'{path.stat().st_size / 1024 / 1024:.0f} MB' if path.exists() else 'file missing!'
            i = len(all_entries) + 1
            all_entries.append(e)
            print(f'  {C_GREEN}{i}{C_RESET}. {C_CYAN}{e["name"]}{C_RESET}  {C_GREEN}(v{e.get("version", CURRENT_VERSION)} ✓){C_RESET}')
            print(f'     File    : {e["file"]}  ({size})')
            print(f'     Use if flag columns are empty (all filters = 0)')
            print()

    print(f'  {"─"*60}')

    try:
        choice = input(f'  Which number to migrate? (0 = cancel): ').strip()
    except (KeyboardInterrupt, EOFError):
        print()
        sys.exit(1)

    if choice == '0' or choice == '':
        print('  Cancelled.')
        sys.exit(1)

    try:
        idx = int(choice) - 1
        if idx < 0 or idx >= len(all_entries):
            raise ValueError
    except ValueError:
        print(f'{C_RED}  Invalid number.{C_RESET}')
        input('  Press Enter to continue...')
        sys.exit(1)

    chosen = all_entries[idx]
    db_path = BASE_DIR / chosen['file']

    if not db_path.exists():
        print(f'{C_RED}  File not found: {db_path}{C_RESET}')
        input('  Press Enter to continue...')
        sys.exit(1)

    print(f'\n  Migrating: {C_CYAN}{chosen["name"]}{C_RESET}')
    print(f'  File     : {chosen["file"]}')
    print()

    # Run migrate.py with the chosen file
    result = subprocess.run(
        [sys.executable, str(BASE_DIR / 'migrate.py'), str(db_path)],
        cwd=str(BASE_DIR)
    )
    sys.exit(result.returncode)


if __name__ == '__main__':
    main()
