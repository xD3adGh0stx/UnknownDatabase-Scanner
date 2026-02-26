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
        print(f'{C_YELLOW}  Geen databases gevonden.{C_RESET}')
        print(f'  Voer eerst "Importeer database" uit.\n')
        return manifest

    print(f'\n  {C_BOLD}{"#":<4} {"Naam":<30} {"Bestand":<35} {"Grootte":>10}{C_RESET}')
    print(f'  {"─"*80}')
    for i, entry in enumerate(manifest, 1):
        path = BASE_DIR / entry['file']
        if path.exists():
            size_mb = path.stat().st_size / 1024 / 1024
            size_str = f'{size_mb:.1f} MB'
            status = C_GREEN + '✓' + C_RESET
        else:
            size_str = 'bestand mist'
            status = C_RED + '✗' + C_RESET
        print(f'  {status} {i:<3} {entry["name"]:<30} {entry["file"]:<35} {size_str:>10}')
    print()
    return manifest


def delete_database():
    manifest = list_databases()
    if not manifest:
        return

    try:
        keuze = input(f'  Welk nummer wil je verwijderen? (0 = annuleren): ').strip()
    except (KeyboardInterrupt, EOFError):
        print()
        return

    if keuze == '0' or keuze == '':
        print('  Geannuleerd.\n')
        return

    try:
        idx = int(keuze) - 1
        if idx < 0 or idx >= len(manifest):
            raise ValueError
    except ValueError:
        print(f'{C_RED}  Ongeldig nummer.{C_RESET}\n')
        return

    entry = manifest[idx]
    name  = entry['name']
    fpath = BASE_DIR / entry['file']

    print(f'\n  Database : {C_CYAN}{name}{C_RESET}')
    print(f'  Bestand  : {entry["file"]}')
    if fpath.exists():
        size_mb = fpath.stat().st_size / 1024 / 1024
        print(f'  Grootte  : {size_mb:.1f} MB')
    print()

    try:
        confirm = input(f'  {C_RED}Weet je het zeker? Dit kan niet ongedaan worden. (ja/nee): {C_RESET}').strip().lower()
    except (KeyboardInterrupt, EOFError):
        print()
        return

    if confirm not in ('ja', 'j', 'yes', 'y'):
        print('  Geannuleerd.\n')
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
            print(f'{C_RED}  [FOUT] Kan bestand niet verwijderen: {last_err}{C_RESET}')
            print(f'  Zorg dat de scanner niet actief is en probeer opnieuw.\n')
            return
    else:
        deleted = True  # File already gone, just clean up manifest

    # Update manifest
    new_manifest = [e for e in manifest if e['name'] != name]
    save_manifest(new_manifest)

    if deleted:
        print(f'{C_GREEN}  Database "{name}" is verwijderd.{C_RESET}\n')
    else:
        print(f'{C_YELLOW}  Database "{name}" verwijderd uit lijst (bestand was al weg).{C_RESET}\n')


def main():
    action = sys.argv[1] if len(sys.argv) > 1 else ''

    if action == 'list':
        list_databases()
    elif action == 'delete':
        delete_database()
    else:
        print(f'Gebruik: python manage.py list | delete')
        sys.exit(1)


if __name__ == '__main__':
    main()
