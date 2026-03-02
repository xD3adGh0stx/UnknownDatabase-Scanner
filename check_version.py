"""
UnknownDatabase Scanner - Version Checker
Checks if any databases need migration.
Returns: exits with code 1 if migration needed, 0 if all up-to-date
"""
import json
from pathlib import Path
import sys

BASE_DIR      = Path(__file__).parent
MANIFEST_PATH = BASE_DIR / 'databases.json'
CURRENT_VERSION = 4

if not MANIFEST_PATH.exists():
    sys.exit(0)  # No databases yet

try:
    manifest = json.loads(MANIFEST_PATH.read_text('utf-8'))
    if not manifest:
        sys.exit(0)

    # Check if any databases are outdated
    outdated = [e for e in manifest if e.get('version', 1) < CURRENT_VERSION]

    if outdated:
        print(f'  ⚠️  {len(outdated)} database(s) need updating:')
        for e in outdated:
            old_v = e.get('version', 1)
            print(f'    - "{e["name"]}" (v{old_v} → v{CURRENT_VERSION})')
        print(f'  Use option 4 to migrate.')
        sys.exit(1)  # Migration needed
    else:
        if manifest:
            print(f'  ✓ All {len(manifest)} database(s) are up-to-date.')

except Exception:
    pass

sys.exit(0)  # All up-to-date

