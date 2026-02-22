import requests

"""
Script required to run to get the snapshots for:
 - scons
 - typer
 - textualize
For the chosen releases of these repos, resolve-snapshots.py doesn't work.
This script only prints the output, it doesn't append it in snapshots.csv, so the output should be copied and pasted in snapshots.csv.
This script doesn't need to be run again, but if I have time left over, I'll integrate this in the main resolve-snapshots.py script for better maintainability.

USAGE:
    python scripts/repo-collection/resolve-edge-cases/resolve-extra-snapshots.py
"""

BASE = "https://archive.softwareheritage.org/api/1"

SCONS_REVISION_ID = "6b937e9999a51b919b0720741a5fa7b76f6e6186"
TYPER_REVISION_ID = "13619fb4a35ee92a276215889c68ea03633c24dd"
TEXTUALIZE_REVISION_ID = "f888e6ae4afc74c8b4d9bc4a18d17071b453a780"

REVISION_ID = TEXTUALIZE_REVISION_ID

NAME = "textualize"
GROUP = ""
ORIGIN_URL = "https://github.com/Textualize/textual"
RELEASE = "1.0.0"
TAG_REF = "refs/tags/1.0.0"
SNAPSHOT_ID = ""

def get_json(url):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

rev = get_json(f"{BASE}/revision/{REVISION_ID}/")

directory_id = rev["directory"]
release_date = rev["date"]

print("name,group,origin_url,release,release_date,tag_ref,snapshot_id,revision_id,directory_id")
print(
    f"{NAME},{GROUP},{ORIGIN_URL},{RELEASE},{release_date},"
    f"{TAG_REF},{SNAPSHOT_ID},{REVISION_ID},{directory_id}"
)