import json
import sys
from pathlib import Path

DATA_DIR = Path("data/raw/software_heritage")

EXIT_OK = 0
EXIT_MISSING_MANIFEST = 10
EXIT_INVALID_JSON = 11
EXIT_STATUS_ERROR = 12
EXIT_NO_DATA = 13


def check_manifests() -> int:
    errors_missing = []
    errors_json = []
    errors_status = []
    checked = 0

    for repo_dir in DATA_DIR.iterdir():
        if not repo_dir.is_dir():
            continue

        for version_dir in repo_dir.iterdir():
            if not version_dir.is_dir():
                continue

            checked += 1
            manifest_path = version_dir / "_MANIFEST.json"
            name = f"{repo_dir.name}/{version_dir.name}"

            if not manifest_path.exists():
                errors_missing.append(f"{name}: missing _MANIFEST.json")
                continue

            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
            except Exception as e:
                errors_json.append(f"{name}: invalid JSON ({e})")
                continue

            if manifest.get("status") != "success":
                errors_status.append(
                    f"{name}: status is {manifest.get('status')!r}"
                )

    if checked == 0:
        print("No version directories found.")
        return EXIT_NO_DATA

    if errors_missing:
        print("Missing manifests:\n")
        for e in errors_missing:
            print("-", e)
        print(
            "This most likely means that Software Heritage does not yet have all"
            " the repositories ready to be fetched. The request for the missing repos has been sent through"
            " the Software Heritage API. Try again later."
        )
        return EXIT_MISSING_MANIFEST

    if errors_json:
        print("Invalid JSON manifests:\n")
        for e in errors_json:
            print("-", e)
        return EXIT_INVALID_JSON

    if errors_status:
        print("Manifest status errors:\n")
        for e in errors_status:
            print("-", e)
        print(
            "This most likely means that Software Heritage does not yet have all"
            " the repositories ready to be fetched. The request for the missing repos has been sent through"
            " the Software Heritage API. Try again later."
        )
        return EXIT_STATUS_ERROR

    print(f"All {checked} manifests are successful.")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(check_manifests())