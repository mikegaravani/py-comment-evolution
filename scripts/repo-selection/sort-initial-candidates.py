import json
from pathlib import Path

# Takes the initial candidates for the 2020s and sorts them by the number of bytes of Python code
# USAGE: python scripts/repo-selection/sort-initial-candidates.py

FILE_PATH = Path("data/metadata/repo-selection/initial_candidates_2020s.json")
OUTPUT_FILE_PATH = Path("data/metadata/repo-selection/initial_candidates_2020s_sorted_by_size.json")

def get_python_bytes(repo):
    return repo.get("languages_bytes", {}).get("Python", 0)

def main():
    with open(FILE_PATH, "r", encoding="utf-8") as f:
        repos = json.load(f)

    sorted_repos = sorted(repos, key=get_python_bytes)

    reduced = [
        {
            "name": repo["name"],
            "python_bytes": get_python_bytes(repo),
            "repo_creation_date": repo.get("repo_creation_date", "")[:4],  # Just the year
        }
        for repo in sorted_repos
    ]

    with open(OUTPUT_FILE_PATH, "w", encoding="utf-8") as out:
        json.dump(reduced, out, indent=2)

    print(f"Sorted repos saved to {OUTPUT_FILE_PATH}")

if __name__ == "__main__":
    main()
