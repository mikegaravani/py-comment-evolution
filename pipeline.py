import subprocess
import sys
from pathlib import Path

# These files were previously thought of as singular scripts
# rather than pipeline steps, which is why they are organized as separate executables.
steps = [
    ["scripts/repo-collection/fetch-swh.py"],
    ["scripts/repo-collection/enrichment/check_all_manifests.py"],
    ["scripts/repo-collection/enrichment/unpack-virtualenv-embedded.py"],
    ["scripts/repo-census/repo-census.py", "--all"],
    ["scripts/comment-extraction/build-file-index.py", "--also-write-csv"],
    ["scripts/comment-extraction/extract-token-comments.py", "--subset", "core", "--write-file-status"],
    ["scripts/comment-extraction/extract-token-comments.py", "--subset", "tests_only", "--write-file-status"],
    ["scripts/comment-extraction/extract-docstring-comments.py", "--subset", "core", "--write-file-status"],
    ["scripts/comment-extraction/extract-docstring-comments.py", "--subset", "tests_only", "--write-file-status"],
    ["scripts/comment-metrics/build-comment-blocks/build-comment-blocks.py", "--subset", "core"],
    ["scripts/comment-metrics/build-comment-blocks/build-comment-blocks.py", "--subset", "tests_only"],
    ["scripts/comment-metrics/enrich_blocks/enrich_blocks.py", "--subset", "core"],
    ["scripts/comment-metrics/enrich_blocks/enrich_blocks.py", "--subset", "tests_only"],
    ["scripts/calculate-results/run_results_pipeline.py", "--subset", "core"],
    ["scripts/calculate-results/run_results_pipeline.py", "--subset", "tests_only"],
]
viz_dir = Path("scripts/visualize-results")
for script in sorted(viz_dir.glob("*.py")):
    steps.append([str(script), "--subset", "core"])
    steps.append([str(script), "--subset", "tests_only"])



for step in steps:
    print(f"\nRunning: {' '.join(step)}")
    result = subprocess.run([sys.executable] + step)

    if result.returncode != 0:
        print(f"Step failed: {' '.join(step)}")
        sys.exit(result.returncode)

print("\nPipeline completed successfully! View the results inside results/ and the datasets inside data/processed/!")