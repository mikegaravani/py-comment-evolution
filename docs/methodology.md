# Methodology

## Snapshot Policy

For each specific repository, in both time periods (2000s and 2020s), we analyze a specific named release. The snapshot corresponding to that release is retrieved from Software Heritage.

The release to be analyzed is chosen based on project maturity and coherence with other repo snapshots. For example, we want the various snapshots to have a similar lifecycle stage in their repos.

If a repository does not have any releases, or there are only releases deemed not fit, then a snapshot of the codebase will be taken at a set point in time from the repository creation date.

All selected releases and snapshots must be uniquely identifiable via Software Heritage identifiers (SWHIDs).
