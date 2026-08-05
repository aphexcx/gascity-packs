
Resolve `<source-anchor-id>` using the same rules as `prepare-worktree`. Read `work_dir` from the source anchor and verify the implementation commit and
summary evidence are present in that worktree. Write per-item summary to
{{summary_path}} when set. If `summary_path` is not set, first use
`gc.implementation.summary_path` from the preceding implementation step when it
is present; otherwise use `{{artifact_root}}/task-<source-anchor-id>-summary.md`.

On success, close only `<source-anchor-id>` with `gc.outcome=pass`. Include the
verified commit and summary path in the source-anchor close reason. Read the
source anchor back with `gc bd show <source-anchor-id> --json` and verify
`status=closed` and `gc.outcome=pass`; if either check fails, fix the source
anchor before closing this step. Do not close this step with pass while the source anchor remains open. Then close this step. Do not close the drain-unit
convoy, parent convoy, or broader workflow root from this step.
