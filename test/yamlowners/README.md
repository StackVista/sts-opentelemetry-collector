# Product YAML owner contracts

Run `GOWORK=off go test -race ./...` here. Replacements intentionally select the
same complete owners as each OCB executable, independently of `go.work`.

Tests call the actual OTTL UserAgent factory, SDK profile/client construction
and Prometheus Scaleway custom YAML decoder. Discovery is never started and
there are no provider requests. Expected results are tied to the selected UAP
v3 generation and Scaleway beta36, not an older fork.

For baseline qualification, copy this module outside the workspace and change
only the two replacement paths to pristine exact upstream Git checkouts; UAP
must have its pinned core submodule initialized. Run the same tests on both.
The collector CI also executes owner suites and the source inventory guard.
