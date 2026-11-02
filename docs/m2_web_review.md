# M2 web integration review — 27 October 2026

Reviewed repository revision: `aa766973b5ba23653eb9b99026c3c0f4d1dadc68`.
Scope: the M2 column in the optional-web work plan, plus the regular M2 routing
and submission work for October 26–27. This is a source review of the already
integrated web layer, not a claim that M2 authored M5's implementation.

## Decision and delivery status

Harsh confirmed that the mentor approved the optional web dashboard. The
confirmation is recorded in `docs/web_contract.md`; no mentor message was sent
on his behalf. The architecture now separates current behavior from the target
shared snapshot and signed submission paths.

The web files are already present on main. There was no open web PR returned
by the repository search during this review. Future web changes should be
reviewed in a dedicated PR, separate from routine DHT/routing changes. This
review does not retroactively approve or merge another member's work.

## Findings requiring follow-up

### High: browser submissions bypass the required signed submission path

Evidence: the nested `submit_demo` callback in `cli/main.py:start` calls
`engine.submit_task(task)`. HTTP token checks in `web/server.py` protect access
to that callback, but do not add node signatures or select a remote worker.

Owners: M4 and M1 for the shared signed adapter; M5 for wiring it into the web
callback. Until available, use a read-only server configuration for compliant
web demonstrations. Add an integration test proving the signed adapter is
invoked and invalid signatures cannot reach execution. Do not claim the existing
web demo proves signed remote routing.

### Medium: snapshot producers and the written schema disagree

Evidence: `web/snapshot.py:build_snapshot` returns `snapshot_at` and
`nodes[].node_id` with ISO peer timestamps, while the contract specifies `ts`,
`id` and now explicitly defines canonical timestamp units. `_normalise_task`
uppercases `COMPLETED` without converting it to `DONE`. The current HTML reads
`node_id`, so changing only the producer would break the display.

Owner: M5, with M3/M4 confirming semantics. Migrate producer, consumers and tests
together, provide temporary aliases, and cover DONE/cancellation/unassigned
node handling. Reuse the live builder through one shared snapshot entry point.
The second screenshot's snapshot/Rich tasks belong to M5, not M2.

### Medium: LAN binding and slow HTTP reads need explicit controls

Evidence: the CLI passes its node `host` to `create_dashboard_server`, so binding
the node to a LAN interface also exposes HTTP. The handler limits body size but
has no configured connection timeout before reading headers/body.

Owner: M5 with M1 security review. Keep HTTP loopback by default independently
of the node address, require explicit LAN opt-in/warning, and bound reads. Test
slow/incomplete headers and bodies without tying up unbounded handler threads.

## Regular M2 work for October 26–27

| Scheduled work | Existing implementation | Status |
|---|---|---|
| Lowest-CPU routing, RAM then ID tie-break | `tasks/router.py:select_node` | Implemented; excludes unhealthy/stale/overloaded peers |
| Serialize, route, send, receive result | `TaskClient` + registered-task TCP adapter | Implemented for the current named-function contract |
| Original callable submit interface | Callable serializer exists in execution; TaskClient still takes names | Cross-module integration remains; do not claim complete |
| Retry failed transport on another worker | `tasks/submit.py` | Implemented with preserved task ID and bounded attempts |
| Mentor web decision | `docs/web_contract.md` | Approval reported by Harsh on October 27 |
| Architecture review | `docs/architecture.md` and this file | Documented; findings await implementation by owners |

No commits have been backdated. This change records work performed on October 27;
it does not claim activity or agreements on October 26 without evidence.

## Handoff message (draft for Harsh to share)

Mentor approval for the optional web view is recorded in the contract. M5 should
provide one shared snapshot entry point for Rich and web, reusing the current
live builder and migrating field aliases safely. M3 please confirm peer fields,
timestamp units and health events. M4/M1 please confirm task-state mapping and
connect the demo callback to the signed submission path; it currently queues
locally. The detailed review and acceptance criteria are in docs/m2_web_review.md
and docs/web_contract.md. Please keep the follow-up web fixes in a dedicated PR.
