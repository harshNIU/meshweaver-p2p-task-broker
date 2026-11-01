# Member 5 Viva Notes — CLI, Dashboard, Integration and QA

## 1. What is MeshWeaver?

MeshWeaver is a peer-to-peer task broker. It allows nodes to communicate over a network and supports task submission and execution. My contribution focuses on the command-line interface, dashboard integration, testing, documentation, and overall project validation.

## 2. What is the role of the CLI?

The CLI provides commands to start a MeshWeaver node and submit tasks to a remote node. It is implemented in `cli/main.py` using Typer. Command-line options allow users to configure host addresses, ports, task arguments, timeouts, and optional bootstrap discovery.

## 3. How do you start a node?

Run `python -m cli.main start`. By default, the node binds to `127.0.0.1` on UDP port `8000`. A different port can be selected using `--port`, and `--host` and `--node-id` can customize the node configuration.

## 4. What is bootstrap discovery?

Bootstrap discovery lets a node contact an existing node to request peer information. The CLI supports `--bootstrap host:port` and the backward-compatible `--seed host:port` option. For example, a second node can use `--bootstrap 127.0.0.1:9000` to contact a node running on port `9000`.

## 5. How does the submit command work?

The `submit` command accepts a task name and optional JSON arguments. It validates the arguments, creates a task identifier, sends a `TASK_REQUEST` message to the target node, and waits for the matching `TASK_RESULT`. It displays the status and result when execution succeeds, or an error when the task fails or times out.

## 6. Why validate command-line inputs?

Input validation prevents invalid configuration from reaching the networking or execution layers. The CLI validates port ranges, positive finite timeouts, JSON syntax, and the expected JSON types for positional and keyword arguments. It reports useful errors instead of proceeding with invalid input.

## 7. What is the dashboard used for?

The optional web dashboard provides an HTTP interface associated with a running MeshWeaver node. It is enabled with `--web`, and its HTTP port can be configured using `--web-port`. The CLI connects the dashboard to a snapshot provider so it can expose information about the node and its current state.

## 8. Which tasks can the dashboard submit?

The current CLI dashboard integration allowlists the built-in `add` and `multiply` tasks. The submission handler validates the task name and argument types before creating and submitting a task to the local execution engine. Other task names are rejected by this handler.

## 9. How is dashboard task submission protected?

The dashboard server provides a submission token. The CLI prints the token and instructs clients to send it in the `X-Token` HTTP header for `POST /api/submit`. The allowlist and input validation restrict what the dashboard submission handler accepts. These measures should not be confused with complete network security or encryption.

## 10. How does the CLI communicate with a remote node?

The CLI creates a UDP node for the client, constructs a `TASK_REQUEST` message, and sends it to the selected destination. It then waits for a `TASK_RESULT` matching the request sequence and task ID. If no matching result arrives within the configured timeout, the command reports a timeout.

## 11. What is integration testing?

Integration testing checks whether components work together correctly, rather than only testing each component in isolation. For MeshWeaver, this includes the CLI, UDP networking, bootstrap discovery, task execution, task-result handling, and dashboard integration.

## 12. Why do the CLI tests use fake objects?

Fake transports and protocols let tests verify CLI behavior without relying on real network communication. They can check messages, destinations, validation, results, failures, timeouts, and transport cleanup in a controlled and repeatable way. Separate integration tests are still needed to validate real component interactions.

## 13. What is the purpose of pytest, Black, and Ruff?

- `pytest` runs the automated tests to check expected behavior.
- `black --check` checks whether Python files follow the project's formatting.
- `ruff check` detects supported code-quality issues.

All three checks provide different types of validation; passing one does not replace the others.

## 14. What is continuous integration (CI)?

Continuous integration runs automated checks when code is pushed or a pull request is updated. GitHub Actions helps detect regressions before changes are integrated into the main branch. A successful CI run indicates that the configured workflow passed for that commit.

## 15. What does a clean Git status mean?

A clean Git status means there are no uncommitted changes in the working tree. If the local branch also matches its remote-tracking branch, there are no local commits ahead of or behind that tracked branch at that point. This helps confirm that work is ready to be reviewed or continued.

## 16. What validation was completed for the current checkpoint?

The full pytest suite reported 175 passing tests. Black reported that 67 files would remain unchanged, and Ruff reported that all checks passed. The latest verified GitHub Actions push and pull-request runs also succeeded for the current branch commit. These results describe the tested checkpoint and should be rechecked if the code changes.

## 17. What would you demonstrate during the viva?

I would explain the project architecture, show the CLI help, start a node, demonstrate bootstrap configuration, and describe how task submission and results work. If demonstrating the dashboard, I would show how it is enabled, explain the snapshot and token-based submission flow, and use only a safe, supported demo task. I would also show the automated test and CI results.

## 18. What are the limitations to acknowledge?

A passing test suite does not prove that every deployment or network condition is safe. Bootstrap discovery can fail if the seed node is unavailable or unreachable. Task submission can time out if a result is not received. Dashboard submission is restricted to the configured allowlist, and the submission token alone does not provide encryption or comprehensive network authentication.

## Short Closing Statement

My contribution focuses on making MeshWeaver easier to operate and validate through the CLI, dashboard integration, automated testing, documentation, and CI checks. At this checkpoint, the full test suite, Black, Ruff, and the verified GitHub Actions runs passed. I would demonstrate the actual supported workflows and clearly explain the project's remaining limitations.
