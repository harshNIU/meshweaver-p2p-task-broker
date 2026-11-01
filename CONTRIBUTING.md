# Contributing to MeshWeaver

Thank you for contributing to MeshWeaver — P2P Task Broker.

## Prerequisites

- Git
- Python 3.10 or newer
- PowerShell, Command Prompt, or another terminal

## Set up the project

Clone the repository and enter its directory:

```powershell
git clone https://github.com/harshNIU/meshweaver-p2p-task-broker.git
cd meshweaver-p2p-task-broker
```

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the project dependencies:

```powershell
python -m pip install -r requirements.txt
```

## Run the tests

Run the complete test suite before submitting changes:

```powershell
python -m pytest -v
```

## Check code quality

Run Black to check formatting:

```powershell
python -m black --check .
```

Run Ruff to check for lint issues:

```powershell
python -m ruff check .
```

Resolve relevant failures before committing. Do not modify unrelated files simply to silence checks.

## Branch and commit workflow

1. Start from the latest version of the appropriate base branch.
2. Create or use your assigned feature branch.
3. Keep changes focused on the task assigned to you.
4. Review your changes with `git status` and `git diff`.
5. Run the tests and code-quality checks.
6. Commit your changes with a clear, descriptive message.
7. Push your branch and open a pull request for review.

Do not commit virtual environments, generated files, credentials, or unrelated changes.

## Documentation and audit evidence

Update the README when setup instructions, supported features, or validation commands change.

Keep network-audit reports and other evidence in their existing documentation locations. Report test counts and audit results only when they have been verified by an actual run.

## Pull requests

A pull request should include:

- A summary of the changes.
- The reason for the changes.
- Tests and quality checks performed.
- Relevant logs or screenshots when required.
- Any known limitations or follow-up work.

Follow the team's review and merge process. Do not merge directly into the main branch unless authorized by the team lead.
