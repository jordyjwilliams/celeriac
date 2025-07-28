# Celeriac

SWE 3/4 - Async Engineering Task

## Setup

This project uses [UV](https://github.com/astral-sh/uv) for dependency management.

### Prerequisites

- Python 3.13 or higher
- UV (install with `pip install uv`)

### Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   uv sync --extra dev
   ```

### Running Tests

```bash
uv run pytest
```

For verbose output:
```bash
uv run pytest -v
```

## Project Structure

- `test_celeriac.py` - Test files and main application functionality
- `queue.py` - Celeriac queue implementation
- `tasks.py` - CeleriacTask class implementation
- `executor.py` - MockTaskExecutor for task execution
- `pyproject.toml` - Project configuration and dependencies
