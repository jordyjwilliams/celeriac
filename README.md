# Celeriac
[![Python](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Linting: ruff](https://img.shields.io/badge/linting-ruff-red.svg)](https://github.com/astral-sh/ruff)
[![Type checking: pyright](https://img.shields.io/badge/type%20checking-pyright-yellow.svg)](https://github.com/microsoft/pyright)

SWE 3/4 - Async Engineering Task

## Setup

This project uses [UV](https://github.com/astral-sh/uv) for dependency management.

### Prerequisites

- Python `>=3.13`.
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
