cip-common (local)
===================

Small local package containing shared utilities used across the CIP system.
This README explains how to install it in editable mode and how to use it quickly.

Quick install (recommended)
---------------------------
1. Create and activate a virtual environment (macOS / zsh):

```bash
python -m venv .venv
source .venv/bin/activate
```

2. From the repository root (the directory that contains `common/`) run:

```bash
pip install -e ./common
```

This installs the package in "editable" mode. Dependencies declared in
`common/pyproject.toml` (e.g. pandas, psycopg2-binary, Deprecated) will be
installed into the active environment.

Quick usage
-----------
After installation you can import the shared modules from any script in the
project.

Example `test_import.py`:

```python
from common import db     # import module re-exported by common
from common import utils

print('common version:', getattr(__import__('common'), '__version__', 'unknown'))
print('to_timestamp example:', utils.to_timestamp(1690000000))
```

Run it:

```bash
python test_import.py
```

Alternatives (dev only)
-----------------------
- If you prefer not to install, add the repo root to PYTHONPATH:

```bash
export PYTHONPATH="$PWD:$PYTHONPATH"
python server/main.py
```

- Or add the repo root to `sys.path` at runtime (not recommended for
production).
