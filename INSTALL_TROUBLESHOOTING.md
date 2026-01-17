# Installation Troubleshooting

## Python 3.13 Compatibility Issues

If you're using Python 3.13 and encounter build errors with `psycopg2-binary`, `asyncpg`, or `pydantic-core`, try one of these solutions:

### Option 1: Use Python 3.11 or 3.12 (Recommended)

```bash
# Create venv with Python 3.11 or 3.12
python3.11 -m venv venv
# or
python3.12 -m venv venv

source venv/bin/activate
pip install -r requirements.txt
```

### Option 2: Install Core Dependencies First

```bash
source venv/bin/activate

# Upgrade pip and build tools
pip install --upgrade pip setuptools wheel

# Install core dependencies that work with Python 3.13
pip install redis fastapi uvicorn[standard] loguru python-multipart python-dotenv tenacity pytz psutil

# Try installing problematic packages with workarounds
pip install pydantic --upgrade
pip install psycopg2-binary --no-build-isolation || pip install psycopg2
pip install asyncpg --no-build-isolation || pip install asyncpg --pre
```

### Option 3: Use Alternative Packages

If build issues persist, you can temporarily use alternatives:

```bash
# Use psycopg (newer, pure Python) instead of psycopg2-binary
pip install psycopg[binary]

# Update config.py to use psycopg instead of psycopg2
```

## Frontend Issues

### PostCSS Config Error

If you see `SyntaxError: Unexpected token 'export'`:

The `postcss.config.js` should use CommonJS syntax. It's already fixed in the repo, but if you see this error:

1. Make sure `postcss.config.js` uses `module.exports` (not `export default`)
2. Delete `node_modules` and reinstall: `rm -rf node_modules && npm install`

### Vite Config

Vite config files (`vite.config.js`, `tailwind.config.js`) should use ES modules (`export default`), which is correct.

## Quick Fix Commands

```bash
# Backend - Install without problematic packages first
source venv/bin/activate
pip install redis fastapi uvicorn[standard] loguru python-multipart python-dotenv tenacity pytz psutil pytest pytest-asyncio pytest-cov pytest-mock fakeredis locust memory-profiler

# Frontend - Clean install
cd frontend
rm -rf node_modules package-lock.json
npm install
```

## Verify Installation

```bash
# Check Python packages
source venv/bin/activate
python -c "import redis, fastapi, pydantic; print('Core packages OK')"

# Check Node packages
cd frontend
npm list --depth=0
```
