# 4D to Snowflake ETL

This project is being built incrementally. The current milestone focuses only on proving that Python can connect successfully to both the 4D database and Snowflake.

## Current milestone

The first step is to validate the two required connections before implementing extraction, transformation, staging, history, or production refresh logic.

## Project structure

- config/
- connections/
- scripts/
- logs/
- tests/

## Local environment

1. Copy .env.example to .env
2. Fill in the real values for your 4D and Snowflake environment
3. Install requirements:

```powershell
py -3.12 -m pip install -r requirements.txt
```

4. Run connection verification:

```powershell
py -3.12 scripts\verify_connections.py
```

## Important notes

- Never commit .env
- Never hard-code credentials in source files
- Use environment variables or a secrets manager in production
