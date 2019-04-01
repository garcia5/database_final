# database_final

## Installation

This project requires `Postgres`, `python3.7`, and the `psycopg2-binary` package to be installed

## Setup instructions

Before running any commands, make sure you are

- In the directory of this README
- Logged in as a Postgres superuser

Then run:

```
$ psql < createdb.sql
$ python3.7 load_data.py
```