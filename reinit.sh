#!/bin/bash

rm -f benchmark.db
rm -f alembic/versions/*.py
sqlite3 benchmark.db "VACUUM;"
alembic revision --autogenerate -m "Initial fields" 
alembic upgrade head
