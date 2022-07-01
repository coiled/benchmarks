#!/bin/bash

rm -f benchmark.db
rm -f alembic/versions/*
sqlite3 benchmark.sqlite3 "VACUUM;"
alembic revision --autogenerate -m "Initial fields" 
alembic upgrade head
