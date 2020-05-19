# py-qe

This python module provides limited functionality for interacting with a Quantum
Engine server.

## Installation

Run `pip install -e .`.

## Setup Postgres database

Use a tool such as [Postico](https://eggerapps.at/postico/) to setup a postgres
server, and create a database on your server.

## Configuring your SQL connection

Use the `qe-sql set-config` command to set your SQL connection.
See `qe-sql set-config --help` for details.

## Uploading a workflowresult to SQL connection

Use the `qe-sql upload` command to upload a workflowresult JSON file to the
postgres database.
See `qe-sql upload --help` for details.
