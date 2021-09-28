# Database Auditing and parsing Using Cockroachdb

## This project to implement a solution of auditing database challenge.

## Tools: 
### Apache Spark
### PostgreSQL
### Cockroach
### Regex

## The project hase the following steps:
  * Install a database solution called CockroachDB on your machine and get it into a running state. 
    Note: follow the link: https://www.cockroachlabs.com/docs/v20.1/install-cockroachdb-windows
  * Connect and create a table and enable auditing on that table as shown in task.py code.
    Note: Once the audit configuration is set, you’ll be able to find a history of CockroachDB SQL commands
    you run against the table.
  * Find the audit data. Then find and collect the audit data related to both commands
    you ran in part 3.
  * Write a python code (see file Parse_audit.py) that create a regular expresions that parses all relevant data from the audit data
    you collected and save the in csv file as shown in cockroach-sql-audit.FUJITSU-PC.FUJITSU-PC_Baha.2021-07-02T22_23_55Z.017400 (2).log.csv. 
