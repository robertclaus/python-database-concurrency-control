# Python Database Concurrency Control

## Project Description
This project is a python query pre-processing engine to allow running database queries at a lower isolation level in the database while retaining serializable level isolation.

## Project Architecture
The project is broken into a few main pieces:

1. QueryFlowTester - This is the main driver for the project and starts up the necessary threads as well as managing the ConcurrencyEngine and ending the test once enough time or queries have passed.
2. ConcurrencyEngine - Manages taking in queries and passing them to Clients only when they are gauranteed to execute serializably.
3. Query - Takes an SQL query and parses it into predicates describing what data sets the query could affect in the database.  Also contains the necessary methods for seeing if any of those data sets could conflict with another query.
4. QueryGenerator - Generates queries from a template (QuerySets) and queues them up for the ConcurrencyEngine.  Could be replaced with an incoming SQL feed from an actual application.
5. ClientManager - Starts database client threads that pass the SQL query to the database when the ConcurrencyEngine says it is safe.
6. QuerySets - A list of queries with wildcards that the QueryGenerator uses.

See PCC.pdf for a summary of how these pieces interact and what Queues they depend on.

## Running the Project
Run the project by calling dbQueryFlowTester with appropriate parameters:

1. Run Concurrency Control - "1" for running concurrency control in Python.  Any other value will generate queries but not parse or serialize them.  This is primarily used for comparing the external concurrency control to standard serialization isolation levels in the database.
2. Seconds to Run - Number of seconds to generate and run queries for.
3. Worker Number - Number of Client threads to start.
4. Max Queries - A maximum number of queries to run.  Useful for initially populating a database for testing.
5. All further parameters are a list of QuerySet numbers that should be used.

Example:
python dbQueryFlowTester.py 1 10 5 100000 0 1
Run the test with concurrency for 10 seconds with 5 workers.  Cut off the test at 100000 queries.  Only generate query set 0 and 1 for this test.

## QuerySet Behavior
QuerySets are defined in dbQuerySets.py.
Wildcard functions are defined in dbQueryGenerator.py and more can be added.  Note that each wildcard is only generated once per query, so re-using a single wildcard will give the same value for that query.

The first QuerySet given to dbQueryFlowTester will generate queries as quickly as possible.  All other QuerySet's given as a parameter will behave as an Application that sends one query and then waits for the response before sending the next.
