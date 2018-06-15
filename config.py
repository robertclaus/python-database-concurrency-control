
# Client Configuration
DB_TYPE_MYSQL = 1
DB_TYPE_POSTGRES = 2

DB_TYPE = 1

# MySQL Configuration
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'test'
MYSQL_DB_NAME = 'mydb' #t for standard, mydb for tatp

# Postgres Configuration
POSTGRES_HOST = '127.0.0.1'
POSTGRES_PORT = '5432'
POSTGRES_USER = 'robertclaus'
POSTGRES_PASSWORD = 'test'
POSTGRES_DB_NAME = 't'

# Engine Configuration
MAX_QUERIES_IN_ENGINE = 8000
NUMBER_OF_DATABASE_CLIENTS = 4
MAX_QUERIES_TO_RUN = 10000
MAX_SECONDS_TO_RUN = 10
CLIENT_BUNDLE_SIZE = 3

# TATP
SUBSCRIBER_COUNT = 5000000
USE_NON_UNIFORM_RANDOM = True

# Generator
DEFAULT_TARGET_DEPTH = 10000
GENERATOR_BUNDLE_SIZE = 100
DEFAULT_GENERATOR_WORKER_COUNT = 10
MAX_GENERATORS = 30

# Phased Policy
# Minimum queries to admit for a column key within a phase (will always admit at least once before checking this)
MIN_QUERIES_TO_ADMIT = 500
MIN_QUERIES_TO_ADMIT_READONLY = 5000
# Maximum queries to run in one phase.  Note MAX_ENGINES_IN_ENGINE may restrict this anyways.
MAX_QUERIES_PER_PHASE = 8000
# Number of queries to try and admit at once before checking if we should move onto a different phase.
QUERIES_TO_ADMIT_AT_TIME = 150
