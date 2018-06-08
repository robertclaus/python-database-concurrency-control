class config:
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


# TATP
SUBSCRIBER_COUNT = 1000000
USE_NON_UNIFORM_RANDOM = True


# Generator
DEFAULT_TARGET_DEPTH = 2000
GENERATOR_BUNDLE_SIZE = 4
DEFAULT_GENERATOR_WORKER_COUNT = 10
