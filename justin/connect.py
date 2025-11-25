import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Load the environment variable
database_url = os.getenv('DATABASE_URL')

if not database_url:
    raise ValueError("DATABASE_URL not found. Make sure .env is loaded correctly.")

# Connect to the PostgreSQL database
conn = psycopg2.connect(database_url)

with conn.cursor() as cur:
    cur.execute("SELECT version()")
    print(cur.fetchone())

# Close the connection
conn.close()