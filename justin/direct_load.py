import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

with open("yourfile.csv", "r", encoding="utf-8") as f:
    cur.copy_expert("""
        COPY weather(date, precip_mm, tavg_c, tmax_c, tmin_c)
        FROM STDIN WITH (FORMAT csv, HEADER true)
    """, f)

conn.commit()
cur.close()
conn.close()
