import os
import psycopg2
from dotenv import load_dotenv
import csv

def ensure_schema(conn):
    print("Ensuring schema...")
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS county (id SERIAL PRIMARY KEY, geofips VARCHAR(10), name TEXT NOT NULL)")
        cur.execute("CREATE TABLE IF NOT EXISTS year (year INTEGER PRIMARY KEY)")
        cur.execute("CREATE TABLE IF NOT EXISTS county_year (id SERIAL PRIMARY KEY, county_id INTEGER REFERENCES county(id), year INTEGER REFERENCES year(year), UNIQUE(county_id, year))")
        cur.execute("CREATE TABLE IF NOT EXISTS agricultural (id SERIAL PRIMARY KEY, county_year_id INTEGER REFERENCES county_year(id) ON DELETE CASCADE, soybean_yield_bu_acre DOUBLE PRECISION, UNIQUE(county_year_id))")
        cur.execute("DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='agricultural' AND column_name='soybean_total_production') THEN EXECUTE 'ALTER TABLE agricultural RENAME COLUMN soybean_total_production TO soybean_yield_bu_acre'; END IF; END $$;")
        cur.execute("CREATE TABLE IF NOT EXISTS economy (id SERIAL PRIMARY KEY, county_year_id INTEGER REFERENCES county_year(id) ON DELETE CASCADE, total_gdp DOUBLE PRECISION, UNIQUE(county_year_id))")
        cur.execute("CREATE TABLE IF NOT EXISTS weather (id SERIAL PRIMARY KEY, county_year_id INTEGER REFERENCES county_year(id) ON DELETE CASCADE, date DATE NOT NULL, precip_mm DOUBLE PRECISION, tavg_c DOUBLE PRECISION, tmax_c DOUBLE PRECISION, tmin_c DOUBLE PRECISION, UNIQUE(county_year_id, date))")
    conn.commit()


def get_or_create_county(cur, geofips, name):
    print(f"get_or_create_county: {geofips}, {name}")
    cur.execute("SELECT id FROM county WHERE geofips=%s", (geofips,))
    r = cur.fetchone()
    if r:
        cid = r[0]
    else:
        cur.execute("INSERT INTO county(geofips,name) VALUES(%s,%s) RETURNING id", (geofips, name))
        cid = cur.fetchone()[0]
    return cid

def ensure_year(cur, y):
    print(f"ensure_year: {y}")
    cur.execute("INSERT INTO year(year) VALUES(%s) ON CONFLICT DO NOTHING", (y,))

def ensure_cy(cur, county_id, y):
    print(f"ensure_cy: {county_id}, {y}")
    cur.execute("INSERT INTO county_year(county_id,year) VALUES(%s,%s) ON CONFLICT (county_id,year) DO NOTHING RETURNING id", (county_id, y))
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("SELECT id FROM county_year WHERE county_id=%s AND year=%s", (county_id, y))
    return cur.fetchone()[0]
def load_soybeans(conn, path):
    print(f"load_soybeans: {path}")
    max_rows = 100
    inserted = 0

    with conn.cursor() as cur, open(path, newline="", encoding="utf-8") as f:
        rd = csv.reader(f)
        hdr = next(rd)
        years = hdr[2:]

        for row in rd:
            if inserted >= max_rows:
                break

            if not row or len(row) < 3: 
                continue

            geofips = row[0].strip()
            region = row[1].strip()
            if not geofips or geofips.lower() == "geofips": 
                continue

            cid = get_or_create_county(cur, geofips, region)

            for i, val in enumerate(row[2:]):
                if inserted >= max_rows:
                    break

                y = years[i]
                if not y or not val or val in ("", "(NA)", "NA"): 
                    continue

                try:
                    v = float(val)
                    yi = int(y)
                except:
                    continue

                ensure_year(cur, yi)
                cyid = ensure_cy(cur, cid, yi)

                cur.execute(
                    "INSERT INTO agricultural(county_year_id,soybean_yield_bu_acre) "
                    "VALUES(%s,%s) "
                    "ON CONFLICT (county_year_id) DO UPDATE SET soybean_yield_bu_acre=EXCLUDED.soybean_yield_bu_acre",
                    (cyid, v),
                )
                inserted += 1

    conn.commit()
    print(f"Inserted {inserted} soybean rows.")


def load_gdp(conn, path):
    print(f"load_gdp: {path}")
    max_rows = 100
    inserted = 0

    with conn.cursor() as cur, open(path, newline="", encoding="utf-8") as f:
        rd = csv.reader(f)
        hdr = next(rd)
        years = hdr[2:]

        for row in rd:
            if inserted >= max_rows:
                break

            if not row or len(row) < 3:
                continue

            geofips = row[0].strip()
            region = row[1].strip()
            if region in ("United States", ""): 
                continue
            if not any(s in region for s in ("County", "Parish", "Borough", "Census Area")):
                continue
            if not geofips or geofips == "0":
                continue

            cid = get_or_create_county(cur, geofips, region)

            for i, val in enumerate(row[2:]):
                if inserted >= max_rows:
                    break

                y = years[i]
                if not y or not val or val in ("", "(NA)", "NA", "(D)"):
                    continue

                try:
                    v = float(val)
                    yi = int(y)
                except:
                    continue

                ensure_year(cur, yi)
                cyid = ensure_cy(cur, cid, yi)

                cur.execute(
                    "INSERT INTO economy(county_year_id,total_gdp) "
                    "VALUES(%s,%s) "
                    "ON CONFLICT (county_year_id) DO UPDATE SET total_gdp=EXCLUDED.total_gdp",
                    (cyid, v),
                )
                inserted += 1

    conn.commit()
    print(f"Inserted {inserted} gdp rows.")


def load_weather(conn, path):
    print(f"load_weather: {path}")
    max_rows = 100
    inserted = 0

    with conn.cursor() as cur, open(path, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)

        for row in rd:
            if inserted >= max_rows:
                break

            g = (row.get("GeoFIPS") or "").strip()
            if not g or g.lower() == "nan": 
                continue

            name = (row.get("county") or "").strip()
            d = (row.get("date") or "").strip()
            if not d: 
                continue

            try:
                y = int(d[:4])
                p = row.get("precip_mm")
                ta = row.get("tavg_C")
                tx = row.get("tmax_C")
                tn = row.get("tmin_C")
                vals = [p, ta, tx, tn]
                nums = [float(v) if v not in (None, "", "NA") else None for v in vals]
            except:
                continue

            cid = get_or_create_county(cur, g, name)
            ensure_year(cur, y)
            cyid = ensure_cy(cur, cid, y)

            cur.execute(
                "INSERT INTO weather(county_year_id,date,precip_mm,tavg_c,tmax_c,tmin_c) "
                "VALUES(%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (county_year_id,date) DO UPDATE SET "
                "precip_mm=EXCLUDED.precip_mm, "
                "tavg_c=EXCLUDED.tavg_c, "
                "tmax_c=EXCLUDED.tmax_c, "
                "tmin_c=EXCLUDED.tmin_c",
                (cyid, d, nums[0], nums[1], nums[2], nums[3]),
            )

            inserted += 1

    conn.commit()
    print(f"Inserted {inserted} weather rows.")

    
def get_connection():
    print("get_connection")
    load_dotenv()
    url = os.getenv('DATABASE_URL')
    if not url:
        raise ValueError('DATABASE_URL not found. Make sure .env is loaded correctly.')
    return psycopg2.connect(url)

def main():
    conn = get_connection()
    ensure_schema(conn)
    load_soybeans(conn, r"c:\Users\101jc\Desktop\Files\Coding\soybean\chris\pivoted_soybeans.csv")
    load_gdp(conn, r"c:\Users\101jc\Desktop\Files\Coding\soybean\chris\total_gdp.csv")
    load_weather(conn, r"c:\Users\101jc\Desktop\Files\Coding\soybean\justin\weather_with_geo.csv")
    conn.close()

if __name__ == "__main__":
    main()