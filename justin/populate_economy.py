import os
import csv
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

def get_connection():
    load_dotenv()
    url = os.getenv('DATABASE_URL')
    if not url:
        raise ValueError('DATABASE_URL not found. Make sure .env is loaded correctly.')
    return psycopg2.connect(url)

def get_cy_id(cursor, county_id, year):
    # Only fetch existing ID, do not create new entry
    cursor.execute("SELECT id FROM county_year WHERE county_id = %s AND year = %s", (county_id, year))
    res = cursor.fetchone()
    return res[0] if res else None

def populate_economy():
    path = r"c:\Users\101jc\Desktop\Files\soybean\chris\total_gdp.csv"
    
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return

    conn = get_connection()
    cursor = conn.cursor()
    
    # Pre-fetch valid counties to avoid DB queries for every row
    print("Fetching valid counties...")
    cursor.execute("SELECT geofips FROM county")
    valid_counties = {row[0] for row in cursor.fetchall()}
    print(f"Found {len(valid_counties)} valid counties.")

    # Count total lines for progress
    with open(path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
    
    print(f"Populating economy from {path}...")
    
    batch_size = 1000
    batch = []

    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return

        years = header[2:] # Years start from 3rd column
        
        processed = 0
        last_percent = -1
        
        for row in reader:
            processed += 1
            percent = int((processed / total_lines) * 100)
            if percent > last_percent:
                print(f"Progress: {percent}%")
                last_percent = percent
            
            if not row or len(row) < 3:
                continue
            
            # Standardize FIPS to 5 digits (e.g. '1001' -> '01001')
            geofips = row[0].strip().zfill(5)
            
            # Skip if county does not exist in DB
            if geofips not in valid_counties:
                continue
                
            # Iterate through year columns
            for i, val in enumerate(row[2:]):
                if i >= len(years): break
                
                year_str = years[i]
                if not val or val.strip() in ("", "(NA)", "NA", "(D)"): 
                    continue
                
                try:
                    year_int = int(year_str)
                    val_float = float(val)
                except ValueError:
                    continue
                
                cy_id = get_cy_id(cursor, geofips, year_int)
                
                if cy_id:
                    batch.append((cy_id, val_float))

                    if len(batch) >= batch_size:
                        psycopg2.extras.execute_values(
                            cursor,
                            """
                            INSERT INTO economy (county_year_id, total_gdp)
                            VALUES %s
                            ON CONFLICT (county_year_id) 
                            DO UPDATE SET total_gdp = EXCLUDED.total_gdp
                            """,
                            batch
                        )
                        batch = []
        
        # Insert remaining
        if batch:
             psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO economy (county_year_id, total_gdp)
                VALUES %s
                ON CONFLICT (county_year_id) 
                DO UPDATE SET total_gdp = EXCLUDED.total_gdp
                """,
                batch
            )

        conn.commit()
        conn.close()
        print("Done.")

if __name__ == "__main__":
    populate_economy()