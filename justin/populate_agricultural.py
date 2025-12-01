import os
import csv
import psycopg2
from dotenv import load_dotenv

def get_connection():
    load_dotenv()
    url = os.getenv('DATABASE_URL')
    if not url:
        raise ValueError('DATABASE_URL not found. Make sure .env is loaded correctly.')
    return psycopg2.connect(url)

def ensure_cy(cursor, county_id, year):
    cursor.execute("""
        INSERT INTO county_year (county_id, year) 
        VALUES (%s, %s) 
        ON CONFLICT (county_id, year) DO NOTHING 
        RETURNING id
    """, (county_id, year))
    res = cursor.fetchone()
    if res:
        return res[0]
    
    cursor.execute("SELECT id FROM county_year WHERE county_id = %s AND year = %s", (county_id, year))
    res = cursor.fetchone()
    return res[0] if res else None

def populate_agricultural():
    path = r"c:\Users\101jc\Desktop\Files\soybean\chris\pivoted_soybeans.csv"
    
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return

    conn = get_connection()
    cursor = conn.cursor()
    
    print("Fetching valid counties...")
    cursor.execute("SELECT geofips FROM county")
    valid_counties = {row[0] for row in cursor.fetchall()}
    print(f"Found {len(valid_counties)} valid counties.")

    with open(path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
    
    print(f"Populating agricultural from {path}...")
    
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return

        years = header[2:]
        
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
            
            geofips = row[0].strip().zfill(5)
            
            if geofips not in valid_counties:
                continue
                
            for i, val in enumerate(row[2:]):
                if i >= len(years): break
                
                year_str = years[i]
                if not val or val.strip() in ("", "(NA)", "NA"): 
                    continue
                
                try:
                    year_int = int(year_str)
                    val_float = float(val)
                except ValueError:
                    continue
                
                cy_id = ensure_cy(cursor, geofips, year_int)
                
                if cy_id:
                    # Note: Inserting into soybean_total_production as per schema.sql
                    cursor.execute("""
                        INSERT INTO agricultural (county_year_id, soybean_total_production)
                        VALUES (%s, %s)
                        ON CONFLICT (county_year_id) 
                        DO UPDATE SET soybean_total_production = EXCLUDED.soybean_total_production
                    """, (cy_id, val_float))
                
        conn.commit()
        conn.close()
        print("Done.")

if __name__ == "__main__":
    populate_agricultural()