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
    cursor.execute("SELECT id FROM county_year WHERE county_id = %s AND year = %s", (county_id, year))
    res = cursor.fetchone()
    return res[0] if res else None

def populate_weather():
    path = r"c:\Users\101jc\Desktop\Files\soybean\justin\weather_with_geo.csv"
    
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return

    conn = get_connection()
    cursor = conn.cursor()
    
    print("Fetching valid counties...")
    cursor.execute("SELECT geofips, name FROM county")
    valid_counties = {row[0] for row in cursor.fetchall()}
    
    # Create a lookup for name resolution: "County Name" -> [FIPS1, FIPS2]
    cursor.execute("SELECT geofips, name FROM county")
    name_to_fips = {}
    for fips, full_name in cursor.fetchall():
        # full_name is "County Name, State"
        if "," in full_name:
            county_part = full_name.split(",")[0].strip() # "Autauga County"
            if county_part not in name_to_fips:
                name_to_fips[county_part] = []
            name_to_fips[county_part].append(fips)

    print(f"Found {len(valid_counties)} valid counties.")
    
    with open(path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
    
    print(f"Populating weather from {path}...")
    
    batch_size = 1000
    batch = []

    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        processed = 0
        last_percent = -1
        
        for row in reader:
            processed += 1
            percent = int((processed / total_lines) * 100)
            if percent > last_percent:
                print(f"Progress: {percent}%")
                last_percent = percent
            
            raw_fips = (row.get("GeoFIPS") or "").strip()
            raw_name = (row.get("county") or "").strip()
            date_str = (row.get("date") or "").strip()
            
            if not date_str:
                continue

            try:
                year = int(date_str[:4])
            except ValueError:
                continue

            fips = None
            
            # 1. Try explicit FIPS
            if raw_fips and raw_fips.lower() != "nan":
                if "." in raw_fips:
                    raw_fips = raw_fips.split(".")[0]
                
                cand_fips = raw_fips.zfill(5)
                if cand_fips in valid_counties:
                    fips = cand_fips
            
            # 2. If no FIPS, try name resolution
            if not fips and raw_name:
                candidate_name = f"{raw_name} County"
                matches = name_to_fips.get(candidate_name, [])
                if len(matches) == 1:
                    fips = matches[0]
            
            if not fips:
                continue

            # Check if county_year exists
            cy_id = get_cy_id(cursor, fips, year)
            if not cy_id:
                continue
                
            try:
                precip = float(row["precip_mm"]) if row.get("precip_mm") else None
                tavg = float(row["tavg_C"]) if row.get("tavg_C") else None
                tmax = float(row["tmax_C"]) if row.get("tmax_C") else None
                tmin = float(row["tmin_C"]) if row.get("tmin_C") else None
            except ValueError:
                continue

            batch.append((cy_id, date_str, precip, tavg, tmax, tmin))

            if len(batch) >= batch_size:
                # Deduplicate batch
                unique_batch_map = {}
                for item in batch:
                    key = (item[0], item[1])
                    unique_batch_map[key] = item
                clean_batch = list(unique_batch_map.values())

                psycopg2.extras.execute_values(
                    cursor,
                    """
                    INSERT INTO weather (county_year_id, date, precip_mm, tavg_c, tmax_c, tmin_c)
                    VALUES %s
                    ON CONFLICT (county_year_id, date) 
                    DO UPDATE SET 
                        precip_mm = EXCLUDED.precip_mm,
                        tavg_c = EXCLUDED.tavg_c,
                        tmax_c = EXCLUDED.tmax_c,
                        tmin_c = EXCLUDED.tmin_c
                    """,
                    clean_batch
                )
                batch = []

        if batch:
             # Deduplicate batch
             unique_batch_map = {}
             for item in batch:
                 key = (item[0], item[1])
                 unique_batch_map[key] = item
             clean_batch = list(unique_batch_map.values())

             psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO weather (county_year_id, date, precip_mm, tavg_c, tmax_c, tmin_c)
                VALUES %s
                ON CONFLICT (county_year_id, date) 
                DO UPDATE SET 
                    precip_mm = EXCLUDED.precip_mm,
                    tavg_c = EXCLUDED.tavg_c,
                    tmax_c = EXCLUDED.tmax_c,
                    tmin_c = EXCLUDED.tmin_c
                """,
                clean_batch
            )

        conn.commit()
        conn.close()
        print("Done.")

if __name__ == "__main__":
    populate_weather()