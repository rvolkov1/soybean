import os
import csv
import urllib.request
import io
import psycopg2
from dotenv import load_dotenv

STATE_ABBR_TO_NAME = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming"
}

def get_connection():
    load_dotenv()
    url = os.getenv('DATABASE_URL')
    if not url:
        raise ValueError('DATABASE_URL not found. Make sure .env is loaded correctly.')
    return psycopg2.connect(url)

def populate_counties():
    # Using a reliable GitHub source for FIPS codes
    url = "https://raw.githubusercontent.com/kjhealy/fips-codes/master/state_and_county_fips_master.csv"
    print(f"Downloading FIPS data from {url}...")
    
    try:
        with urllib.request.urlopen(url) as response:
            content = response.read().decode('utf-8')
    except Exception as e:
        print(f"Failed to download FIPS data: {e}")
        return

    # Parse CSV
    f = io.StringIO(content)
    reader = csv.DictReader(f)
    
    conn = get_connection()
    cursor = conn.cursor()
    
    print("Populating county table...")
    
    inserted = 0
    updated = 0
    
    try:
        for row in reader:
            abbr = row['state']
            if not abbr or abbr == 'NA':
                continue

            # Convert AL → Alabama
            state = STATE_ABBR_TO_NAME.get(abbr, abbr)

            fips = row['fips'].zfill(5)
            name = row['name']

            cursor.execute("""
                INSERT INTO county (geofips, name, state) 
                VALUES (%s, %s, %s)
                ON CONFLICT (geofips) 
                DO UPDATE SET name = EXCLUDED.name, state = EXCLUDED.state
                RETURNING (xmax = 0) AS inserted
            """, (fips, name, state))
            
            result = cursor.fetchone()
            if result and result[0]:
                inserted += 1
            else:
                updated += 1

            print(f"Processed: {fips} - {name}, {state}")
                
        conn.commit()
        print(f"Finished. Inserted: {inserted}, Updated: {updated}")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    populate_counties()