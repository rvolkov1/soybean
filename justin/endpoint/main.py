from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse
import pandas as pd
import psycopg
import io
import os
from typing import Optional

app = FastAPI()

@app.get("/")
async def export(
    start: Optional[int] = Query(None, description="Start year (inclusive)"),
    end: Optional[int] = Query(None, description="End year (inclusive)"),
    growing_season: bool = Query(False, description="If true, filter weather data to growing season (May-Oct)")
):
    conn = psycopg.connect(os.getenv("DATABASE_URL"))

    # Define weather filtering logic
    # Growing season: May (5) to October (10)
    # Completeness check: ~6 months * 30 days = 180 days (using 150 as safe lower bound) vs 365 days (using 330 as safe lower bound)
    if growing_season:
        weather_condition = "EXTRACT(MONTH FROM date) BETWEEN 5 AND 10"
        min_months = 5 # Expect 6 months (May-Oct). Threshold set to 5.
    else:
        weather_condition = "TRUE"
        min_months = 11 # Expect 12 months (Full Year). Threshold set to 11.

    query = f"""
        WITH weather_stats AS (
            SELECT 
                county_year_id,
                SUM(precip_mm) as precip_mm_total,
                AVG(tavg_c) as tavg_c,
                COUNT(*) as day_count
            FROM weather
            WHERE {weather_condition}
            GROUP BY county_year_id
            HAVING COUNT(*) >= {min_months}
        )
        SELECT 
            c.geofips,
            c.name as county_name,
            c.state,
            cy.year,
            w.precip_mm_total,
            w.tavg_c,
            a.soybean_total_production,
            e.total_gdp
        FROM county_year cy
        JOIN county c ON cy.county_id = c.geofips
        JOIN agricultural a ON cy.id = a.county_year_id
        JOIN economy e ON cy.id = e.county_year_id
        JOIN weather_stats w ON cy.id = w.county_year_id
        WHERE 
            (%(start)s IS NULL OR cy.year >= CAST(%(start)s AS INTEGER))
            AND (%(end)s IS NULL OR cy.year <= CAST(%(end)s AS INTEGER))
    """

    # Execute query using pandas
    # Note: We use params to safely pass start/end arguments
    params = {"start": start, "end": end}
    df = pd.read_sql(query, conn, params=params)
    
    conn.close()

    # Convert DataFrame → CSV (streaming)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    filename = "soybean_data_export.csv"
    if growing_season:
        filename = "soybean_data_export_growing_season.csv"

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"'
    }

    return StreamingResponse(buffer, media_type="text/csv", headers=headers)


@app.get("/status")
def home():
    return {"status": "ok", "message": "CSV export server running"}
