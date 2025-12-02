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

    # Select columns based on growing season flag
    # Completeness check: ~6 months * 30 days = 180 days (using 150 as safe lower bound) vs 365 days (using 330 as safe lower bound)
    if growing_season:
        precip_col = "precip_growing"
        tavg_col = "tavg_growing"
    else:
        precip_col = "precip_full"
        tavg_col = "tavg_full"

    query = f"""
        SELECT 
            geofips,
            county_name,
            state,
            year,
            {precip_col} as precip_mm_total,
            {tavg_col} as tavg_c,
            soybean_total_production,
            total_gdp
        FROM soybean_data_view
        WHERE 
            (CAST(%(start)s AS INTEGER) IS NULL OR year >= CAST(%(start)s AS INTEGER))
            AND (CAST(%(end)s AS INTEGER) IS NULL OR year <= CAST(%(end)s AS INTEGER))
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
