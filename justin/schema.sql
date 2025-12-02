
CREATE TABLE county (
    geofips VARCHAR(10) PRIMARY KEY,
    name TEXT NOT NULL,
    state TEXT NOT NULL
);

CREATE TABLE county_year (
    id SERIAL PRIMARY KEY,
    county_id VARCHAR(10) REFERENCES county(geofips),
    year INTEGER,
    UNIQUE(county_id, year)
);

CREATE TABLE weather (
    id SERIAL PRIMARY KEY,
    county_year_id INTEGER REFERENCES county_year(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    month INTEGER GENERATED ALWAYS AS (EXTRACT(MONTH FROM date)) STORED,
    precip_mm DOUBLE PRECISION,
    tavg_c DOUBLE PRECISION,
    tmax_c DOUBLE PRECISION,
    tmin_c DOUBLE PRECISION,
    UNIQUE(county_year_id, date)
);

CREATE TABLE agricultural (
    id SERIAL PRIMARY KEY,
    county_year_id INTEGER REFERENCES county_year(id) ON DELETE CASCADE,
    soybean_total_production DOUBLE PRECISION,
  
    UNIQUE(county_year_id)
);

CREATE TABLE economy (
    id SERIAL PRIMARY KEY,
    county_year_id INTEGER REFERENCES county_year(id) ON DELETE CASCADE,
    total_gdp DOUBLE PRECISION,
  
    UNIQUE(county_year_id)
);

CREATE OR REPLACE VIEW soybean_data_view AS
WITH weather_full AS (
    SELECT 
        county_year_id,
        SUM(precip_mm) as precip_mm_total,
        AVG(tavg_c) as tavg_c,
        COUNT(*) as day_count
    FROM weather
    GROUP BY county_year_id
),
weather_growing AS (
    SELECT 
        county_year_id,
        SUM(precip_mm) as precip_mm_total,
        AVG(tavg_c) as tavg_c,
        COUNT(*) as day_count
    FROM weather
    WHERE EXTRACT(MONTH FROM date) BETWEEN 5 AND 9
    GROUP BY county_year_id
)
SELECT 
    c.geofips,
    c.name as county_name,
    c.state,
    cy.year,
    a.soybean_total_production,
    e.total_gdp,
    wf.precip_mm_total as precip_full,
    wf.tavg_c as tavg_full,
    wf.day_count as days_full,
    wg.precip_mm_total as precip_growing,
    wg.tavg_c as tavg_growing,
    wg.day_count as days_growing
FROM county_year cy
JOIN county c ON cy.county_id = c.geofips
JOIN agricultural a ON cy.id = a.county_year_id
JOIN economy e ON cy.id = e.county_year_id
JOIN weather_full wf ON cy.id = wf.county_year_id
JOIN weather_growing wg ON cy.id = wg.county_year_id;

