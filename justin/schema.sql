
CREATE TABLE county (
    id SERIAL PRIMARY KEY,
    geofips VARCHAR(10),
    name TEXT NOT NULL
);

CREATE TABLE year (
    year INTEGER PRIMARY KEY
);

CREATE TABLE county_year (
    id SERIAL PRIMARY KEY,
    county_id INTEGER REFERENCES county(id),
    year INTEGER REFERENCES year(year),
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

