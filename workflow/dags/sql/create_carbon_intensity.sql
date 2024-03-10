CREATE TABLE IF NOT EXISTS craverolucio_coderhouse.carbon_intensity (
    from_date VARCHAR(50) PRIMARY KEY,
    to_date VARCHAR(50) SORTKEY,
    intensity_max SMALLINT,
    intensity_average SMALLINT,
    intensity_min SMALLINT,
    intensity_index VARCHAR(50)
);