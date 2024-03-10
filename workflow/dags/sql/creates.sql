CREATE TABLE IF NOT EXISTS measurements_stg(
  station_number VARCHAR(255),
  value DECIMAL(10, 2),
  timestamp_measured VARCHAR(255),
  formula VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS measurements (
  station_number VARCHAR(255),
  value DECIMAL(10, 2),
  timestamp_measured VARCHAR(255),
  formula VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS stations_stg(
  station VARCHAR(255),
  type VARCHAR(255),
  municipality VARCHAR(255),
  url VARCHAR(255),
  province VARCHAR(255),
  organisation VARCHAR(255),
  location VARCHAR(255),
  year_start NVARCHAR(255)
);
CREATE TABLE IF NOT EXISTS stations(
  station VARCHAR(255),
  type VARCHAR(255),
  municipality VARCHAR(255),
  url VARCHAR(255),
  province VARCHAR(255),
  organisation VARCHAR(255),
  location VARCHAR(255),
  year_start NVARCHAR(255)
);