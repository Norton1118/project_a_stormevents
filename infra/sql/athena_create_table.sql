CREATE EXTERNAL TABLE IF NOT EXISTS stormevents.events_parquet (
  event_id           string,
  state              string,
  event_type         string,
  episode_id         string,
  cz_name            string,
  begin_date_time    string,
  end_date_time      string,
  injuries_direct    int,
  deaths_direct      int,
  damage_property    string,
  magnitude          double,
  latitude           double,
  longitude          double
)
PARTITIONED BY (year int, event_type_part string)
STORED AS PARQUET
LOCATION 's3://stormevents-586639910153-us-east-2/data/stormevents/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');