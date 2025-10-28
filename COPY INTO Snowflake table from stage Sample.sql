COPY INTO songs
  FROM @sparkify_stage
  FILE_FORMAT = (TYPE = 'JSON' FIELD_DELIMITER = ',' STRIP_OUTER_ARRAY = TRUE)
  PATTERN='.*song_data.*\.json';



  -- Suppose we have a staging table log_staging with ts_col as NUMBER (ms)
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT 
  TO_TIMESTAMP_NTZ(ts_col/1000),
  DATE_PART('hour', TO_TIMESTAMP_NTZ(ts_col/1000)),
  DATE_PART('day', TO_TIMESTAMP_NTZ(ts_col/1000)),
  DATE_PART('week', TO_TIMESTAMP_NTZ(ts_col/1000)),
  DATE_PART('month', TO_TIMESTAMP_NTZ(ts_col/1000)),
  DATE_PART('year', TO_TIMESTAMP_NTZ(ts_col/1000)),
  DATE_PART('dow', TO_TIMESTAMP_NTZ(ts_col/1000))
FROM log_staging
WHERE page = 'NextSong';


MERGE INTO users AS tgt
USING (SELECT userId::INT AS userId, firstName, lastName, gender, level FROM log_staging WHERE userId IS NOT NULL) AS src
ON tgt.user_id = src.userId
WHEN MATCHED AND tgt.level <> src.level THEN
  UPDATE SET level = src.level, first_name = src.firstName, last_name = src.lastName
WHEN NOT MATCHED THEN
  INSERT (user_id, first_name, last_name, gender, level) VALUES (src.userId, src.firstName, src.lastName, src.gender, src.level);
