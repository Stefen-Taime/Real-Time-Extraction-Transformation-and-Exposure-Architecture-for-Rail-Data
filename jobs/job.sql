CREATE TABLE train_passages (
    line_id STRING,
    station_id STRING,
    direction STRING,
    passage_time STRING,
    passage_timeid STRING PRIMARY KEY NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '172.20.0.12',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'metro',
    'table-name' = 'train_passages',
    'server-time-zone' = '+01:00'
);


CREATE TABLE metro_stations (
    station_id STRING PRIMARY KEY NOT ENFORCED,
    station_name STRING,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '172.20.0.12',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'metro',
    'table-name' = 'metro_stations',
    'server-time-zone' = '+01:00'
);

CREATE VIEW combined_data AS
SELECT 
    tp.line_id, 
    tp.station_id, 
    tp.direction, 
    tp.passage_time, 
    tp.passage_timeid,
    ms.station_name, 
    ms.latitude, 
    ms.longitude
FROM train_passages tp
JOIN metro_stations ms ON tp.station_id = ms.station_id;



CREATE TABLE kafka_updates (
    identifier STRING,
    delay_minutes INT,
    message STRING,
    update_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' 
) WITH (
  'connector' = 'kafka',
  'topic' = 'update_line',
  'properties.bootstrap.servers' = '172.20.0.6:29092',
  'properties.group.id' = 'group.update_lines',
  'format' = 'avro-confluent',  
  'scan.startup.mode' = 'earliest-offset',
  'avro-confluent.schema-registry.url' = 'http://172.20.0.7:8081' 
);


CREATE VIEW latest_delay_view AS
SELECT 
    identifier,
    delay_minutes AS latest_delay_minutes
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY identifier 
               ORDER BY update_timestamp DESC
           ) as row_num
    FROM kafka_updates
) 
WHERE row_num = 1;

CREATE TABLE enriched_update (
    identifier STRING,
    latest_delay_minutes INT,
    direction STRING,
    passage_time BIGINT,
    passage_timeid STRING,
    station_name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    PRIMARY KEY (identifier) NOT ENFORCED
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://172.20.0.13:9200',
    'index' = 'enriched_update',
    'document-id.key-delimiter' = ''
);


INSERT INTO enriched_update
SELECT 
    k.identifier,
    k.latest_delay_minutes,
    c.direction,
    CAST(c.passage_time AS BIGINT), 
    c.passage_timeid,
    c.station_name,
    c.latitude,
    c.longitude
FROM (
    SELECT 
        identifier,
        delay_minutes AS latest_delay_minutes,
        update_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY identifier 
            ORDER BY update_timestamp DESC
        ) as row_num
    FROM kafka_updates
) k
JOIN combined_data c ON k.identifier = c.passage_timeid
WHERE k.row_num = 1;
