-- Bike Flows
CREATE TABLE bike_inflow (
	`time` TIMESTAMP(3), 
	`region_id` BIGINT, 
    WATERMARK FOR `time` AS `time`
)WITH (
    'connector' = 'kafka',  -- using kafka connector
    'topic' = 'bike_inflow',  -- kafka topic
    'scan.startup.mode' = 'earliest-offset',  -- reading from the beginning
    'properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker address
    'format' = 'json'  -- the data format is json
);


CREATE TABLE bike_outflow (
	`time` TIMESTAMP(3), 
	`region_id` BIGINT, 
    WATERMARK FOR `time` AS `time`
)WITH (
    'connector' = 'kafka',  -- using kafka connector
    'topic' = 'bike_outflow',  -- kafka topic
    'scan.startup.mode' = 'earliest-offset',  -- reading from the beginning
    'properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker address
    'format' = 'json'  -- the data format is json
);


CREATE TABLE bike_inflow_es (
    interval_start TIMESTAMP, 
	`region_id` BIGINT, 
    inflow BIGINT
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = 'https://demo0.es.asia-east1.gcp.elastic-cloud.com:9243',  -- elasticsearch address
    'username' = 'elastic',
    'password' = '7FIVOml6LcwBxoN6cptivrCp',
    'index' = 'bike_inflow_es'  -- elasticsearch index name, similar to database table name
);


INSERT INTO bike_inflow_es
(SELECT TUMBLE_START(`time`, INTERVAL '5' MINUTE) AS interval_start, 
  `region_id`, 
  COUNT(*) AS inflow 
FROM bike_inflow 
GROUP BY TUMBLE(`time`, INTERVAL '5' MINUTE), `region_id`);


CREATE TABLE bike_outflow_es (
    interval_start TIMESTAMP, 
	`region_id` BIGINT, 
    outflow BIGINT
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = 'https://demo0.es.asia-east1.gcp.elastic-cloud.com:9243',  -- elasticsearch address
    'username' = 'elastic',
    'password' = '7FIVOml6LcwBxoN6cptivrCp',
    'index' = 'bike_outflow_es'  -- elasticsearch index name, similar to database table name
);


INSERT INTO bike_outflow_es
(SELECT TUMBLE_START(`time`, INTERVAL '5' MINUTE) AS interval_start, 
  `region_id`, 
  COUNT(*) AS outflow 
FROM bike_outflow 
GROUP BY TUMBLE(`time`, INTERVAL '5' MINUTE), `region_id`);



-- Taxi Flows
CREATE TABLE taxi_inflow (
	`time` TIMESTAMP(3), 
	`region_id` BIGINT, 
    WATERMARK FOR `time` AS `time`
)WITH (
    'connector' = 'kafka',  -- using kafka connector
    'topic' = 'taxi_inflow',  -- kafka topic
    'scan.startup.mode' = 'earliest-offset',  -- reading from the beginning
    'properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker address
    'format' = 'json'  -- the data format is json
);


CREATE TABLE taxi_outflow (
	`time` TIMESTAMP(3), 
	`region_id` BIGINT, 
    WATERMARK FOR `time` AS `time`
)WITH (
    'connector' = 'kafka',  -- using kafka connector
    'topic' = 'taxi_outflow',  -- kafka topic
    'scan.startup.mode' = 'earliest-offset',  -- reading from the beginning
    'properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker address
    'format' = 'json'  -- the data format is json
);


CREATE TABLE taxi_inflow_es (
    interval_start TIMESTAMP, 
	`region_id` BIGINT, 
    inflow BIGINT
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = 'https://demo0-3885e4.kb.asia-east1.gcp.elastic-cloud.com:9243',  -- elasticsearch address
    'username' = 'elastic',
    'password' = 'EIwywZtLnUwUDqp8AerEhn8S',
    'index' = 'taxi_inflow_es'  -- elasticsearch index name, similar to database table name
);


INSERT INTO taxi_inflow_es
(SELECT TUMBLE_START(`time`, INTERVAL '5' MINUTE) AS interval_start, 
  `region_id`, 
  COUNT(*) AS inflow 
FROM taxi_inflow 
GROUP BY TUMBLE(`time`, INTERVAL '5' MINUTE), `region_id`);


CREATE TABLE taxi_outflow_es (
    interval_start TIMESTAMP, 
	`region_id` BIGINT, 
    outflow BIGINT
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = 'https://demo0-3885e4.kb.asia-east1.gcp.elastic-cloud.com:9243',  -- elasticsearch address
    'username' = 'elastic',
    'password' = 'EIwywZtLnUwUDqp8AerEhn8S',
    'index' = 'taxi_outflow_es'  -- elasticsearch index name, similar to database table name
);


INSERT INTO taxi_outflow_es
(SELECT TUMBLE_START(`time`, INTERVAL '5' MINUTE) AS interval_start, 
  `region_id`, 
  COUNT(*) AS outflow 
FROM taxi_outflow 
GROUP BY TUMBLE(`time`, INTERVAL '5' MINUTE), `region_id`);