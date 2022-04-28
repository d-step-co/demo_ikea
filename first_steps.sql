CREATE DATABASE demo_ikea;
CREATE SCHEMA demo_ikea;


CREATE TABLE demo_ikea.demo_ikea.newsfeed 
(
	newsfeed_date		BIGINT
  , newsfeed_item		JSONB
  , insert_timestamp	TIMESTAMP
)