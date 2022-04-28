CREATE OR REPLACE VIEW demo_ikea.demo_ikea.ml_mart
AS
SELECT
    CONCAT(newsfeed_item ->> 'id', newsfeed_item ->> 'date')		AS item_id
  , TO_TIMESTAMP(CAST(newsfeed_item ->> 'date' AS bigint))::date	AS item_date
  , newsfeed_item ->> 'text'						AS item_text
  , regexp_replace(newsfeed_item ->> 'text',
  	'[^\U0001F300-\U0001F6FF]|^\u00a9|^\u00ae|[\u2000-\u3300]|^\ud83c[^\ud000-\udfff]|^\ud83d[^\ud000-\udfff]|^\ud83e[^\ud000-\udfff]', '', 'g') AS item_emoji
  , (newsfeed_item ->> 'likes')::jsonb ->> 'count'			AS item_likes
  , GREATEST(((newsfeed_item ->> 'views')::jsonb ->> 'count')::int, 0)	AS item_views
  , (newsfeed_item ->> 'reposts')::jsonb ->> 'count'			AS item_reposts
  , (newsfeed_item ->> 'comments')::jsonb ->> 'count'			AS item_comments
  , insert_timestamp
FROM
    demo_ikea.demo_ikea.newsfeed
