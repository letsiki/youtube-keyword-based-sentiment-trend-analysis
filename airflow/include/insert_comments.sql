INSERT INTO comments_table (video_id, author, comment, published_at)
VALUES (%s, %s, %s, %s)
ON CONFLICT (video_id, author, comment) DO NOTHING;;