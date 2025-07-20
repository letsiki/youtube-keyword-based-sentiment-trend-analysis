CREATE TABLE IF NOT EXISTS text_from_audio (
    video_id TEXT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    uploader VARCHAR(128) NOT NULL,
    "description" TEXT NOT NULL,
    view_count INTEGER NOT NULL,
    upload_date DATE NOT NULL,
    text_field TEXT NOT NULL

);

CREATE TABLE IF NOT EXISTS comments_table (
    id SERIAL PRIMARY KEY,
    video_id TEXT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    author VARCHAR(128) NOT NULL,
    comment TEXT NOT NULL,
    published_at VARCHAR(128) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS unique_comment ON comments_table (video_id, author, comment);
