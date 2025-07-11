CREATE TABLE IF NOT EXISTS text_from_audio (
    id SERIAL PRIMARY KEY,
    text_field TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS comments (
    video_id TEXT PRIMARY KEY,
    comments JSONB NOT NULL
);
