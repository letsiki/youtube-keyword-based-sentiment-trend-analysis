CREATE TABLE IF NOT EXISTS text_from_audio (
    video_id TEXT PRIMARY KEY,
    text_field TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS comments (
        id SERIAL PRIMARY KEY,
    video_id TEXT NOT NULL,
    comments JSONB NOT NULL
);
