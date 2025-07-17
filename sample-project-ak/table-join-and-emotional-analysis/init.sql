CREATE TABLE IF NOT EXISTS text_from_audio (
    video_id TEXT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    uploader VARCHAR(128) NOT NULL,
    "description" TEXT NOT NULL,
    view_count INTEGER NOT NULL,
    upload_date DATE NOT NULL,
    text_field TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS comments (
    id SERIAL PRIMARY KEY,
    video_id TEXT NOT NULL,
    inserted_at DATE NOT NULL,
    author TEXT NOT NULL,
    comment TEXT NOT NULL,
    published_at TEXT NOT NULL
);

-- Dummy data for text_from_audio
INSERT INTO text_from_audio (video_id, title, uploader, description, view_count, upload_date, text_field)
VALUES
('vid1', 'First Video', 'user1', 'desc 1', 1000, '2024-01-01', 'some audio text'),
('vid2', 'Second Video', 'user2', 'desc 2', 2000, '2024-02-01', 'other audio text'),
('vid3', 'Third Video', 'user3', 'desc 3', 3500, '2024-02-15', 'audio content for third video'),
('vid4', 'Fourth Video', 'user4', 'desc 4', 500, '2024-03-01', 'this is the fourth audio'),
('vid5', 'Fifth Video', 'user5', 'desc 5', 8000, '2024-04-01', 'fifth video audio transcript');

-- Dummy data for comments
INSERT INTO comments (video_id, inserted_at, author, comment, published_at)
VALUES
('vid1', '2024-03-01', 'Alice', 'I absolutely loved this video!', '2024-03-01T10:00:00Z'),
('vid1', '2024-03-01', 'Frank', 'Great job on this video!', '2024-03-01T11:00:00Z'),
('vid2', '2024-03-02', 'Bob', 'This was so boring and terrible.', '2024-03-02T11:00:00Z'),
('vid3', '2024-03-03', 'Charlie', 'Interesting points, but I felt it was a bit long.', '2024-03-03T12:00:00Z'),
('vid3', '2024-03-03', 'Grace', 'Some good insights, but overall confusing.', '2024-03-03T13:00:00Z'),
('vid4', '2024-03-04', 'Dana', 'What a surprise, I actually liked it!', '2024-03-04T13:00:00Z'),
('vid5', '2024-03-05', 'Eve', 'I was scared it would be bad, but it was amazing!', '2024-03-05T14:00:00Z'),
('vid5', '2024-03-05', 'Henry', 'This made me so happy!', '2024-03-05T15:00:00Z');
