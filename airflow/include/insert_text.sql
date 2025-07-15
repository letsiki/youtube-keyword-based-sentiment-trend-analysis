INSERT INTO text_from_audio (video_id, text_field)
VALUES (%s, %s)
ON CONFLICT (video_id) DO NOTHING;