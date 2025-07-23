INSERT INTO netanyahu_plus_gaza_text_from_audio (video_id, title, uploader, "description", view_count, upload_date, text_field)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (video_id) 
DO UPDATE 
SET 
    title = EXCLUDED.title,
    uploader = EXCLUDED.uploader,
    "description" = EXCLUDED."description",
    view_count = EXCLUDED.view_count,
    upload_date = EXCLUDED.upload_date,
    text_field = EXCLUDED.text_field;