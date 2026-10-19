INSERT INTO lisa.activity_monitor_thread_sections
    (activity_id, section_id, title, status, section_order)
VALUES (%(activity_id)s, %(section_id)s, %(title)s, %(status)s, %(section_order)s)
ON CONFLICT (activity_id, section_id) DO UPDATE SET
    title = EXCLUDED.title, status = EXCLUDED.status,
    section_order = EXCLUDED.section_order, updated_at = now();
