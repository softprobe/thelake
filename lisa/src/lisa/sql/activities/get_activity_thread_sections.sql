SELECT activity_id, section_id, title, status, section_order,
       section_depth, parent_section_key, ordinal_label
FROM lisa.activity_monitor_thread_sections
WHERE activity_id = %(activity_id)s
ORDER BY section_order;
