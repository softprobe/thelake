SELECT reference_type, reference_id
FROM lisa.readable_activity_monitor_links
WHERE activity_id = %(activity_id)s
ORDER BY reference_type, reference_id;
