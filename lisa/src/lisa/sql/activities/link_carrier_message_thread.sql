INSERT INTO lisa.activity_monitor_carrier_user_messages(carrier_message_id, activity_id)
VALUES (%(external_id)s, %(activity_id)s)
ON CONFLICT DO NOTHING;
