SELECT payload_hash
FROM lisa.session_evaluation_receipts
WHERE session_id = %(session_id)s;
