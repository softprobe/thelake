DELETE FROM __TABLE__
WHERE (state = 'complete' AND completed_at < now() - INTERVAL '30 days')
   OR (state = 'processing' AND lease_until < now() - INTERVAL '30 days');
