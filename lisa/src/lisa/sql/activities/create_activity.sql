INSERT INTO lisa.activities(id, kind, parent_id, status, title, source, context_path)
VALUES (%(id)s, %(kind)s, %(parent_id)s, 'queued', %(title)s, %(source)s, %(context_path)s);
