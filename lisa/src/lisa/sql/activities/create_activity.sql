INSERT INTO lisa.activities(id, kind, status, title, source, context_path)
VALUES (%(id)s, %(kind)s, 'queued', %(title)s, %(source)s, %(context_path)s);
