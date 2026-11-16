SELECT id, CAST(attributes AS JSON) AS attrs_json, CAST(events AS JSON) AS events_json FROM {{TABLE}} WHERE ts >= '{{START}}' AND ts <= '{{END}}' ORDER BY id LIMIT 100
