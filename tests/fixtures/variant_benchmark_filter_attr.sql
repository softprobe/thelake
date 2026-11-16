SELECT COUNT(*) FROM {{TABLE}} WHERE ts >= '{{START}}' AND ts <= '{{END}}' AND CAST(attributes['sp.user.id'] AS VARCHAR) = 'user-42'
