SELECT EXISTS(SELECT 1 FROM score_configs WHERE config_id = {{config_id}} LIMIT 1)
