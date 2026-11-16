INSERT INTO bench_variant VALUES
    (1, TIMESTAMP '2026-01-01 00:00:00', '{"sp.user.id":"user-1","nested":{"empty_arr":[],"count":42,"ok":true}}'::JSON::VARIANT, '[]'::JSON::VARIANT, 'user-1'),
    (2, TIMESTAMP '2026-01-01 00:00:01', '[]'::JSON::VARIANT, '[{"name":"evt","attrs":{"k":"v"}}]'::JSON::VARIANT, 'user-2'),
    (3, TIMESTAMP '2026-01-01 00:00:02', '{}'::JSON::VARIANT, '[]'::JSON::VARIANT, 'user-3');
