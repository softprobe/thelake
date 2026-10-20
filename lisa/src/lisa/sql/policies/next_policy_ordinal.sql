SELECT COALESCE(
    MAX((regexp_match(id, '^P1[.]([1-9][0-9]*)(?:[.][1-9][0-9]*)?$'))[1]::bigint),
    0
) + 1 AS ordinal
FROM lisa.policies;
