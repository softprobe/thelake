SELECT EXISTS (
  SELECT 1 FROM {{physical_table}}
  WHERE workspace_id = ?
);
