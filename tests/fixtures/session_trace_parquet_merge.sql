SET preserve_insertion_order = false;
CALL ducklake_flush_inlined_data('softprobe', schema_name => '{{schema}}', table_name => 'traces');
CALL ducklake_merge_adjacent_files('softprobe', 'traces', schema => '{{schema}}');
