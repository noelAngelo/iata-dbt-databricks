{% macro generate_audit_columns() %}
    _metadata.file_path as audit_file_path,
    _metadata.file_name as audit_file_name,
    _metadata.file_modification_time as audit_file_updated_at,
    current_timestamp() as audit_tgt_load_timestamp
{% endmacro %}