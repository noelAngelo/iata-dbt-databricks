select
    *,
    {{ generate_audit_columns() }}
from read_files('{{ var("DBT_INPUT_PATH") }}/iata_data/airport_codes.json')