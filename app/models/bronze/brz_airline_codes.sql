select
    *,
    {{ generate_audit_columns() }}
from read_files('{{ var("DBT_INPUT_PATH") }}/iata_data/airline_codes.json')