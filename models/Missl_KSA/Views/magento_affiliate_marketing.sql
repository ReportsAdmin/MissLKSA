Select
date_updated_on_,
_sdc_table_version,
__sdc_sheet_id,
__sdc_row,
_sdc_received_at,
_sdc_sequence,
country__ksa_egypt_ as Country_KSA_Egypt,
_sdc_batched_at,
_sdc_extracted_at,
code,
__sdc_spreadsheet_id
from `noted-computing-279322.affiliate_codes_1.Affiliate_Marketing_Codes`
where lower(country__ksa_egypt_) = 'ksa' or lower(country__ksa_egypt_) = 'both'