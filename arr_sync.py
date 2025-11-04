from utilities.gsheet_api import GSheetConnector
from utilities.hubspot_api import HubSpotConnector
from datetime import datetime
import logging

# Logging setup 
logging.basicConfig(
    level=logging.INFO,  
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

spreadsheet_id = "1m1NTcjYmSIks7qInkUfw3_SHBqoob-Ix1mEqCwglwwI"
range_names = ["ARR HS Export!A:D"] 
columns_to_clear = "A2:AG"
sheet_name_clear = "LineItem Import"
first_col = "A"
last_col = "AG"
BATCH_SIZE = 200

# HubSpot setup
stage_ids = ["1018520978", "8913715", "50301000", "28032678"]

def main():
    # Initialize connectors
    hubspot = HubSpotConnector()
    gsheet = GSheetConnector()

    

    # Search for active deals in HubSpot
    active_deals = hubspot.search_deals_stage_id(stage_ids)
    if not active_deals or "results" not in active_deals:
        logger.warning("No active deals found or failed to retrieve deals.")
        return

    logger.info(f"Found {len(active_deals.get('results', []))} active deals.")

    # Start row index for GSheet updates and collect batch payload
    row_index = 2
    sheet_updates = []

    # Iterate through deals one at a time
    for deal in active_deals.get("results", []):
        deal_id = deal.get("id")
        deal_props = deal.get("properties", {})

        # Get line items for this deal
        assoc_data = hubspot.get_associations("deals", deal_id, "line_items")
        if assoc_data is None:
            logger.error("Job aborted due to association retrieval failure for deal %s.", deal_id)
            return

        assoc_results = assoc_data.get("results")
        if assoc_results is None:
            logger.error("Job aborted due to malformed association payload for deal %s.", deal_id)
            return

        if not assoc_results:
            logger.warning("No line items associated with deal %s; continuing to next deal.", deal_id)
            continue

        # Process each line item for this deal
        for assoc in assoc_results:
            line_item_id = assoc.get("id")
            if not line_item_id:
                continue

            # Get line item properties
            line_item_data = hubspot.get_line_item_by_id(line_item_id,)
            if not line_item_data or "properties" not in line_item_data:
                logger.error(f"Job aborted due to line item retrieval failure for line item ID: {line_item_id}")
                return

            li_props = line_item_data.get("properties", {})

            # Prepare row values
            row_values = [
                deal_props.get("company_name", ""),                       # Column A
                deal_props.get("dealname", ""),                           # Column B
                li_props.get("name", ""),                                 # Column C
                deal_props.get("icp_sync", ""),                           # Column D
                deal_props.get("date_entered_upcoming_churn_sync", ""),   # Column E
                deal_props.get("cs_active_sync", ""),                     # Column F
                li_props.get("quantity", ""),                             # Column G
                li_props.get("discount", ""),                             # Column H
                li_props.get("recurringbillingfrequency", ""),            # Column I
                li_props.get("hs_recurring_billing_period", ""),          # Column J
                li_props.get("hs_recurring_billing_terms", ""),           # Column K
                li_props.get("hs_billing_start_delay_type", ""),          # Column L
                li_props.get("hs_recurring_billing_start_date", ""),      # Column M
                li_props.get("hs_post_tax_amount", ""),                   # Column N
                deal_props.get("client_cancellation_period_deals", ""),   # Column O
                deal_props.get("hs_object_id", ""),                       # Column P
                deal_props.get("dealtype", ""),                           # Column Q
                deal_props.get("contract_start_date", ""),                # Column R
                deal_props.get("contract_end_date", ""),                  # Column S
                deal_props.get("contract_length", ""),                    # Column T
                deal_props.get("contract_renewal_date_deals", ""),        # Column U
                deal_props.get("hs_is_closed_won", ""),                   # Column V
                deal_props.get("hs_is_closed", ""),                       # Column W
                deal_props.get("deal_currency_code", ""),                 # Column X
                deal_props.get("closedate", ""),                          # Column Y
                deal_props.get("dealstage", ""),                          # Column Z
                deal_props.get("pipeline", ""),                           # Column AA
                deal_props.get("lifecycle_stage", ""),                    # Column AB
                deal_props.get("hs_v2_date_entered_28032678", ""),        # Column AC
                deal_props.get("admin___ready_for_deletions___2506", ""), # Column AD
                deal_props.get("company_id", ""),                         # Column AE
                li_props.get("hs_object_id", ""),                         # Column AF
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")              # Column AG
            ]

            range_to_update = f"{sheet_name_clear}!{first_col}{row_index}:{last_col}{row_index}"
            sheet_updates.append(
                {
                    "range": range_to_update,
                    "values": [row_values],
                }
            )
            row_index += 1

    if sheet_updates:
        # Clear line item import sheet
        gsheet.clear_selected_columns(spreadsheet_id, sheet_name_clear, columns_to_clear)
        total_rows = len(sheet_updates)
        logger.info("Prepared %d rows for Google Sheets update.", total_rows)

        for start in range(0, total_rows, BATCH_SIZE):
            chunk = sheet_updates[start : start + BATCH_SIZE]
            if not gsheet.batch_update_values(spreadsheet_id, chunk):
                first_row = start + 2  # Sheet starts at row 2
                last_row = first_row + len(chunk) - 1
                logger.error(
                    "Stopping sync after failing to update rows %d-%d in Google Sheets.",
                    first_row,
                    last_row,
                )
                return

        logger.info("Successfully wrote %d rows to Google Sheets.", total_rows)
    else:
        logger.info("No line items to write to Google Sheets.")

    # Update ARR in HubSpot from new GSheet values
    company_list = gsheet.batch_get_values(spreadsheet_id, range_names)
    value_ranges = company_list.get("valueRanges", [])
    if not value_ranges or not value_ranges[0].get("values"):
        logger.warning("No company ARR data found in GSheet.")
        return

    updated_count = 0
    for row in value_ranges[0].get("values", [])[1:]:
        if len(row) < 4:
            continue
        company_id, arr_booked, nrr, weighted_nrr = row[0], row[1], row[2], row[3]
        if not company_id:
            logger.warning("Skipping HubSpot update due to missing company ID.")
            continue

        properties = {
            "current_booked_arr": arr_booked,
            "current_nrr__mom_": nrr,
            "total_current_nrr__mom____weighted": weighted_nrr,
        }
        result = hubspot.update_company_properties(str(company_id), properties)
        if result:
            updated_count += 1
        else:
            logger.error("Aborting company updates due to failure updating company ID %s.", company_id)
            return

    if updated_count:
        logger.info("Updated %d companies in HubSpot.", updated_count)
    else:
        logger.info("No HubSpot company updates to apply.") 


if __name__ == "__main__":
    main()
