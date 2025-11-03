import psycopg2
import psycopg2.extras
from datetime import date, timedelta
from utilities.hupspot_api import HubSpotConnector

# Instantiate HubSpot connector
hubcon = HubSpotConnector()

# Define date window
today_minus_30 = date.today() - timedelta(days=32)

# Database connection
connection = psycopg2.connect(
    host="production-aurora.cluster-ro-cmqpdlmvonoj.eu-central-1.rds.amazonaws.com",
    database="data_science",
    user="mathis.mira",
    password="production-aurora.cluster-ro-cmqpdlmvonoj.eu-central-1.rds.amazonaws.com:5432/?Action=connect&DBUser=mathis.mira&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAV6F57T6EP7TFID5X%2F20251024%2Feu-central-1%2Frds-db%2Faws4_request&X-Amz-Date=20251024T110527Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=569e335ae145d673cc268fa37bbf21cc6d836ee3ccacae72ee09b66d13b4d8a0"
)

def fetch_kpis():
    """
    Fetches only the first and last KPI values (delta) per organization & KPI within last 30 days.
    This prevents summing cumulative snapshots.
    """
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        WITH kpi_window AS (
            SELECT 
                organization_id,
                organization_name,
                kpi,
                value,
                run_timestamp,
                ROW_NUMBER() OVER (PARTITION BY organization_id, kpi ORDER BY run_timestamp ASC) AS rn_asc,
                ROW_NUMBER() OVER (PARTITION BY organization_id, kpi ORDER BY run_timestamp DESC) AS rn_desc
            FROM business_review_kpis
            WHERE run_timestamp > %s
            AND accounting_year_parameter = EXTRACT(YEAR FROM NOW())
            AND kpi IN (
                'Number of log entries',
                'Number of log entries created via bulk import'
            )
        )
        SELECT
            organization_id,
            organization_name,
            kpi,
            MAX(CASE WHEN rn_asc = 1 THEN value END) AS first_value,
            MAX(CASE WHEN rn_desc = 1 THEN value END) AS last_value,
            (MAX(CASE WHEN rn_desc = 1 THEN value END) - MAX(CASE WHEN rn_asc = 1 THEN value END)) AS delta
        FROM kpi_window
        GROUP BY organization_id, organization_name, kpi;
    """, (today_minus_30,))

    
    result = cursor.fetchall()
    cursor.close()
    return result

def main():
    result = fetch_kpis()
    aggregated_data = {}

    # Process KPI deltas
    for row in result:
        org_id = row.get("organization_id")
        org_name = row.get("organization_name")
        kpi_name = row.get("kpi")
        kpi_delta = row.get("delta", 0)  # ✅ Use delta instead of raw value

        if org_id not in aggregated_data:
            aggregated_data[org_id] = {
                "organization_name": org_name,
                "kpis": {}
            }

        # Store KPI delta value directly
        aggregated_data[org_id]["kpis"][kpi_name] = kpi_delta

    # Push to HubSpot
    for org_id, data in aggregated_data.items():
        org_results = hubcon.search_company(filters={"organisation_id": org_id})
        results = org_results.get("results", [])

        if not results:
            print(f"No HubSpot company found for organisation_id {org_id}")
            continue

        hubspot_company_id = results[0].get("id")
        hubspot_company_name=results[0].get("properties",{}).get("name","")

        properties_to_update = {}
        for kpi_name, kpi_delta in data["kpis"].items():
            if kpi_name == "Number of log entries":
                properties_to_update["number_log_entries_past_30_days"] = kpi_delta
            elif kpi_name == "Number of log entries created via bulk import":
                properties_to_update["number_bulk_entries_past_30_days"] = kpi_delta

        if properties_to_update:
            hubcon.update_company_properties(hubspot_company_id, properties_to_update)
            print(f"Updated company {hubspot_company_id} {hubspot_company_name} with KPI delta values {properties_to_update}")
        else:
            print(f"No KPI values to update for organization {org_id}")

if __name__ == "__main__":
    main()
