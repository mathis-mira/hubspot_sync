from datetime import datetime, timedelta
from typing import Dict, Optional, Set, Union

from dotenv import load_dotenv

from utilities.hupspot_api import HubSpotConnector
from utilities.mixpanel_api import MixpanelConnector


load_dotenv()

hubcon = HubSpotConnector()
mixpanel = MixpanelConnector()


# time window
DELTA=90 # DELTA days ago
START_DATE = (datetime.now() - timedelta(days=DELTA)).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")


MIXPANEL_EVENT_TO_HUBSPOT_PROPERTY: Dict[str, Union[str, Dict[str, str]]] = {
    # mixpanel event name -> hubSpot property name (extend as needed)
    "log-entry - create": "manual_log_entries_past_90_days",
    "api-call": "api_calls_past_90_days",
    "bulk-import - ok": "bulk_imports_requested_past_90_days",
    "products - ok": "bulk_imports_uploaded_past_90_days",
    "Session start": "user_sessions_past_90_days",
    # URL keyword -> hubSpot property for page views (extend as needed)
    "page-view": {
        "dashboard": "dashboard_views_past_90_days",
    },
}
def main() -> None:
    event_names = list(MIXPANEL_EVENT_TO_HUBSPOT_PROPERTY)
    org_ids = mixpanel.get_property_values("organization_id")
    
    #initialize dict with all orgs to update by cozero id with all events to be tracked

    aggregated: Dict[str, Dict[str, Dict[str, object]]] = {
        event: {
            org_id: {
                "urls": [],
                "count": 0,
            }
            for org_id in org_ids
        }
        for event in event_names
    }
    
    #initialize dict to filter out duplicates 

    seen_insert_ids: Dict[str, Set[str]] = {event: set() for event in event_names}
    
    #get event occurences and count them

    event_stream = mixpanel.export_events(event_names, start_date=START_DATE, end_date=END_DATE)

    for event in event_stream:
        event_name = event.get("event")
        if event_name not in aggregated:
            continue

        properties = event.get("properties") or {}
        insert_id = properties.get("$insert_id")
        if insert_id:
            if insert_id in seen_insert_ids[event_name]:
                continue
            seen_insert_ids[event_name].add(insert_id)


        cozero_org_id = properties.get("organization_id")
        if cozero_org_id in (None, "", "UNKNOWN"):
            continue

        org_id_str = str(cozero_org_id)
        
        #add org if not already in the initialized dict (required to get all organizations)
        entry = aggregated[event_name].setdefault(
            org_id_str,
            {
                "urls": [],
                "count": 0,
            },
        )

        

        url = properties.get("url")
        if url:
            entry.setdefault("urls", []).append(url)
            
        entry["count"] = int(entry.get("count", 0)) + 1


    hubspot_cache: Dict[str, Optional[str]] = {}

    for event_name, mapping in MIXPANEL_EVENT_TO_HUBSPOT_PROPERTY.items():
        entries = aggregated.get(event_name, {})

        for org_id, entry in entries.items():
            #if event is not page-view
            if isinstance(mapping, str):
                property_updates = {mapping: entry.get("count", 0)}
            
            #if event is page-view we need to only count urls with the corresponding keyword in the mapping
            else:
                urls = entry.get("urls", [])
                property_updates = {
                    hubspot_property: sum(1 for url in urls if keyword in url)
                    for keyword, hubspot_property in mapping.items()
                }
                
                
            #hubspot upload

            if org_id in hubspot_cache:
                hubspot_org_id = hubspot_cache[org_id]
            else:
                try:
                    resp = hubcon.search_company({"organisation_id": org_id}, {}, 1)
                except Exception as exc:
                    print(f"HubSpot search failed for organisation_id {org_id}: {exc}")
                    hubspot_cache[org_id] = None
                    continue

                results = resp.get("results", []) if resp else []
                hubspot_org_id = results[0].get("id") if results else None
                if not hubspot_org_id:
                    print(f"No HubSpot company found for organisation_id {org_id}")
                hubspot_cache[org_id] = hubspot_org_id

            if not hubspot_org_id:
                continue

            try:
                hubcon.update_company_properties(
                    hubspot_org_id, property_updates
                )
                printable = ", ".join(
                    f"{prop}={value}" for prop, value in property_updates.items()
                )
                print(f"Company cozero id:{org_id} updated: {printable}")
            except Exception as exc:
                print(f"Failed to update HubSpot company {hubspot_org_id}: {exc}")
                continue
    
    companies_updated = sum(1 for company_id in hubspot_cache.values() if company_id)
    print(f"{companies_updated} company profiles have been updated in HubSpot")

if __name__ == "__main__":
    try:
        main()
    finally:
        mixpanel.close()
