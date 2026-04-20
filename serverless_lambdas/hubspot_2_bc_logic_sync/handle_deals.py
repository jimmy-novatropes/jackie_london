from typing import Dict, Any
import traceback


def handle_deal(event: Dict[str, Any], credentials: Dict[str, str]) -> str:
    if event["subscriptionType"] == "deal.creation":
        deal_response = classify_deal(event, credentials)
        return deal_response
    elif event["subscriptionType"] == "deal.propertyChange":
        deal_response = classify_deal(event, credentials)
        return deal_response
    elif event["subscriptionType"] == "deal.associationChange":
        deal_response = classify_deal(event, credentials)
        return deal_response
    return f"⚠️ Unknown deal event: {event}"


def classify_deal(event: Dict[str, Any], credentials: Dict[str, str]):
    try:
        if 'objectId' in event:
            deal_id = event.get("objectId")
        elif 'fromObjectId' in event and 'associationType' in event:
            deal_id = event.get("fromObjectId") if event.get("associationType").startswith("DEAL_") else event.get("toObjectId")
        else:
            print("❌ No objectId or from/toObjectId in event")

        property_changed = event.get("propertyName")
        property_value = event.get("propertyValue")
        hubspot_token = credentials["HUBSPOT_TOKEN"]


    #     deal_props_map = invert_dict(load_json_file("deal_properties.json"))
    #     company_props_map = invert_dict(load_json_file("company_properties.json"))
    #     contact_props_map = invert_dict(load_json_file("contact_properties.json"))
    #     dropdown_options_json = load_json_file("dropdown_options.json")
    #
    #     creds = load_netsuite_credentials()
    #     session, base_url = create_netsuite_client(creds)
    #     deal_owners = get_all_hubspot_users(hubspot_token)
    #     dropdown_options, dropdown_options_extra = get_dropdown_mappings(session, base_url)
    #
    #
    #     if not dropdown_options:
    #         dropdown_options = dropdown_options_json
    #     else:
    #         d1 = dropdown_options["subsidiary"]
    #         result = {
    #             k.split(":")[-1].strip(): v
    #             for k, v in d1.items()
    #         }
    #         dropdown_options["subsidiary"] = result
    #
    #     # Refresh dropdown options from NetSuite in case of changes
    #     deal_props_map["manual_syn"] = "manual_syn"
    #     deal_props_map["netsuite__delivery_type"] = "netsuite__delivery_type"
    #     deal_props_map["netsuite_if_retirement"] = "netsuite_if_retirement"
    #     deal_props_map["netsuite_retirement_language"] = "netsuite_retirement_language"
    #     deal_props_map["acr_retirement_reason"] = "acr_retirement_reason"
    #     deal_props_map["acr_purpose_of_retirement"] = "acr_purpose_of_retirement"
    #     deal_props_map["other_retirement_language"] = "other_retirement_language"
    #
    #
    #     deal = get_deal_with_associations(deal_id, hubspot_token, deal_props_map)
    #     deal_owner_id = deal["properties"].get("hubspot_owner_id")
    #     deal_owner_name = next((o["firstName"] + " " + o["lastName"] for o in deal_owners if o["id"] == deal_owner_id), None)
    #     employee_id = dropdown_options["employees"].get(deal_owner_name)
    #
    #     netsuite_deal_id = deal.get("properties", {}).get("netsuite_id")
    #     if netsuite_deal_id:
    #
    #         handle_existing_records(
    #             deal_id, netsuite_deal_id, session, creds, hubspot_token,
    #             deal_props_map, company_props_map, dropdown_options_extra,
    #             deal, event, employee_id
    #         )
    #
    #     elif ((property_changed == "manual_syn" and property_value == "true") or
    #           (property_changed == "dealstage" and property_value == "closedwon") or
    #           (property_changed == "dealstage" and property_value == "2488265443")):
    #         handle_new_records(
    #             deal_id, session, creds, hubspot_token,
    #             deal_props_map, company_props_map, contact_props_map,
    #             dropdown_options, deal, employee_id,
    #             base_url
    #         )
    #     else:
    #         update_hubspot_object(
    #             "deals",
    #             deal_id,
    #             {
    #                 "netsuite_sync_status": ""
    #             },
    #             hubspot_token
    #         )
    #
    except Exception as e:
        traceback.print_exc()
        print(f"❌ Exception in classify_deal: {e}, event: {event}")
