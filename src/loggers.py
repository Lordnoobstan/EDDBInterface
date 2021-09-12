import json
import psycopg2.pool
import sql

# Items that aren't logged in commodity tables because they cannot be traded
untradable_salvage = [
    "DamagedEscapePod",
    "Hostage",
    "OccupiedCryoPod",
    "PersonalEffects",
    "WreckageComponents"
]

# Item prefixes that aren't logged in commodity tables because they cannot be traded
types_not_logged = [
    "USS"
]


# Returns whether or not the specified commodity should be logged
def log_commodity(name: str) -> bool:
    if name in untradable_salvage:
        return False

    for type_name in types_not_logged:
        if name.startswith(type_name):
            return False

    return True


# Logs basic body information provided in FSDJump Journal
def handle_fsd_jump_journal(message_json: json, pool: psycopg2.pool.ThreadedConnectionPool) -> \
        tuple[str, list[str], str, str]:
    payload = message_json["message"]
    payload["BodyID"] = str(payload["BodyID"])
    payload["StarPos"] = [str(position_component) for position_component in payload["StarPos"]]
    payload["SystemAddress"] = str(payload["SystemAddress"])

    if not sql.is_system_in_database(system_id=payload["SystemAddress"], pool=pool):
        sql.update_system_row(payload["StarSystem"], payload["SystemAddress"], payload["StarPos"], pool=pool)

    body_type = payload["BodyType"]
    body_information = (payload["Body"], payload["BodyID"], payload["SystemAddress"])

    if body_type == "Star":
        if not sql.is_star_in_database(system_id=payload["SystemAddress"], body_id=payload["BodyID"], pool=pool):
            sql.update_star_row(*body_information, pool=pool)
    elif body_type == "Planet":
        if not sql.is_planet_in_database(system_id=payload["SystemAddress"], body_id=payload["BodyID"], pool=pool):
            sql.update_planet_row(*body_information, pool=pool)
    else:
        return "Ignored", [f"Bodies of type {body_type} are not logged with this event"], payload["SystemAddress"], ""

    return "Success", [], payload["SystemAddress"], payload["BodyID"]


# Logs basic body information provided in Location Journal
def handle_location_journal(message_json: json, pool: psycopg2.pool.ThreadedConnectionPool) -> \
        tuple[str, list[str], str, str]:
    payload = message_json["message"]
    payload["BodyID"] = str(payload["BodyID"])
    payload["StarPos"] = [str(position_component) for position_component in payload["StarPos"]]
    payload["SystemAddress"] = str(payload["SystemAddress"])

    if not sql.is_system_in_database(payload["SystemAddress"], pool=pool):
        sql.update_system_row(payload["StarSystem"], payload["SystemAddress"], payload["StarPos"], pool=pool)

    body_type = payload["BodyType"]
    body_information = (payload["Body"], payload["BodyID"], payload["SystemAddress"])

    distance = payload["DistFromStarLS"] if "DistFromStarLS" in payload.keys() else None
    market_id = str(payload["MarketID"]) if "MarketID" in payload.keys() else None

    if body_type == "Star":
        if not sql.is_star_in_database(payload["SystemAddress"], payload["BodyID"], pool=pool) or \
                distance is not None:
            sql.update_star_row(*body_information, distance=distance, pool=pool)
    elif body_type == "Planet":
        if not sql.is_planet_in_database(payload["SystemAddress"], payload["BodyID"]) or \
                distance is not None:
            sql.update_planet_row(*body_information, distance=distance, pool=pool)

    if payload["Docked"]:
        if not sql.is_station_in_database(payload["SystemAddress"], station_name=payload["StationName"], pool=pool) or \
                distance is not None:
            sql.update_station_row(payload["StationName"], payload["BodyID"], payload["SystemAddress"], market_id,
                                   distance, payload["StationType"], pool=pool)

    return "Success", [], payload["SystemAddress"], payload["BodyID"]


# Logs complete body information provided in Scan Journal
def handle_scan_journal(message_json: json, pool: psycopg2.pool.ThreadedConnectionPool) -> \
        tuple[str, list[str], str, str]:
    payload = message_json["message"]
    payload["BodyID"] = str(payload["BodyID"])
    payload["SystemAddress"] = str(payload["SystemAddress"])

    body_information: tuple[str, str, list[str]] = (payload["BodyName"], payload["BodyID"], payload["SystemAddress"])

    distance = payload["DistanceFromArrivalLS"] if "DistanceFromArrivalLS" in payload.keys() else None
    planet_class = payload["PlanetClass"] if "PlanetClass" in payload.keys() else None
    star_class = payload["StarType"] if "StarType" in payload.keys() else None

    if not sql.is_system_in_database(system_id=payload["SystemAddress"], pool=pool):
        return "Ignored", ["Parent bodies are not already logged"], "", ""

    if planet_class is not None:
        if "TerraformState" in payload.keys():
            sql.update_planet_row(*body_information, planet_class, payload["TerraformState"], payload["MassEM"],
                                  distance, payload["WasDiscovered"], payload["WasMapped"], pool=pool)
        else:
            return "Ignored", ["Lacking terraform information"], payload["SystemAddress"], payload["BodyID"]
    elif star_class is not None:
        sql.update_star_row(*body_information, star_class, payload["StellarMass"], distance, pool=pool)
    else:
        return "Ignored", ["Bodies of this type are not logged"], payload["SystemAddress"], ""

    return "Success", [], payload["SystemAddress"], payload["BodyID"]


def handle_commodity(message_json: json, pool: psycopg2.pool.ThreadedConnectionPool) -> tuple[str, list[str], str, str]:
    payload = message_json["message"]
    payload["marketId"] = str(payload["marketId"])

    if not sql.is_system_in_database(system_name=payload["systemName"], pool=pool) or \
            not sql.is_station_in_database(system_name=payload["systemName"], station_name=payload["stationName"],
                                           pool=pool):
        return "Ignored", ["Parent bodies are not already logged"], "", ""

    system_id = sql.get_system_id(payload["systemName"])
    body_id = sql.get_station_body_id(payload["marketId"])
    meta_message = []

    if len(payload["commodities"]) == 0:
        return "Ignored", ["No commodities present"], system_id, body_id

    for commodity in payload["commodities"]:
        if log_commodity(commodity["name"]):
            commodity_id = f"{payload['marketId']}_{commodity['name']}"

            sql.update_commodity_row(commodity["name"], commodity_id, payload["marketId"], commodity["buyPrice"],
                                     commodity["sellPrice"], commodity["meanPrice"], commodity["stock"],
                                     commodity["demand"], pool=pool)
        else:
            meta_message.append(f"Ignoring untradable commodity {commodity['name']}")

    return "Success", meta_message, system_id, body_id
