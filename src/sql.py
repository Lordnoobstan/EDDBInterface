import psycopg2
import psycopg2.pool
import time
import json
from datetime import datetime, timezone

with open("config.json") as config_file:
    database_login_info = json.load(config_file)["SQL Login"]


class SQLConnection:
    def __init__(self, credentials: dict[str, str], pool: psycopg2.pool.ThreadedConnectionPool = None):
        connection = None
        cursor = None
        success = False

        while not success:
            try:
                if pool is None:
                    connection = psycopg2.connect(**credentials)
                    success = True
                else:
                    connection = pool.getconn()
                cursor = connection.cursor()

                connection.autocommit = True
                break
            except psycopg2.OperationalError as e:
                print(e)
                print(f"Failed to connect to the SQL database with the credentials {credentials}")

            time.sleep(3)

        self.connection = connection
        self.cursor = cursor
        self.connection_pool = pool

    def execute(self, query, parameters=None) -> tuple:
        try:
            return self.cursor.execute(query, parameters)
        except (psycopg2.OperationalError, AttributeError) as e:
            print(e)
            return tuple(),

    def close(self):
        if self.cursor is not None:
            self.cursor.close()

        if self.connection is not None:
            if self.connection_pool is None:
                self.connection.close()
            else:
                self.connection_pool.putconn(self.connection)


# Writes the system table to the database supplied in configuration
def create_systems_table():
    database = None

    try:
        database = SQLConnection(database_login_info)

        database.cursor.execute("CREATE EXTENSION postgis;")

        database.cursor.execute("CREATE TABLE systems ("
                                "name TEXT,"
                                "system_id TEXT,"
                                "location geometry,"
                                "PRIMARY KEY (system_id)"
                                ");")

        database.cursor.execute("CREATE TABLE abstract_bodies ("
                                "name TEXT,"
                                "body_id TEXT,"
                                "system_id TEXT REFERENCES systems(system_id),"
                                "body_type TEXT,"
                                "distance FLOAT,"
                                "PRIMARY KEY (body_id, system_id)"
                                ");")

        database.cursor.execute("CREATE TABLE stars ("
                                "name TEXT,"
                                "body_id TEXT,"
                                "system_id TEXT REFERENCES systems(system_id),"
                                "class TEXT,"
                                "mass FLOAT,"
                                "distance FLOAT,"
                                "PRIMARY KEY (body_id, system_id)"
                                ");")

        database.cursor.execute("CREATE TABLE planets ("
                                "name TEXT,"
                                "body_id TEXT,"
                                "system_id TEXT REFERENCES systems(system_id),"
                                "class TEXT,"
                                "terraforming_state TEXT,"
                                "distance FLOAT,"
                                "is_discovered BOOL,"
                                "is_mapped BOOL,"
                                "PRIMARY KEY (body_id, system_id)"
                                ");")

        database.cursor.execute("CREATE TABLE stations ("
                                "name TEXT,"
                                "body_id TEXT,"
                                "system_id TEXT REFERENCES systems(system_id),"
                                "station_id TEXT,"
                                "distance FLOAT,"
                                "station_type TEXT,"
                                "last_updated TIMESTAMP,"
                                "PRIMARY KEY (station_id)"
                                ");")

        database.cursor.execute("CREATE TABLE commodities ("
                                "name TEXT,"
                                "commodity_id TEXT,"
                                "station_id TEXT,"
                                "buy_price INT,"
                                "sell_price INT,"
                                "mean_price INT,"
                                "units_in_stock INT,"
                                "units_in_demand INT,"
                                "PRIMARY KEY (commodity_id)"
                                ");")

        database.cursor.execute("CREATE TABLE logs ("
                                "status TEXT,"
                                "meta_message TEXT[],"
                                "event_type TEXT,"
                                "payload TEXT,"
                                "system_of_interest TEXT,"
                                "body_of_interest TEXT,"
                                "upload_timestamp TIMESTAMP"
                                ");")
    except psycopg2.Error:
        print("Systems table already exists")
    finally:
        if database:
            database.close()


# Checks to see if the specified system exists in the database
def is_system_in_database(system_id: str = None, system_name: str = None,
                          pool: psycopg2.pool.ThreadedConnectionPool = None) -> bool:
    database = SQLConnection(database_login_info, pool)

    database.execute("SELECT EXISTS(SELECT 1 FROM systems WHERE system_id = %s OR name = %s);", (system_id, system_name))
    system = database.cursor.fetchone()

    database.close()

    return system[0]


# Check to see if the specified star exists in the database
def is_star_in_database(system_id: str = None, body_id: str = None, star_name: str = None,
                        pool: psycopg2.pool.ThreadedConnectionPool = None) -> bool:
    assert system_id is not None and (body_id is not None or star_name is not None), \
        "You must specify a valid method of identifying the star"
    database = SQLConnection(database_login_info, pool)

    database.execute("SELECT EXISTS(SELECT 1 FROM stars WHERE (system_id = %s AND body_id = %s OR name = %s));",
                     (system_id, body_id, star_name))
    star = database.cursor.fetchone()

    database.close()

    return star[0]


# Check to see if the specified planet exists in the database
def is_planet_in_database(system_id: str = None, body_id: str = None, planet_name: str = None,
                          pool: psycopg2.pool.ThreadedConnectionPool = None) -> bool:
    assert system_id is not None and (body_id is not None or planet_name is not None), \
        "You must specify a valid method of identifying the planet"
    database = SQLConnection(database_login_info, pool)

    database.execute("SELECT EXISTS(SELECT 1 FROM planets WHERE system_id = %s AND body_id = %s OR name = %s);",
                     (system_id, body_id, planet_name))
    planet = database.cursor.fetchone()

    database.close()

    return planet[0]


# Check to see if the specified station exists in the database
def is_station_in_database(system_id: str = None, system_name: str = None, station_name: str = None,
                           pool: psycopg2.pool.ThreadedConnectionPool = None):
    assert (system_id is not None or system_name is not None) and station_name is not None, \
        "You must specify a valid method of identifying the station"
    database = SQLConnection(database_login_info, pool)

    if system_id is None:
        system_id = get_system_id(system_name)

    database.execute("SELECT EXISTS(SELECT 1 FROM stations WHERE system_id = %s AND name = %s);",
                     (system_id, station_name))
    station = database.cursor.fetchone()

    database.close()

    return station[0]


# Returns the id of the specified system
def get_system_id(system_name: str) -> str:
    database = SQLConnection(database_login_info)

    database.execute("SELECT system_id FROM systems WHERE name = %s;", (system_name,))
    system_id = database.cursor.fetchone()[0]

    database.close()

    return system_id


# Returns the body if of the specified market
def get_station_body_id(market_id: str) -> str:
    database = SQLConnection(database_login_info)

    database.execute("SELECT body_id FROM stations WHERE station_id = %s;", (market_id,))
    body_id = database.cursor.fetchone()[0]

    database.close()

    return body_id


# Inserts the specified system information into the database, it is up to the user to
# check to ensure the system has not already been logged, otherwise an error may occur
def update_system_row(system_name: str, system_id: int, location: list,
                      pool: psycopg2.pool.ThreadedConnectionPool = None) -> None:
    database = SQLConnection(database_login_info, pool)

    database.execute("INSERT INTO systems VALUES (%s, %s, %s)"
                     "ON CONFLICT (system_id) DO NOTHING;", (system_name, system_id, f"POINT({' '.join(location)})"))

    database.close()


# Inserts the specified star information into the database
def update_star_row(star_name: str, body_id: str, system_id: str, star_class: str = "unknown", mass: float = 0, distance: float = -1,
                    pool: psycopg2.pool.ThreadedConnectionPool = None) -> None:
    if distance is None:
        distance = -1

    database = SQLConnection(database_login_info, pool)

    parameters = {
        "name": star_name,
        "body_id": body_id,
        "system_id": system_id,
        "star_class": star_class,
        "mass": mass,
        "distance": distance,
        "body_type": "Star"
    }

    database.execute("INSERT INTO abstract_bodies VALUES("
                     "%(name)s,"
                     "%(body_id)s,"
                     "%(system_id)s,"
                     "%(body_type)s,"
                     "%(distance)s"
                     ") ON CONFLICT (body_id, system_id) DO UPDATE "
                     "SET distance = %(distance)s "
                     "WHERE abstract_bodies.system_id = %(system_id)s AND abstract_bodies.body_id = %(body_id)s;",
                     parameters)

    database.execute("INSERT INTO stars VALUES("
                     "%(name)s,"
                     "%(body_id)s,"
                     "%(system_id)s,"
                     "%(star_class)s,"
                     "%(mass)s,"
                     "%(distance)s"
                     ") ON CONFLICT (body_id, system_id) DO UPDATE "
                     "SET class = %(star_class)s,"
                     "distance = %(distance)s,"
                     "mass = %(mass)s "
                     "WHERE stars.system_id = %(system_id)s AND stars.body_id = %(body_id)s;", parameters)

    database.close()


# Inserts the specified planet information into the database
def update_planet_row(planet_name: str, body_id: str, system_id: str, planet_class: str = "unknown",
                      terraforming_state: str = "unknown", distance: float = 0, is_discovered: bool = True,
                      is_mapped: bool = True, pool: psycopg2.pool.ThreadedConnectionPool = None) -> None:
    if distance is None:
        distance = -1

    database = SQLConnection(database_login_info, pool)

    parameters = {
        "name": planet_name,
        "body_id": body_id,
        "system_id": system_id,
        "planet_class": planet_class,
        "terraforming_state": terraforming_state,
        "distance": distance,
        "is_discovered": is_discovered,
        "is_mapped": is_mapped,
        "body_type": "Planet"
    }

    database.execute("INSERT INTO abstract_bodies VALUES("
                     "%(name)s,"
                     "%(body_id)s,"
                     "%(system_id)s,"
                     "%(body_type)s,"
                     "%(distance)s"
                     ") ON CONFLICT (body_id, system_id) DO UPDATE "
                     "SET distance = %(distance)s "
                     "WHERE abstract_bodies.system_id = %(system_id)s AND abstract_bodies.body_id = %(body_id)s",
                     parameters)

    database.execute("INSERT INTO planets VALUES ("
                     "%(name)s,"
                     "%(body_id)s,"
                     "%(system_id)s,"
                     "%(planet_class)s,"
                     "%(terraforming_state)s,"
                     "%(distance)s,"
                     "%(is_discovered)s,"
                     "%(is_mapped)s"
                     ") ON CONFLICT (body_id, system_id) DO UPDATE "
                     "SET class = %(planet_class)s,"
                     "terraforming_state = %(terraforming_state)s,"
                     "distance = %(distance)s,"
                     "is_discovered = %(is_discovered)s,"
                     "is_mapped = %(is_mapped)s "
                     "WHERE planets.system_id = %(system_id)s AND planets.body_id = %(body_id)s;", parameters)


# Inserts the specified station information into the database
def update_station_row(station_name: str, body_id: str, system_id: str, station_id: str, distance: float = -1,
                       station_type: str = "unknown", last_updated: datetime = None,
                       pool: psycopg2.pool.ThreadedConnectionPool = None) -> None:
    if distance is None:
        distance = -1

    if last_updated is None:
        last_updated = datetime.now(timezone.utc)

    database = SQLConnection(database_login_info, pool)

    parameters = {
        "name": station_name,
        "body_id": body_id,
        "system_id": system_id,
        "station_id": station_id,
        "distance": distance,
        "station_type": station_type,
        "last_updated": last_updated,
        "body_type": "Station"
    }

    database.execute("INSERT INTO abstract_bodies VALUES("
                     "%(name)s,"
                     "%(body_id)s,"
                     "%(system_id)s,"
                     "%(body_type)s,"
                     "%(distance)s"
                     ") ON CONFLICT (body_id, system_id) DO UPDATE "
                     "SET distance = %(distance)s "
                     "WHERE abstract_bodies.system_id = %(system_id)s AND abstract_bodies.body_id = %(body_id)s",
                     parameters)

    database.execute("INSERT INTO stations VALUES ("
                     "%(name)s,"
                     "%(body_id)s,"
                     "%(system_id)s,"
                     "%(station_id)s,"
                     "%(distance)s,"
                     "%(station_type)s,"
                     "%(last_updated)s"
                     ") ON CONFLICT (station_id) DO UPDATE "
                     "SET distance = %(distance)s,"
                     "last_updated = %(last_updated)s "
                     "WHERE stations.station_id = %(station_id)s;", parameters)


# Inserts the specified commodity information into the database
def update_commodity_row(commodity_name: str, commodity_id: str, station_id: str, buy_price: int, sell_price: int,
                         mean_price: int, units_in_stock: int, units_in_demand: int,
                         pool: psycopg2.pool.ThreadedConnectionPool = None) -> None:
    database = SQLConnection(database_login_info, pool)

    parameters = {
        "name": commodity_name,
        "commodity_id": commodity_id,
        "station_id": station_id,
        "buy_price": buy_price,
        "sell_price": sell_price,
        "mean_price": mean_price,
        "units_in_stock": units_in_stock,
        "units_in_demand": units_in_demand
    }

    database.execute("INSERT INTO commodities VALUES ("
                     "%(name)s,"
                     "%(commodity_id)s,"
                     "%(station_id)s,"
                     "%(buy_price)s,"
                     "%(sell_price)s,"
                     "%(mean_price)s,"
                     "%(units_in_stock)s,"
                     "%(units_in_demand)s"
                     ") ON CONFLICT (commodity_id) DO UPDATE "
                     "SET buy_price = %(buy_price)s,"
                     "sell_price = %(sell_price)s,"
                     "mean_price = %(mean_price)s,"
                     "units_in_stock = %(units_in_stock)s,"
                     "units_in_demand = %(units_in_demand)s "
                     "WHERE commodities.commodity_id = %(commodity_id)s;", parameters)

    database.close()


# Inserts the specified information into the logs database
def insert_log_row(status: str, meta_message: list[str], event_type: str, payload: json, system_of_interest: str,
                   body_of_interest: str, upload_timestamp: datetime = None):
    if not upload_timestamp:
        upload_timestamp = datetime.now(timezone.utc)

    database = SQLConnection(database_login_info)

    database.execute("INSERT INTO logs VALUES (%s, %s, %s, %s, %s, %s, %s)",
                     (status, meta_message, event_type, payload, system_of_interest,
                      body_of_interest, upload_timestamp))
