import psycopg2
from config import config
from connect import connect_mongo
from utils import md5_hash


def insert_data_list_in_tables(sql, data_list):
    """ insert multiple dates into the tables  """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, data_list)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def data_for_cities_table():
    sql = "INSERT INTO cities(id, city_name) VALUES(%s,%s)"
    collection = connect_mongo()
    cursor = collection.find()
    cities = set()
    for doc in cursor:
        address = doc['address']
        if 'locality' not in address:
            continue
        locality = address['locality']
        hash_city_id = md5_hash((locality['name'],))
        cities.add((hash_city_id, locality['name']))

    insert_data_list_in_tables(sql, cities)


data_for_cities_table()


def data_for_homes_table():
    sql = """INSERT INTO homes(id, city_id, build_year, number_of_floors, address,
        parking, lifts_passenger, lifts_freight, lat, lon)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
    collection = connect_mongo()
    cursor = collection.find()
    homes = set()
    homes_id = set()
    for doc in cursor:
        address = doc['address']
        house = doc.get('house', None)
        if not house:
            continue
        locality = address.get('locality', None)
        if not locality:
            continue
        build_year = house.get('build_year', None)
        floors = house.get('floors', None)
        parking = house.get('parking', None)
        if parking:
            parking_str = ', '.join([inner_dict['display_name'] for inner_dict in parking])
        else:
            parking_str = None
        lifts_freight = house.get('lifts_freight', None)
        lifts_passenger = house.get('lifts_passenger', None)
        position = address['position']
        lat = position.get('lat', None)
        lon = position.get('lon', None)
        hash_house_id = md5_hash((address['name'], locality['name']))
        hash_city_id = md5_hash((locality['name'],))
        if hash_house_id not in homes_id:
            homes.add((hash_house_id, hash_city_id, build_year, floors, address['name'], parking_str,
                       lifts_passenger, lifts_freight, lat, lon))
            homes_id.add(hash_house_id)
    insert_data_list_in_tables(sql, homes)

data_for_homes_table()


def data_for_apartments_table():
    sql = """INSERT INTO apartments(id, home_id, floor, number_of_rooms, area,
        kitchen_area, number_of_bedrooms, number_of_bathrooms, separated_bathrooms)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
    collection = connect_mongo()
    cursor = collection.find()
    apts = set()
    apt_ids = set()
    for doc in cursor:
        house = doc.get('house', None)
        if not house:
            continue
        if 'object_info' not in doc:
            continue
        apt_info = doc['object_info']
        address = doc['address']
        locality = address.get('locality', None)
        if not locality:
            continue
        floor = apt_info.get('floor', None)
        number_of_rooms = apt_info.get('rooms', None)
        area = apt_info.get('area', None)
        kitchen_area = apt_info.get('kitchen_area', None)
        number_of_bedrooms = apt_info.get('bedrooms', None)
        number_of_bathrooms = apt_info.get('bathrooms', None)
        separated_bathrooms = bool(apt_info.get('separated_bathrooms', None))

        hash_house_id = md5_hash((address['name'], locality['name']))
        hash_apt_id = md5_hash((address['name'], locality['name'], floor, area, number_of_rooms))
        if hash_apt_id not in apt_ids:
            apts.add((hash_apt_id, hash_house_id, floor, number_of_rooms, area, kitchen_area,
                       number_of_bedrooms, number_of_bathrooms, separated_bathrooms))
            apt_ids.add(hash_apt_id)
    insert_data_list_in_tables(sql, apts)

data_for_apartments_table()


def data_for_companies_table():
    sql = """INSERT INTO companies(id, company_name, phone) VALUES(%s,%s,%s)"""
    collection = connect_mongo()
    cursor = collection.find()
    companies = set()
    company_ids = set()
    for doc in cursor:
        seller = doc['seller']
        house = doc.get('house', None)
        if not house:
            continue
        address = doc['address']
        locality = address.get('locality', None)
        if not locality:
            continue
        company = seller.get('company', None)
        company_name = company.get('name', None)
        if not company_name:
            continue
        phone = company.get('phone', None)
        hash_company_id = md5_hash((company_name,))
        if hash_company_id not in company_ids:
            companies.add((hash_company_id, company_name, phone))
            company_ids.add(hash_company_id)


    insert_data_list_in_tables(sql, companies)

data_for_companies_table()


def data_for_agents_table():
    sql = """INSERT INTO agents(id, company_id, agent_name, phone, is_agent) VALUES(%s,%s,%s,%s,%s)"""
    collection = connect_mongo()
    cursor = collection.find()
    agents = set()
    agent_ids = set()
    counter = 0

    for doc in cursor:
        counter += 1
        seller = doc['seller']
        house = doc.get('house', None)
        if not house:
            continue
        address = doc['address']
        locality = address.get('locality', None)
        if not locality:
            continue

        company = seller.get('company', None)
        company_name = company.get('name', None)
        hash_company_id = md5_hash((company_name,))
        if not company_name:
            hash_company_id = None

        agent = seller.get('agent', None)
        phone = agent.get('phone', None)
        is_agent = agent.get('is_agent', None)
        agent_name = agent.get('full_name', None)
        if not agent_name:
            print(doc['_id'])
            continue

        hash_city_id = md5_hash((locality['name'],))
        hash_agent_id = md5_hash((hash_city_id, agent_name, phone))
        if hash_agent_id not in agent_ids:
            agents.add((hash_agent_id, hash_company_id, agent_name, phone, is_agent))
            agent_ids.add(hash_agent_id)
        if len(agents) > 100:
            insert_data_list_in_tables(sql, agents)
            agents = set()
            print('agents inserted. Docs passed ', counter)



    insert_data_list_in_tables(sql, agents)

data_for_agents_table()


def data_for_listings_table():
    sql = """INSERT INTO listings(id, apt_id, agent_id, category, deal_type, published_date, offer_type,
        living_area, room_area, window_view, communal_payments, deposit, commission)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    collection = connect_mongo()
    cursor = collection.find()
    listings = set()
    listing_ids = set()
    counter = 0

    for doc in cursor:
        counter += 1
        seller = doc['seller']
        house = doc.get('house', None)
        if not house:
            continue
        address = doc['address']
        locality = address.get('locality', None)
        if not locality:
            continue
        price_info = doc['price_info']
        object_info = doc['object_info']
        floor = object_info.get('floor', None)
        number_of_rooms = object_info.get('rooms', None)
        area = object_info.get('area', None)

        agent = seller.get('agent', None)
        phone = agent.get('phone', None)
        agent_name = agent.get('full_name', None)
        if not agent_name:
            continue

        category = doc.get('category', None)
        deal_type = doc.get('deal_type')
        published_date = doc.get('published_date', None)
        offer_type = doc.get('offer_type', None)
        living_area = object_info.get('living_area', None)
        if not living_area:
            living_area = None
        room_area = object_info.get('room_area', None)
        if not room_area:
            room_area = None
        window_view = object_info.get('window_view', None)
        if window_view:
            window_view_str = ', '.join([inner_dict['display_name'] for inner_dict in window_view])
        else:
            window_view_str = None
        communal_payments = price_info.get('communal_payments', None)
        if communal_payments:
            communal_payments_str = communal_payments.get('display_name', None)
        else:
            communal_payments_str = None
        deposit = price_info.get('deposit', None)
        if not deposit:
            deposit = None
        commission = price_info.get('commission', None)
        if not commission:
            commission = None

        id = doc['_id']
        hash_apt_id = md5_hash((address['name'], locality['name'], floor, area, number_of_rooms))
        hash_city_id = md5_hash((locality['name'],))
        hash_agent_id = md5_hash((hash_city_id, agent_name, phone))
        if id not in listing_ids:
            listings.add((id, hash_apt_id, hash_agent_id, category, deal_type, published_date, offer_type,
                          living_area, room_area, window_view_str, communal_payments_str, deposit,
                          commission))
            listing_ids.add(id)
        if len(listings) > 100:
            insert_data_list_in_tables(sql, listings)
            listings = set()
            print('listings inserted. Docs passed ', counter)



    insert_data_list_in_tables(sql, listings)

data_for_listings_table()


def data_for_prices_table():
    sql = """INSERT INTO prices(listing_id, price, price_date)
        VALUES(%s,%s,%s)"""
    collection = connect_mongo()
    cursor = collection.find()
    prices = set()
    counter = 0

    for doc in cursor:
        counter += 1

        house = doc.get('house', None)
        if not house:
            continue
        address = doc['address']
        locality = address.get('locality', None)
        if not locality:
            continue

        seller = doc.get('seller', None)
        agent = seller.get('agent', None)
        agent_name = agent.get('full_name', None)
        if not agent_name:
            continue

        price_from_date_to_date = doc['price']
        listing_id = doc['_id']
        for date, price in price_from_date_to_date.items():
            price_date = date
            prices.add((listing_id, price, price_date))
            if len(prices) > 100:
                insert_data_list_in_tables(sql, prices)
                prices = set()
                print('prices inserted. Docs passed ', counter)

    insert_data_list_in_tables(sql, prices)

data_for_prices_table()
