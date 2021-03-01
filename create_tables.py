import psycopg2
from config import config


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (

        """
        
        CREATE TABLE IF NOT EXISTS cities (
            id int8 PRIMARY KEY,
            city_name VARCHAR(500) NOT NULL
        )
        """,

        """
        
        CREATE TABLE IF NOT EXISTS homes (
            id int8 PRIMARY KEY,
            city_id int8 NOT NULL,
            build_year int,
            number_of_floors int,
            address varchar(700) NOT NULL,
            parking varchar(500),
            lifts_passenger int,
            lifts_freight int,
            lat float NOT NULL,
            lon float NOT NULL,

            CONSTRAINT FK_homes_cities FOREIGN KEY(city_id) REFERENCES cities(id)
        )
        """,

        """
        
        CREATE TABLE IF NOT EXISTS apartments (
            id int8 PRIMARY KEY,
            home_id int8 NOT NULL,
            floor int,
            number_of_rooms int,
            area float,
            kitchen_area float,
            number_of_bedrooms int,
            number_of_bathrooms int,
            separated_bathrooms bool,

            CONSTRAINT FK_apartments_homes FOREIGN KEY(home_id) REFERENCES homes(id)
        )
        """,

        """
        
        CREATE TABLE IF NOT EXISTS companies (
            id int8 PRIMARY KEY,
            company_name varchar(500),
            phone varchar(40)
        )
        """,

        """
        
        CREATE TABLE IF NOT EXISTS agents (
            id int8 PRIMARY KEY,
            company_id int8,
            agent_name varchar(100) NOT NULL,
            phone varchar(40),
            is_agent bool,

            CONSTRAINT FK_agents_companies FOREIGN KEY(company_id) REFERENCES companies(id)
        )
        """,

        """  
            
        CREATE TABLE IF NOT EXISTS listings (
            id int8 PRIMARY KEY,
            apt_id int8 NOT NULL,
            agent_id int8 NOT NULL,
            category varchar(15),
            deal_type varchar(15) NOT NULL,
            published_date date,
            offer_type varchar(15) NOT NULL,
            living_area int,
            room_area int,
            window_view varchar(500),
            communal_payments varchar(100),
            deposit float,
            commission float,

            CONSTRAINT FK_listings_apartments FOREIGN KEY(apt_id) REFERENCES apartments(id),
            CONSTRAINT FK_listings_agents FOREIGN KEY(agent_id) REFERENCES agents(id)
        )
        """,

        """
        
        CREATE TABLE IF NOT EXISTS prices (
            listing_id int8,
            price float NOT NULL,
            price_date date NOT NULL,

            CONSTRAINT FK_prices_listings FOREIGN KEY(listing_id) REFERENCES listings(id)
        )
        """
    )
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()