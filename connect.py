import psycopg2
from pymongo import MongoClient
from config import config


def connect_pgsql():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        params = config()

        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        cur = conn.cursor()

        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        db_version = cur.fetchone()
        print(db_version)

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def connect_mongo(config_file='database.ini', collection=None):
    """ Connect to the Mongo database server """
    params = config(section='mongodb', filename=config_file)
    uri = f'mongodb://{params["user"]}:{params["password"]}@{params["host"]}:{params["port"]}/'
    client = MongoClient(uri)
    db = client[params['database']]
    if collection:
        return db[collection]
    else:
        return db[params['collection']]


if __name__ == '__main__':
    connect_pgsql()