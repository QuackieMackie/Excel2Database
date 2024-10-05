import psycopg2


def get_db_connection(config):
    connection = psycopg2.connect(
        host=config['DATABASE']['server'],
        port=config['DATABASE']['port'],
        dbname=config['DATABASE']['database'],
        user=config['DATABASE']['user'],
        password=config['DATABASE']['password'],
        sslmode='require'
    )
    return connection
