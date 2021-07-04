from configparser import ConfigParser
import  psycopg2


def config(filename='db/creds.ini', section='postgresql'):
    # Change the directory to the folder where the script itself is located
    # path = os.path.dirname(os.path.abspath(__file__))
    # os.chdir(path)
    
    # Create a parser
    parser = ConfigParser()
    
    # Read config file
    parser.read(filename)
    
    # Get section, default to postgresql
    conn_detail = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            conn_detail[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return conn_detail


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # Read connection parameters
        params = config()
        
        conn_dic = {
            "host"      : params['host'],
            "port"      : params['port'],
            "database"  : params['database'],
            "user"      : params['user'],
            "password"  : params['password']
        }

        # Connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...\n')
        conn = psycopg2.connect(**conn_dic)
        print('Connected.\n')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return 0

    return conn


def execute_query(conn, command):
    """ Query Execution function """
    try:
        cur = conn.cursor()
        cur.execute(command)
        conn.commit()
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 0


def load_data(conn, file, table):
    try:
        cur = conn.cursor()
        cur.copy_from(file, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 0
    finally:
        cur.close()


def disconnect(conn):
    conn.close()
    print('Connection Closed...\n')