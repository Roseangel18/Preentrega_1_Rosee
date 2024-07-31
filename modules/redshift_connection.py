import psycopg2



def read_redshift_password(file_path):
    with open(file_path, 'r') as f:
        return f.read()

def connect_redshift(dbname, host, port, username, password):
    conn_string = f"dbname='{dbname}' host='{host}' port='{port}' user='{username}' password='{password}'"
    try:
        conn = psycopg2.connect(conn_string)
        print("Conexión a Redshift exitosa!!")
        return conn
    except Exception as e:
        print(f"Error al conectar a Redshift: {e}")
        return None

def create_canciones_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS roseangelbazan1_coderhouse.Canciones
            (
                id varchar(100) primary key,
                artista varchar(100),
                cancion varchar(50),
                genero varchar (50),
                album varchar(100),
                total_track INTEGER,
                popularity INTEGER,
                fecha_lanzamiento date,
                duracion_ms INTEGER,
                album_img varchar(300)
            )
        """)
        conn.commit()
        print("Se crea la tabla Canciones ")

def truncate_canciones_table(conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE Canciones")
        count = cur.rowcount
        print(count)