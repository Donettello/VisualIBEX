import mysql.connector
import pandas as pd

from config import DB_CONFIG, EMPRESAS_IBEX, RUTA_PRUEBA

def cargar_empresas_info():
    try:
        # Conexión con la base de datos
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Conectado a MariaDB con éxito.")

        # Creamos la query de inserción.
        # Se usa IGNORE para que si el ticker está, no salga error
        sql = "INSERT IGNORE INTO empresas_info (ticker, nombre_empresa) VALUES (%s, %s)"

        # Convertimos el diccionario en lista de tuplas para la inserción
        datos = list(EMPRESAS_IBEX.items())

        # Ejecución masiva
        cursor.executemany(sql, datos)
        conn. commit()

        print(f"Proceso finalizado. Filas insertadas/actualizadas: {cursor.rowcount}")

    except Exception as e:
        print(f"Error durante la carga: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexión cerrada")
    
    return

def primera_carga_db():
    try:
        # Leemos CSV con Pandas
        print(f"Leyendo {RUTA_PRUEBA}")
        df = pd.read_csv(RUTA_PRUEBA, sep=';')

        # Conectamos a MariaDB
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Conectado a MariaDB con éxito.")

        # Preparamos la query para la inserción
        sql = "INSERT IGNORE INTO historico_ibex \
            (fecha, ticker, precio_apertura, precio_cierre, rent_sesion, rent_diaria, confirmado)\
            VALUES (%s, %s, %s, %s, %s, %s, %s)"
        
        # Convertir DataFrame a lista de tuplas para la inserción masiva
        # Mapeamos las columnas del CSV a las de la DB
        datos_para_db = []
        for _, fila in df.iterrows():
            try:    
                datos_para_db.append((
                    fila['Fecha'],
                    fila['Ticker'],
                    round(float(fila['Precio apertura']), 4),
                    round(float(fila['Precio cierre']), 4),
                    round(float(fila['Rentabilidad sesion (%)']), 4),
                    round(float(fila['Rentabilidad diaria (%)']), 4), 
                    fila['Confirmado']
                ))
            except KeyError as e:
                print(f"Error: No encuentro la columna {e}. Revisa si en el CSV se llama distinto.")
                return # Cortamos para que revises los nombres

        
        # Ejecución masiva
        cursor.executemany(sql, datos_para_db)
        conn.commit()

        print(f"Éxito: Se han procesado {len(datos_para_db)} filas.")
        print(f"Filas realmente insertadas (sin duplicados): {cursor.rowcount}")

    except Exception as e:
        print(f"Error durante la carga: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexión cerrada")
    return

if __name__ == '__main__':
    primera_carga_db()