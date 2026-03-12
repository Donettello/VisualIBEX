import mysql.connector
import pandas as pd
import time
import yfinance as yf

from config import EMPRESAS_IBEX
from config_privado import *
from datetime import datetime

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

def cargar_historico_total():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    fecha_inicio_api = '2021-12-25'
    limite_2022 = datetime(2022, 1, 1).date()
    fecha_fin_api = '2026-02-03' # Ya tengo los datos a partir de esta fecha

    print(f"Iniciando carga masiva de {len(EMPRESAS_IBEX)} tickers")
    print("Desde 01/2022 -> 02/2026")

    for ticker, nombre in EMPRESAS_IBEX.items():
        try:
            print(f"Descargando datos para la empresa {ticker}")
            accion = yf.Ticker(ticker)

            print(f"Obteniendo historial")
            # Descargamos el bloque de datos
            df = accion.history(start=fecha_inicio_api, end=fecha_fin_api)

            # Comprobamos que hay  datos
            if df.empty:
                print(f"Sin datos suficientes de {ticker} para este rango")
                continue

            df.sort_index()

            for i in range(len(df)):
                fecha_actual = df.index[i].date()

                if fecha_actual < limite_2022:
                    continue

                p_apertura = float(df['Open'].iloc[i])
                p_cierre = float(df['Close'].iloc[i])

                # Rentabilidad durante la sesión
                rent_sesion = ((p_cierre - p_apertura) / p_apertura) * 100

                # Rentabilidad respecto al cierre anterior
                if i > 0:
                    p_anterior = float(df['Close'].iloc[i - 1])
                    rent_diaria = ((p_cierre - p_anterior) / p_anterior) * 100
                else:
                    rent_diaria = 0.0
                
                sql = '''
                    INSERT IGNORE INTO historico_ibex
                    (fecha, ticker, precio_apertura, precio_cierre, rent_sesion, rent_diaria, confirmado)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                '''

                valores = (
                    fecha_actual,
                    ticker,
                    round(float(p_apertura), 4),
                    round(float(p_cierre), 4),
                    round(float(rent_sesion), 4),
                    round(float(rent_diaria), 4),
                    1 # Datos consolidados
                )

                cursor.execute(sql, valores)

            conn.commit()
            print(f"Datos de {nombre} sincronizados")
            time.sleep(1) # Para no ser baneado por Yahoo

        except Exception as e:
            print(f"Error en {ticker}: {e}")
            conn.rollback

    cursor.close()
    conn.close()

    print("El histórico de 4 años se ha rellenado correctamente.")
    return

def inyectar_historico(ticker, fecha_inicio, fecha_final=None):
    """
    Inyecta datos históricos de Yahoo Finance en la base de datos MariaDB.
    Si fecha_final no se indica, toma la fecha actual.
    """
    if fecha_final is None:
        fecha_final = datetime.now().strftime('%Y-%m-%d')

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print(f"Iniciando carga: {ticker} desde {fecha_inicio} hasta {fecha_final}")
    
    try:
        accion = yf.Ticker(ticker)

        # Usamos rango donde SÍ hay datos
        df = accion.history(start=fecha_inicio, end=fecha_final)

        if df.empty:
            print(f"No hay datos de {ticker} para el rango seleccionado")
            return
        
        df.sort_index()

        for i in range(len(df)):
            fecha_actual = df.index[i].date()
            p_apertura = float(df['Open'].iloc[i])
            p_cierre = float(df['Close'].iloc[i])

            rent_sesion = ((p_cierre - p_apertura) / p_apertura) * 100

            if i > 0:
                p_cierre_ayer = float(df['Close'].iloc[i-1])
                rent_diaria = ((p_cierre - p_cierre_ayer) / p_cierre_ayer) * 100
            else:
                rent_diaria = 0.0
            
            sql = """
                INSERT IGNORE INTO historico_ibex 
                (fecha, ticker, precio_apertura, precio_cierre, rent_sesion, rent_diaria, confirmado)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                fecha_actual, 
                ticker, 
                round(p_apertura, 4), 
                round(p_cierre, 4), 
                round(rent_sesion, 2), 
                round(rent_diaria, 2), 1
            ))

        conn.commit()
        print(f"Histórico de {ticker} inyectado desde su debut.")
    except Exception as e:
        print(f"Error con {ticker}: {e}")
    finally:
        cursor.close()
        conn.close()
    
    return

if __name__ == '__main__':
    '''Ejemplos de uso de inyectar_historico'''
    # Caso 1: Parche específico para Puig
    # inyectar_historico("PUIG.MC", "2024-05-01", "2026-02-03")

    # Caso 2: Un ticker que quieras añadir nuevo desde 2022 hasta hoy
    # inyectar_historico("TEF.MC", "2022-01-01") 

    # Caso 3: Rellenar un hueco de una semana específica
    # inyectar_historico("SAN.MC", "2023-06-01", "2023-06-08")