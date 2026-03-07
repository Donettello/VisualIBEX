import mysql.connector
import os
import logging
import pandas as pd
import time
import yfinance as yf

from config import DB_CONFIG
from datetime import datetime, timedelta

def manejador_csv(nueva_fila, ruta_csv, max_registros=20):
    """
        Guarda una fila en el CSV y mantiene los últimos X registros
        nueva_fila: dict con formato {'Fecha':...,
                                      'Ticker':...,
                                      'Precio apertura':..., 
                                      'Precio cierre':..., 
                                      'Rentabilidad sesion(%)':...,
                                      'Rentabilidad diaria(%)':...}
    """
    # Comprobamos que la dirección del fichero es la correcta
    if os.path.exists(ruta_csv):
        df = pd.read_csv(ruta_csv, sep=';')
    else:
        # En caso de que  no exista
        df = pd.DataFrame(columns=['Fecha',
                                   'Ticker',
                                   'Precio apertura', 
                                   'Precio cierre', 
                                   'Rentabilidad sesion (%)', 
                                   'Rentabilidad diaria (%)'])

    # Añadimos la nueva fila al DataFrame
    # Convertimos la nueva fila en un pequeño DataFrame
    df_nueva = pd.DataFrame([nueva_fila])
    df = pd.concat([df, df_nueva], ignore_index=True)

    # Mantenemos sólo los últimos registros por empresa
    df = df.groupby('Ticker').tail(max_registros)

    # Guardamos los datos en el CSV
    df.to_csv(ruta_csv, index=False, sep=';')
    logging.info(f"Registro guardado. Total en archivo: {len(df)} lineas.")
    return

def revisar_pendientes(cursor, conn):
    # Construimos la query para obtener los que quedan pendientes de confirmación
    query = "SELECT fecha, ticker FROM historico_ibex WHERE confirmado=0;"
    cursor.execute(query)
    pendientes = cursor.fetchall()

    if not pendientes:
        # No hay valores pendientes
        return
    
    logging.info(f"Confirmando {len(pendientes)} cierres pendientes...")
    print(f"Confirmando {len(pendientes)} cierres pendientes")

    # Para cada par fecha, ticker
    for fecha, ticker in pendientes:
        try:
            # Calculamos el día siguiente para el rango de Yahoo
            fecha_dt = datetime.strptime(str(fecha), '%Y-%m-%d')
            fecha_fin = (fecha_dt + timedelta(days=1)).strftime('%Y-%m-%d')

            accion = yf.Ticker(ticker)
            # Pedimos el histórico del día
            df_confirmar = accion.history(start=fecha.strftime('%Y-%m-%d'), end=fecha_fin)

            if not df_confirmar.empty:
                # Obtenemos el precio oficial
                precio_oficial = float(df_confirmar['Close'].iloc[0])
                # Actualizamos la base de datos
                update_query = "UPDATE historico_ibex SET precio_cierre=%s, confirmado=1 WHERE fecha=%s AND ticker=%s;"
                cursor.execute(update_query, (precio_oficial, fecha, ticker))
                logging.info(f"{ticker} [{fecha}] actualizado a precio oficial")
                print(f"{ticker} [{fecha}] actualizado a precio oficial")

        except Exception as e:
            logging.error(f"No se pudo confirmar ticker {ticker}: {e}")
        finally:
            time.sleep(0.5)
        
    conn.commit()

if __name__ == "__main__":
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        revisar_pendientes(cursor, conn)
        cursor.close()
        conn.close()
    except Exception as e:
        print("Error en la conexión de la base de datos")