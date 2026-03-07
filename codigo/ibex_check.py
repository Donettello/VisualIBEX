import logging
import mysql.connector
import time
import os
import yfinance as yf

from datetime import datetime
from manejador_datos import manejador_csv, revisar_pendientes
from config import RUTA_PRUEBA, RUTA_ULTIMA_SESION, TICKERS_LISTA, RUTA_LOG, DB_CONFIG

logging.basicConfig(
    filename=RUTA_LOG,
    filemode='w', # Para controlar mejor  lo que ocurre cada día
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def guardar_registro():
    ahora = datetime.now()
    mensaje = f"Se ha ejecutado el código el: {ahora.strftime('%d-%m-%Y %H:%M:%S')}"

    with open("/home/donettello/Documents/VisualIBEX/ibex_log.txt", 'a') as f:
        f.write(mensaje + "\n")
    
    print(mensaje)

def capturar_cierre():
    print("--- Obteniendo datos de la bolsa ---")
    # Usamos Inditex para saber si hoy ha habido sesión
    tickers = "ITX.MC" # Inditex
    accion = yf.Ticker(tickers)
    
    try:
        # Intentamos descargar el nombre y el último precio
        _ = accion.info.get('longName', 'Inditex')
        print(f"Conexión exitosa")
    except Exception as e:
        print(f"Error al conectar: {e}")
    
    hist = accion.history(period="1d")

    if not hist.empty:
        # Obtener la fecha REAL de la última sesión de bolsa
        fecha_ultima_sesion = hist.index[-1].strftime('%Y-%m-%d')

        # Obtener la fecha del sistema
        fecha_sistema = datetime.now().strftime('%Y-%m-%d')

        ruta_txt = "/home/donettello/Documents/VisualIBEX/ultima_sesion.txt"

        if os.path.exists(ruta_txt):
            with open(ruta_txt, 'r') as f:
                fecha_ultima_registrada = f.read().strip() # .strip() quita espacios o saltos de línea invisibles
        else:
            fecha_ultima_registrada = "" # Si el archivo no existe, lo tratamos como vacío

        if fecha_ultima_sesion == fecha_sistema and fecha_ultima_sesion != fecha_ultima_registrada:
            # Hoy toca trabajar
            captura_diaria()
            
            # Actualizar fichero de control
            with open(RUTA_ULTIMA_SESION, 'w') as f:
                f.write(fecha_sistema)
                        
            print(f"Ficheros actualizados")
        else:
            print("Hoy no toca")
            logging.info("Hoy no ha habido sesión")
    
def obtener_ultimo_cierre_db(cursor, ticker):
    '''Obtiene el último precio_cierre registrado en la BD del ticker'''
    query = "SELECT precio_cierre FROM historico_ibex WHERE ticker=%s ORDER BY fecha DESC LIMIT 1;"
    cursor.execute(query, (ticker, ))
    resultado = cursor.fetchone()
    return float(resultado[0]) if resultado else None

def captura_diaria():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Revisamos que no hay precios de cierre pendientes
    revisar_pendientes(cursor, conn)

    for t in TICKERS_LISTA:
        try:
            accion = yf.Ticker(t)

            # Intento de capturar el historiaal del día
            hist = accion.history(period='1d')

            if not hist.empty and not str(hist['Close'].iloc[-1]) == 'nan':
                p_apertura = hist['Open'].iloc[-1]
                p_cierre_hoy = hist['Close'].iloc[-1]
                confirmado = 1
            else:
                # No se ha cerrado la sesión aún
                logging.warning(f"Obteniendo datos en vivo de ticker {t}")
                info = accion.fast_info
                p_apertura = info['open']
                p_cierre_hoy = info['last_price']
                confirmado = 0

            # Obtenemos nuestro precio de ayer de la base de datos
            p_cierre_ayer = obtener_ultimo_cierre_db(cursor, t)

            if p_cierre_ayer is None:
                # Ha fallado la base de datos
                hist_old = accion.history(period='5d')
                p_cierre_ayer = hist_old['Close'].iloc[-2]

            # Calculamos las rentabilidades
            rent_sesion = ((p_cierre_hoy - p_apertura) / p_apertura) * 100
            rent_diaria = ((p_cierre_hoy - p_cierre_ayer) / p_cierre_ayer) * 100

            # Preparamos el dato
            nuevo_dato = {
                'Fecha': datetime.now().strftime('%Y-%m-%d'),
                'Ticker': t,
                'Precio apertura': round(p_apertura, 4),
                'Precio cierre': round(p_cierre_hoy, 4),
                'Rentabilidad sesion (%)': round(rent_sesion, 4),
                'Rentabilidad diaria (%)': round(rent_diaria,4),
                'Confirmado': confirmado
            }

            # Ruta de guardado de los datos
            manejador_csv(nuevo_dato, RUTA_PRUEBA, max_registros=20)
            logging.info(f"Ticker {t} introducido correctamente.")

        except Exception as e:
            logging.error(f"Error con Ticker {t}: {e}")
            continue

    cursor.close()
    conn.close()
    return

if __name__ == "__main__":
    capturar_cierre()
