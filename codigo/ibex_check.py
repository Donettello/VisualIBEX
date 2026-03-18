import logging
import mysql.connector
import time
import os
import yfinance as yf

from database_manager import DatabaseManager
from datetime import datetime
from dotenv import load_dotenv
from manejador_datos import manejador_csv, revisar_pendientes
from config import TICKERS_LISTA
from notifier import Notifier

load_dotenv()
db_manager = DatabaseManager()
notificador = Notifier()

logging.basicConfig(
    filename=os.getenv('RUTA_LOG'),
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
        msg = f"Error al conectar: {e}"
        notificador.enviar_alerta(msg)
        print(msg)
    
    hist = accion.history(period="1d")

    if not hist.empty:
        # Obtener la fecha REAL de la última sesión de bolsa
        fecha_ultima_sesion = hist.index[-1].strftime('%Y-%m-%d')

        # Obtener la fecha del sistema
        fecha_sistema = datetime.now().strftime('%Y-%m-%d')

        ruta_ultima_sesion = os.getenv('RUTA_ULTIMA_SESION')

        if os.path.exists(ruta_ultima_sesion):
            with open(ruta_ultima_sesion, 'r') as f:
                fecha_ultima_registrada = f.read().strip() # .strip() quita espacios o saltos de línea invisibles
        else:
            fecha_ultima_registrada = "" # Si el archivo no existe, lo tratamos como vacío

        if fecha_ultima_sesion == fecha_sistema and fecha_ultima_sesion != fecha_ultima_registrada:
            # Hoy toca trabajar
            captura_diaria()
            
        else:
            print("Hoy no toca")
            logging.info("Hoy no ha habido sesión")

def captura_diaria():
    fecha_hoy = datetime.now().strftime('%Y-%m-%d')

    # Revisamos que no hay precios de cierre pendientes
    revisar_pendientes()
    sw_error = False

    for t in TICKERS_LISTA:
        time.sleep(0.5)
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
            p_cierre_ayer = db_manager.obtener_ultimo_cierre(t)

            if p_cierre_ayer is None:
                # Ha fallado la base de datos
                hist_old = accion.history(period='5d')
                p_cierre_ayer = hist_old['Close'].iloc[-2]

            # Calculamos las rentabilidades
            rent_sesion = ((p_cierre_hoy - p_apertura) / p_apertura) * 100
            rent_diaria = ((p_cierre_hoy - p_cierre_ayer) / p_cierre_ayer) * 100

            # --- NUEVA SECCIÓN: GUARDADO EN MARIA DB ---
            # Hacemos un diccionario para insertarlo en la base de datos
            valores_db = {
                'fecha': fecha_hoy,
                'ticker': t,
                'p_ape': round(float(p_apertura), 4),
                'p_cie': round(float(p_cierre_hoy), 4),
                'r_ses': round(float(rent_sesion), 4),
                'r_dia': round(float(rent_diaria), 4),
                'conf': confirmado
            }

            db_manager.guardar_precio_diario(valores_db)

            # Preparamos el dato
            nuevo_dato = {
                'Fecha': fecha_hoy,
                'Ticker': t,
                'Precio apertura': round(p_apertura, 4),
                'Precio cierre': round(p_cierre_hoy, 4),
                'Rentabilidad sesion (%)': round(rent_sesion, 4),
                'Rentabilidad diaria (%)': round(rent_diaria,4),
                'Confirmado': confirmado
            }

            # Ruta de guardado de los datos
            # manejador_csv(nuevo_dato, RUTA_CSV_MAESTRO, max_registros=20)
            manejador_csv(nuevo_dato, os.getenv('RUTA_CSV_MAESTRO'), max_registros=20)
            logging.info(f"Ticker {t} introducido correctamente.")

        except Exception as e:
            msg = f"Error con Ticker {t}: {e}"
            logging.error(msg)
            notificador.enviar_alerta("Desde checkeador del IBEX:\n" + msg)
            sw_error = True
            continue
        
    if not sw_error:
        # Actualizar fichero de control
        with open(os.getenv('RUTA_ULTIMA_SESION'), 'w') as f:
            f.write(datetime.now().strftime('%Y-%m-%d'))
                    
        print(f"Ficheros actualizados")
    return

if __name__ == "__main__":
    capturar_cierre()
