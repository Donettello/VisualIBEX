import datetime
import logging
import time
import os
import yfinance as yf

from manejador_datos import manejador_csv
from config import RUTA_PRUEBA, RUTA_ULTIMA_SESION, TICKERS_LISTA, EMPRESAS_IBEX

def guardar_registro():
    ahora = datetime.datetime.now()
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
        # Obtener la fecha REAL de la  sesión de bolsa
        fecha_sesion = hist.index[-1].strftime('%Y-%m-%d')

        # Obtener la fecha del sistema
        fecha_sistema = datetime.datetime.now().strftime('%Y-%m-%d')

        ruta_txt = "/home/donettello/Documents/VisualIBEX/ultima_sesion.txt"

        if os.path.exists(ruta_txt):
            with open(ruta_txt, 'r') as f:
                fecha_ultima = f.read().strip() # .strip() quita espacios o saltos de línea invisibles
        else:
            fecha_ultima = "" # Si el archivo no existe, lo tratamos como vacío

        if fecha_sesion == fecha_sistema and fecha_sesion != fecha_ultima:
            # Hoy toca trabajar
            for t in TICKERS_LISTA:
                print(f"Obteniendo datos de la bolsa para la empresa {EMPRESAS_IBEX[t]}")
                logging.info(f"Obteniendo datos de la bolsa para la empresa {EMPRESAS_IBEX[t]}")
                time.sleep(1.5)

                # Capturamos el precio de Inditex
                accion = yf.Ticker(t)
                hist = accion.history(period="5d")

                try:
                    # Extraemos los precios
                    precio_apertura = hist['Open'].iloc[-1]
                    precio_cierre_hoy = hist['Close'].iloc[-1]
                    precio_cierre_ayer = hist['Close'].iloc[-2]

                    rentabilidad_dia = ((precio_cierre_hoy-precio_cierre_ayer)/precio_cierre_ayer) * 100
                    rentabilidad_sesion = ((precio_cierre_hoy-precio_apertura)/precio_apertura) * 100
                except Exception as e:
                    logging.error(f"Error con ticker {t}:{e}")

                # Preparamos el dato
                nuevo_dato = {
                    'Fecha': fecha_sesion,
                    'Ticker': t,
                    'Precio inicio sesion': round(precio_apertura, 4),
                    'Precio final sesion': round(precio_cierre_hoy, 4),
                    'Rentabilidad sesion (%)': round(rentabilidad_sesion, 4),
                    'Rentabilidad diaria (%)': round(rentabilidad_dia,4)
                }

                # Ruta de guardado de los datos
                manejador_csv(nuevo_dato, RUTA_PRUEBA, max_registros=20)
                logging.info(f"Ticker {t} introducido correctamente.")
            
            # Actualizar fichero de control
            with open(RUTA_ULTIMA_SESION, 'w') as f:
                f.write(fecha_sistema)
            
            guardar_registro()
            
            print(f"Ficheros actuaalizados")
        else:
            print("Hoy no toca")
    

if __name__ == "__main__":
    capturar_cierre()