import yfinance as yf
import pandas as pd
from manejador_datos import manejador_csv
from config import RUTA_CSV_MAESTRO, TICKERS_LISTA

def poblar_historico(ticker = 'ITX.MC'):
    print(f"Descargando datos para {ticker}")

    # Obtenemos laa acción del ticker
    accion = yf.Ticker(ticker)

    # Obtenemos un poco más de 20 días para poder hacer cálculos
    hist = accion.history(period="1mo")

    if len(hist) <=20:
        print("Cantidad de datos insuficiente")
        return
    
    # Tomamos los últimos 20 registros disponibles
    indices_a_procesar = hist.index[-20:]

    for i in range(len(hist)):
        # Sólo procesamos si está dentro de los últimos 20 días
        fecha_actual = hist.index[i]
        if fecha_actual not in indices_a_procesar:
            continue

        # Extraemos los precios
        precio_apertura = hist['Open'].iloc[i]
        precio_cierre_hoy = hist['Close'].iloc[i]
        precio_cierre_ayer = hist['Close'].iloc[i-1]

        rentabilidad_dia = ((precio_cierre_hoy-precio_cierre_ayer)/precio_cierre_ayer) * 100
        rentabilidad_sesion = ((precio_cierre_hoy-precio_apertura)/precio_apertura) * 100

        # Preparamos el dato
        nuevo_dato = {
            'Fecha': fecha_actual.strftime('%Y-%m-%d'),
            'Precio inicio sesion': round(precio_apertura, 4),
            'Precio final sesion': round(precio_cierre_hoy, 4),
            'Rentabilidad sesion (%)': round(rentabilidad_sesion, 4),
            'Rentabilidad diaria (%)': round(rentabilidad_dia,4)
        }

        # Ruta de guardado de los datos
        ruta_csv = "/home/donettello/Documents/VisualIBEX/data/datos_inditex.csv"
        manejador_csv(nuevo_dato, ruta_csv, max_registros=20)

    print(f"\n¡Proceso completado! Guardadas las últimas 20 sesiones de {ticker}")

    return

def poblar_historico_ibex():

    for t in TICKERS_LISTA:
        print(f'Obteniendo datos para {t}')

        # Obtenemos laa acción del ticker
        accion = yf.Ticker(t)

        # Obtenemos un poco más de 20 días para poder hacer cálculos
        hist = accion.history(period="1mo")

        if len(hist) <=20:
            print("Cantidad de datos insuficiente")
            continue
        
        # Tomamos los últimos 20 registros disponibles
        indices_a_procesar = hist.index[-20:]

        for i in range(len(hist)):
            # Sólo procesamos si está dentro de los últimos 20 días
            fecha_actual = hist.index[i]
            if fecha_actual not in indices_a_procesar:
                continue

            # Extraemos los precios
            precio_apertura = hist['Open'].iloc[i]
            precio_cierre_hoy = hist['Close'].iloc[i]
            precio_cierre_ayer = hist['Close'].iloc[i-1]

            rentabilidad_dia = ((precio_cierre_hoy-precio_cierre_ayer)/precio_cierre_ayer) * 100
            rentabilidad_sesion = ((precio_cierre_hoy-precio_apertura)/precio_apertura) * 100

            # Preparamos el dato
            nuevo_dato = {
                'Fecha': fecha_actual.strftime('%Y-%m-%d'),
                'Ticker': t,
                'Precio inicio sesion': round(precio_apertura, 4),
                'Precio final sesion': round(precio_cierre_hoy, 4),
                'Rentabilidad sesion (%)': round(rentabilidad_sesion, 4),
                'Rentabilidad diaria (%)': round(rentabilidad_dia,4)
            }

            manejador_csv(nuevo_dato, RUTA_CSV_MAESTRO, max_registros=20)

    return

if __name__ == "__main__":
    poblar_historico_ibex()
