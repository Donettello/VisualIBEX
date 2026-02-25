import datetime
import os
import yfinance as yf

from manejador_datos import manejador_csv

def guardar_registro():
    ahora = datetime.datetime.now()
    mensaje = f"Se ha ejecutado el código el: {ahora.strftime('%d-%m-%Y %H:%M:%S')}"

    with open("/home/donettello/Documents/VisualIBEX/ibex_log.txt", 'a') as f:
        f.write(mensaje + "\n")
    
    print(mensaje)

def capturar_cierre():
    print("--- Obteniendo datos de la bolsa ---")
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
            print(f"Obteniendo datos de la bolsa")

            # Capturamos el precio de Inditex
            ticket = "ITX.MC"
            accion = yf.Ticker(ticket)
            hist = accion.history(period="2d")

            # Extraemos los precios
            precio_apertura = hist['Open'].iloc[-1]
            precio_cierre_hoy = hist['Close'].iloc[-1]
            precio_cierre_ayer = hist['Close'].iloc[-2]

            rentabilidad_dia = ((precio_cierre_hoy-precio_cierre_ayer)/precio_cierre_ayer) * 100
            rentabilidad_sesion = ((precio_cierre_hoy-precio_apertura)/precio_apertura) * 100

            # Preparamos el dato
            nuevo_dato = {
                'Fecha': fecha_sesion,
                'Precio inicio sesion': round(precio_apertura, 4),
                'Precio final sesion': round(precio_cierre_hoy, 4),
                'Rentabilidad sesion': round(rentabilidad_sesion, 4),
                'Rentabilidad diaria': round(rentabilidad_dia,4)
            }

            # Ruta de guardado de los datos
            ruta_csv = "/home/donettello/Documents/VisualIBEX/data/datos_inditex.csv"
            manejador_csv(nuevo_dato, ruta_csv, max_registros=20)
            
            # Actualizar fichero de control
            with open("/home/donettello/Documents/VisualIBEX/ultima_sesion.txt", 'w') as f:
                f.write(fecha_sistema)
            
            #  Control de crontab
            ahora = datetime.datetime.now()
            mensaje = f"Se ha ejecutado el código el: {ahora.strftime('%d-%m-%Y %H:%M:%S')}"

            with open("/home/donettello/Documents/VisualIBEX/ibex_log.txt", 'a') as f:
                f.write(mensaje + "\n")
            
            print(f"Ficheros actuaalizados")
        else:
            print("Hoy no toca")
    

if __name__ == "__main__":
    capturar_cierre()