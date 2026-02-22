import datetime
import yfinance as yf

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

        if fecha_sesion == fecha_sistema:
            print("Hoy toca trabajar")
        else:
            print("Hoy es festivo")
    

if __name__ == "__main__":
    capturar_cierre()