import config_privado
import os
import time
import yfinance as yf

from database_manager import DatabaseManager
from datetime import datetime
from config import EMPRESAS_IBEX
from notifier import Notifier

db_manager = DatabaseManager()
notificador = Notifier()

# Cargamos rutas y datos desde la DB y el ENV
RUTA_LOG = os.getenv('RUTA_LOG_INTEGRIDAD')
EMPRESAS = db_manager.obtener_diccionario_empresas()

def inicializar_log():
    """Borra el contenido del log anterior antes de empezar la sesión"""
    try:
        # Al abrir con 'w', el contenido anterior desaparece inmediatamente
        with open(RUTA_LOG, 'w', encoding='utf-8') as f:
            f.write(f"--- NUEVA EJECUCIÓN: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n")
    except Exception as e:
        print(f"No se pudo resetear el log: {e}")

def escribir_log(mensaje, nivel="INFO"):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    linea = f"{timestamp} - {nivel} - {mensaje}\n"
    try:
        # Aseguramos que la carpeta existe, si no, la creamos
        os.makedirs(os.path.dirname(RUTA_LOG), exist_ok=True)
        with open(RUTA_LOG, 'a', encoding='utf-8') as f:
            f.write(linea)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        print(f"Error escribiendo en el log: {e}")

def verificar_integridad_mercado():
    inicializar_log()
    print(f"Iniciando verificación de {len(EMPRESAS_IBEX)} tickers...")
    print(f"Escribiendo log en: {RUTA_LOG}")
    escribir_log("--- INICIO DE VERIFICACIÓN DIARIA ---")
    
    errores_detectados = 0
    discrepancias_nombre = 0
    
    for ticker, nombre_esperado in EMPRESAS_IBEX.items():
        #print(f"DEBUG para el ticker {ticker}")
        try:
            accion = yf.Ticker(ticker)
            # Si falla con Error 500, esperamos 1 segundo y reintentamos una vez.
            intentos = 0
            info = None
            while intentos < 2 and info is None:
                try:
                    info = accion.info
                except Exception:
                    intentos += 1
                    time.sleep(1) # Pausa de cortesía para el servidor de Yahoo

            if info is None:
                raise Exception("Yahoo devolvió error interno (500) repetidamente.") 
            
            # 1. Verificación de precio/existencia
            precio = info.get('regularMarketPrice') or info.get('currentPrice')
            
            if precio is None:
                msg = f"{ticker}: No devuelve precio (posible error de ticker)."
                escribir_log(msg, "ERROR")
                errores_detectados += 1
                continue

            # 2. Verificación de nombre
            nombre_oficial = info.get('longName', 'Desconocido')
            if nombre_esperado.lower() not in nombre_oficial.lower():
                msg = f"NOMBRE: {ticker} (Dict: {nombre_esperado} | Yahoo: {nombre_oficial})"
                escribir_log(msg, "WARNING")
                discrepancias_nombre += 1

        except Exception as e:
            msg = f"FALLO en {ticker}: {str(e)}"
            escribir_log(msg, "ERROR")
            errores_detectados += 1

    # Resumen final
    if errores_detectados > 0 or discrepancias_nombre > 0:
        resumen = f"Se han detectado {errores_detectados} errores y {discrepancias_nombre} discrepancias en el IBEX."
        # Si quieres detalles específicos
        notificador.enviar_alerta(resumen + "\nRevisa el log en la Raspberry para más detalles.")
        escribir_log("Alerta enviada a Telegram.")
    else:
        escribir_log("Todo correcto. Sin alertas.")
    
    escribir_log("--- FIN DE VERIFICACIÓN ---")

if __name__ == "__main__":
    verificar_integridad_mercado()
    # enviar_alerta_telegram("HABEMUS BOT")