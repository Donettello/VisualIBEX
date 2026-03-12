# Rutas generales de los ficheros
RUTA_CSV_MAESTRO = "/home/donettello/Documents/VisualIBEX/data/datos_ibex_completo.csv"
RUTA_ULTIMA_SESION = "/home/donettello/Documents/VisualIBEX/ultima_sesion.txt"
RUTA_LOG = "/home/donettello/Documents/VisualIBEX/info.log"
RUTA_STATUS = "/home/donettello/Documents/VisualIBEX/status_tickers.log"
RUTA_PRUEBA = "/home/donettello/Documents/VisualIBEX/data/datos_ibex_prueba.csv"

# Configuración para conectarse a la base de datos
DB_CONFIG = {
    'host': 'localhost',
    'user': 'donettello',
    'password': 'Puesvaaseresta0',
    'database': 'visualibex_db'
}

# Tabla donde se guarda el histórico del IBEX35
TABLA_HISTORICO = 'historico_ibex'