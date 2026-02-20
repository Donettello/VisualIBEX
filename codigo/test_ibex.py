import yfinance as yf

def test():
    print("--- Diagnosticando yfinance ---")
    ticker = "ITX.MC" # Inditex
    accion = yf.Ticker(ticker)
    
    try:
        # Intentamos descargar el nombre y el último precio
        nombre = accion.info.get('longName', 'Inditex')
        precio = accion.fast_info['last_price']
        precio_limpio = round(precio, 2)
        print(f"Conexión exitosa con: {nombre}")
        print(f"Precio actual: {precio_limpio} EUR")
    except Exception as e:
        print(f"Error al conectar: {e}")

if __name__ == "__main__":
    test()