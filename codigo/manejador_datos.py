import mysql.connector
import os
import logging
import pandas as pd
import time
import yfinance as yf

from database_manager import DatabaseManager
from datetime import datetime, timedelta
from notifier import Notifier

db_manager = DatabaseManager()
notificador = Notifier()

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

def revisar_pendientes():
    # Construimos la query para obtener los que quedan pendientes de confirmación
    pendientes = db_manager.obtener_pendientes()

    if not pendientes:
        # No hay valores pendientes
        logging.info("Todo está confirmado")
        return
    
    logging.info(f"Confirmando {len(pendientes)} cierres pendientes...")
    print(f"Confirmando {len(pendientes)} cierres pendientes")

    # Para cada par fecha, ticker
    for fecha, ticker, p_apertura in pendientes:
        try:
            # Calculamos el día siguiente para el rango de Yahoo
            fecha_dt = datetime.strptime(str(fecha), '%Y-%m-%d')
            fecha_fin = (fecha_dt + timedelta(days=1)).strftime('%Y-%m-%d')

            accion = yf.Ticker(ticker)
            # Pedimos el histórico del día
            df_confirmar = accion.history(start=fecha.strftime('%Y-%m-%d'), end=fecha_fin)

            if not df_confirmar.empty:
                # 1. El nuevo precio oficial
                p_cierre_oficial = float(df_confirmar['Close'].iloc[0])

                res_ayer = db_manager.obtener_ultimo_cierre_confirmado(ticker, fecha)
                # res_ayer = cursor.fetchone()

                if res_ayer:
                    p_cierre_anterior = float(res_ayer[0])

                    # Recalcular rentabilidades
                    nueva_rent_sesion = ((p_cierre_oficial - p_apertura) / p_apertura) * 100
                    nueva_rent_diaria = ((p_cierre_oficial - p_cierre_anterior) / p_cierre_anterior) * 100

                    valores = {
                        'p_fin': p_cierre_oficial,      # Corresponde a %(p_fin)s
                        'r_ses': nueva_rent_sesion,     # Corresponde a %(r_ses)s (rent_sesion)
                        'r_dia': nueva_rent_diaria,     # Corresponde a %(r_dia)s (rent_diaria)
                        'fecha': fecha,                 # Corresponde a %(fecha)s
                        'ticker': ticker                # Corresponde a %(ticker)s
                    }
                    
                    try:
                        db_manager.actualizar_cierre(valores)
                        
                        logging.info(f"{ticker} [{fecha}] actualizado a precio oficial")
                        print(f"{ticker} [{fecha}] actualizado a precio oficial")
                    except Exception as e:
                        msg = f"Error al actualizar {ticker}: {e}"
                        logging.error(msg)
                        notificador.enviar_alerta(msg, "ERROR")
                        continue
                else:
                    msg = f"No hay sesión previa para {ticker} el {fecha}"
                    logging.warning("Desde manejador de datos:\n" + msg)
                    notificador.enviar_alerta(msg)
                    

        except Exception as e:
            msg = f"No se pudo confirmar ticker {ticker}: {e}"
            logging.error(msg)
            notificador.enviar_alerta("Desde manejador de datos:\n" + msg, "ERROR")
        finally:
            time.sleep(0.5)
    
    return
        

if __name__ == "__main__":
    '''
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        revisar_pendientes(cursor, conn)
        cursor.close()
        conn.close()
    except Exception as e:
        print("Error en la conexión de la base de datos")'''
    pass