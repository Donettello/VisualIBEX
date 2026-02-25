import os
import pandas as pd
def manejador_csv(nueva_fila, ruta_csv, max_registros=20):
    """
        Guarda una fila en el CSV y mantiene los últimos X registros
        nueva_fila: dict con formato {'Fecha':..., 
                                      'Precio inicio sesion':..., 
                                      'Precio final sesion':..., 
                                      'Rentabilidad sesion':...,
                                      'Rentabilidad diaria':...}
    """
    # Comprobamos que la dirección del fichero es la correcta
    if os.path.exists(ruta_csv):
        df = pd.read_csv(ruta_csv, sep=';')
    else:
        # En caso de que  no exista
        df = pd.DataFrame(columns=['Fecha', 
                                   'Precio inicio sesion', 
                                   'Precio final sesion', 
                                   'Rentabilidad sesion', 
                                   'Rentabilidad diaria'])

    # Añadimos la nueva fila al DataFrame
    # Convertimos la nueva fila en un pequeño DataFrame
    df_nueva = pd.DataFrame([nueva_fila])
    df = pd.concat([df, df_nueva], ignore_index=True)

    # Mantenemos sólo los últimos registros
    if len(df) > max_registros:
        df = df.tail(max_registros)

    # Guardamos los datos en el CSV
    df.to_csv(ruta_csv, index=False, sep=';')
    print(f"Registro guardado. Total en archivo: {len(df)} lineas.")
    return

