import mysql.connector
import os
from dotenv import load_dotenv

# Cargamos las variables del archivo .env
load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASS'),
            'database': os.getenv('DB_NAME')
        }
        self.tabla_hist = os.getenv('TABLA_HISTORICO')
        self.tabla_info = os.getenv('TABLA_EMPRESAS') # Añadir a tu .env

    def _conectar(self):
        return mysql.connector.connect(**self.config)

    def guardar_precio_diario(self, datos_dict):
        """Recibe un diccionario con los datos y los inserta/actualiza"""
        conn = self._conectar()
        cursor = conn.cursor()
        sql = f"""
            INSERT INTO {self.tabla_hist} 
            (fecha, ticker, precio_apertura, precio_cierre, rent_sesion, rent_diaria, confirmado)
            VALUES (%(fecha)s, %(ticker)s, %(p_ini)s, %(p_fin)s, %(r_web)s, %(r_intra)s, %(conf)s)
            ON DUPLICATE KEY UPDATE precio_final=%(p_fin)s, rent_web=%(r_web)s, rent_intra=%(r_intra)s
        """
        cursor.execute(sql, datos_dict)
        conn.commit()
        cursor.close()
        conn.close()

    def obtener_ultimo_cierre(self, ticker):
        """Abstracción del SELECT"""
        conn = self._conectar()
        cursor = conn.cursor()
        sql = f"SELECT precio_cierre FROM {self.tabla_hist} WHERE ticker = %s ORDER BY fecha DESC LIMIT 1"
        cursor.execute(sql, (ticker,))
        resultado = cursor.fetchone()
        cursor.close()
        conn.close()
        return resultado[0] if resultado else None
    
    def actualizar_cierre(self, datos_actual):
        conn = self._conectar()
        cursor = conn.cursor()
        sql = f"UPDATE {self.tabla_hist} SET precio_cierre=%s, rent_sesion=%s, rent_diaria=%s, confirmado=1 WHERE fecha=%s AND ticker=%s"
        cursor.execute(sql, datos_actual)
        conn.commit()
        cursor.close()
        conn.close()
    
    # --- MÉTODOS PARA EMPRESAS_INFO ---
    def obtener_diccionario_empresas(self):
        """Carga el diccionario {ticker: nombre} desde la base de datos"""
        conn = self._conectar()
        cursor = conn.cursor()
        sql = f"SELECT ticker, nombre_empresa FROM {self.tabla_info}"
        cursor.execute(sql)
        # Convertimos el resultado en un diccionario de Python
        resultado = {ticker: nombre for (ticker, nombre) in cursor.fetchall()}
        cursor.close()
        conn.close()
        return resultado