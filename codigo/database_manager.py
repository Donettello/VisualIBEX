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
            VALUES (%(fecha)s, %(ticker)s, %(p_ape)s, %(p_cie)s, %(r_ses)s, %(r_dia)s, %(conf)s)
            ON DUPLICATE KEY UPDATE precio_cierre=%(p_cie)s, rent_sesion=%(r_ses)s, rent_diaria=%(r_dia)s
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
        return float(resultado[0]) if resultado else None
    
    def obtener_pendientes(self):
        conn = self._conectar()
        cursor = conn.cursor()
        sql = f"SELECT fecha, ticker, precio_apertura FROM {self.tabla_hist} WHERE confirmado=0;"
        cursor.execute(sql)
        pendientes = cursor.fetchall()
        cursor.close()
        conn.close()
        return pendientes
    
    def obtener_ultimo_cierre_confirmado(self, ticker, fecha):
        conn = self._conectar()
        cursor = conn.cursor()
        sql = f"SELECT precio_cierre FROM {self.tabla_hist} WHERE ticker=%s AND fecha<%s ORDER BY fecha DESC LIMIT 1"
        cursor.execute(sql, (ticker, fecha))
        resultado = cursor.fetchone()
        cursor.close()
        conn.close()
        return float(resultado[0]) if resultado else None
    
    def actualizar_cierre(self, datos_actual):
        conn = self._conectar()
        cursor = conn.cursor()
        sql = f"UPDATE {self.tabla_hist} SET precio_cierre=%(p_fin)s, rent_sesion=%(r_ses)s, rent_diaria=%(r_dia)s, confirmado=1 WHERE fecha=%(fecha)s AND ticker=%(ticker)s"
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