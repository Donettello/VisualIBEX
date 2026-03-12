import mysql.connector
import os
from dotenv import load_dotenv

# Cargamos las variables del archivo .env
load_dotenv()

class GestorBD:
    def __init__(self):
        self.config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASS'),
            'database': os.getenv('DB_NAME')
        }
        self.tabla = os.getenv('TABLE_HISTORICO')
        self.tabla_info = os.getenv('TABLE_EMPRESAS_INFO') # Añadir a tu .env

    def _conectar(self):
        return mysql.connector.connect(**self.config)

    def insertar_registro_diario(self, fecha, ticker, apertura, cierre, rent_diaria, rent_intra, confirmado=0):
        """Abstracción del INSERT"""
        conn = self._conectar()
        cursor = conn.cursor()
        
        # Usamos f-string solo para el nombre de la tabla (que es seguro), 
        # pero %s para los valores (para evitar SQL Injection)
        sql = f"""
            INSERT INTO {self.tabla} 
            (fecha, ticker, precio_inicio, precio_final, rent_web, rent_intra, confirmado)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE precio_final=%s, rent_web=%s, rent_intra=%s
        """
        valores = (fecha, ticker, apertura, cierre, rent_diaria, rent_intra, confirmado, 
                   cierre, rent_diaria, rent_intra)
        
        cursor.execute(sql, valores)
        conn.commit()
        cursor.close()
        conn.close()

    def obtener_ultimo_cierre(self, ticker):
        """Abstracción del SELECT"""
        conn = self._conectar()
        cursor = conn.cursor()
        sql = f"SELECT precio_final FROM {self.tabla} WHERE ticker = %s ORDER BY fecha DESC LIMIT 1"
        cursor.execute(sql, (ticker,))
        resultado = cursor.fetchone()
        cursor.close()
        conn.close()
        return resultado[0] if resultado else None
    
    # --- MÉTODOS PARA EMPRESAS_INFO ---
    def obtener_diccionario_empresas(self):
        """Carga el diccionario {ticker: nombre} desde la base de datos"""
        conn = self._conectar()
        cursor = conn.cursor()
        sql = f"SELECT ticker, nombre FROM {self.tabla_info}"
        cursor.execute(sql)
        # Convertimos el resultado en un diccionario de Python
        resultado = {ticker: nombre for (ticker, nombre) in cursor.fetchall()}
        cursor.close()
        conn.close()
        return resultado