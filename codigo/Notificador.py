import requests
import os
from dotenv import load_dotenv

load_dotenv()
class Notifier:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    def enviar_alerta(self, mensaje, nivel="INFO"):
        icono = "⚠️" if nivel == "ERROR" else "ℹ️"
        payload = {
            'chat_id': self.chat_id,
            'text': f"{icono} *IBEX MONITOR*\n\n{mensaje}",
            'parse_mode': 'Markdown'
        }
        try:
            requests.post(self.url, data=payload, timeout=10)
        except Exception as e:
            print(f"Error enviando notificación: {e}")