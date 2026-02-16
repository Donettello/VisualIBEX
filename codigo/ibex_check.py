import datetime

def guardar_registro():
    ahora = datetime.datetime.now()
    mensaje = f"Se ha ejecutado el código el: {ahora.strftime('%d-%m-%Y %H:%M:%S')}"

    with open("/home/pi/Documents/VisualIBEX/ibex_log.txt", 'a') as f:
        f.write(mensaje + "\n")
    
    print(mensaje)

if __name__ == "__main__":
    guardar_registro()