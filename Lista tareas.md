Pendientes de realizar:
* Comprobar programas necesarios
* Clonar repositorio.
* Para poder actualizar github, hacer commit y push.
* Añadir tarea a Cron. Dos opciones:
    * Primera opción:
        1. nano crontab -e
        2. 35 17 * * 1-5 /usr/bin/python3 /home/pi/Documents/VisualIBEX/codigo/ibex_check.py
        3. Ctrl+0
        4. Enter
        5. Ctrl+X
        6. Tiene que salir 'crontab: installing new crontab'
    * Segunda opción:
        1. Escribir 'echo "35 17 * * 1-5 /usr/bin/python3 /home/pi/Documents/VisualIBEX/codigo/ibex_check.py" > mis_tareas.txt'
        2. 'crontab mis_tareas.txt'
* Comprobar con 'crontab -l'
* Esperar que se ejecute cron con el código de 'ibex_check.py'
* 'pip install yfinance'

* Aquí ya podría modificar 'ibex_check.py' o hacer diferentes archivos '.py' para tener todo un poco modulado

NOTA: En crontab los valores son

\# Min Hora Día Mes Dia_sem Comando


