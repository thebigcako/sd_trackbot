import mysql_config
import pika
import logging
import time

import dropbox_api

# Función principal del comprobador
def main():
    # Sube el ultimo log a dropbox y lo borra
    dropbox_api.subir_log("checker.log")

    # Configura el logging para que guarde el log en el archivo checker.log
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler("{0}/{1}.log".format(".", "checker")),
            logging.StreamHandler()
        ])


    # Indefinidamente comprueba trackings
    while (True):
        logging.info("Comprobando tracks...")

        # Obtiene los trackings no actualizados desde hace 30 minutos
        tracks = mysql_config.obtener_trackings_desactualizados(1800)
        logging.info("Tracks desactualizados: " + str(len(tracks)))

        # Abre la conexión a la cola
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Envía los tracks a la cola de pendiente de procesar
        for track in tracks:
            logging.info("Enviando track: " + track)
            channel.basic_publish(exchange='', routing_key='process_queue', body=track)

        # Cierra la cola
        channel.close()
        connection.close()

        # Espera 30 minutos
        time.sleep(1800)



if __name__ == '__main__':
     main()
