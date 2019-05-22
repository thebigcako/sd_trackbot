import pika
import telebot
import logging

import dropbox_api
import mysql_config

channel = None
bot = telebot.TeleBot("token")


# Abre un canal cuando se conecta a RabbitMQ
def on_connected(connection):
    connection.channel(1, on_channel_open)
    logging.info("Conectado")


# Cuando el canal está abierto, consume las colas de pendiente de procesar y de errores
def on_channel_open(new_channel):
    global channel
    channel = new_channel
    channel.basic_consume('processed_queue', handle_delivery, auto_ack=True)
    channel.basic_consume('errors_queue', handle_delivery, auto_ack=True)
    logging.info("Listo")


# Procesa los mensajes entrantes de las colas
def handle_delivery(channel, method, header, body):
    logging.info("Recibido: " + body.decode('utf-8'))

    # Decodifica el mensaje
    track = body.decode('utf-8')

    # Determina de donde viene el mensaje
    if (method.routing_key == "errors_queue"):

        # Si es error manda que no se encontró información
        chat_id = mysql_config.obtener_chatid_track(track)
        bot.send_message(chat_id, "No se encontró información para el track " + str(track))
        logging.warn("Falta de actualización enviada a " + str(chat_id))

    else:
        # En caso de haber datos actualizados, manda mensaje con los datos actualizados
        logging.info("Datos del track: " + track + " refrescados")
        try:
            chat_id = mysql_config.obtener_chatid_track(track)
            actualizacion = mysql_config.obtener_ultimo_estado(track)
            bot.send_message(chat_id, "Actualización del track " + track + "\n - " + str(actualizacion))
            logging.info("Datos enviados")
        except Exception as e:
            logging.error("Error al enviar los datos: " + e)


# Función principal del escritor
def main():
    # Sube el log anterior
    dropbox_api.subir_log("writer-app.log" )

    # Configura el log
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler("{0}/{1}.log".format(".", "writer-app")),
            logging.StreamHandler()
        ])

    # Conecta con el servidor RabbitMQ local
    parameters = pika.ConnectionParameters('localhost')
    connection = pika.SelectConnection(parameters, on_connected)
    try:

        logging.info("INIT")

        # Mantiene el programa ocupado esperando que no se cierre la conexion a rabbitmq
        connection.ioloop.start()

    except KeyboardInterrupt:
        # Cierra la conexión a rabbitmq
        connection.close()
        # Espera que la conexión sea cerrada
        connection.ioloop.start()


if __name__ == '__main__':
    main()
