#!/usr/bin/env python
from threading import Thread

import telebot
import pika
import logging

from telebot import types
from telebot.apihelper import ApiException

import dropbox_api
import mysql_config

# Objeto del bot
bot = telebot.TeleBot("token")
channel = None
connection = None


# Envía mensaje de bienvenida
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    try:
        bot.reply_to(message,
                     "Bienvenido a SD TrackBot. Introduce tus tracking para recibir alertas cuando se muevan. /lista devuelve su lista de trackings y /borrar le permite borrar un tracking")
    except ApiException as e:
        print("No se pudo enviar el mensaje al usuario: " + str(e))


@bot.message_handler(commands=['borrar'])
def borrar_tracking(message):
    tracking = str(message.text).split()[1:]

    if (len(tracking) > 0):

        if not mysql_config.comprobar_tracking(tracking[0], message.chat.id):
            mysql_config.borrar_tracking(tracking[0])
            bot.reply_to(message, "Tracking borrado")

        else:
            bot.reply_to(message, "Error al borrar")

    else:
        markup = types.ReplyKeyboardMarkup()

        # Carga los trackings del usuario
        tracks = mysql_config.obtener_trackings_chat_id(message.chat.id)
        if (len(tracks) < 1):
            bot.reply_to(message, "No hay trackings a borrar")
        else:
            for item in tracks:
                markup.add(types.InlineKeyboardButton("/borrar " + str(item[0])))
            bot.reply_to(message, "Seleccione tracking", reply_markup=markup)


# Envía los datos cuando se solicita la lista
@bot.message_handler(commands=['lista'])
def enviar_lista(message):
    # Carga los trackings del usuario
    tracks = mysql_config.obtener_trackings_chat_id(message.chat.id)
    texto = None
    if (tracks == None):
        texto = "No dispone de trackings registrados"
    else:
        texto = "\U0001F426Su lista de trackings\U0001F426\n"
        for item in tracks:
            texto += "-  \U0001F4E6 " + item[0] + "\n         "
            texto += mysql_config.obtener_ultimo_estado(item[0])
            texto += "\n\n"

    bot.reply_to(message, texto)


# Responde a los mensajes de los usuarios
@bot.message_handler()
def recibir_mensaje(message):
    if (message.content_type == 'text'):
        logging.info("Mensaje recibido: " + str(message.text) + " de " + str(message.chat.username) + " (" + str(
            message.chat.first_name) + " " + str(message.chat.last_name) + ")")

        # Si no está el track, se añade y se procesa el siguiente mensaje
        if mysql_config.anadir_track(message.chat.id, str(message.text)):
            bot.reply_to(message, "Por favor, espere")
            channel.basic_publish(exchange='', routing_key='process_queue', body=message.text)
        else:
            # Si está, se notifica al usuario de que el tracking existe en el sistema
            bot.send_message(message.chat.id, "El track " + str(
                message.text) + " ya se encuentra registrado en el sistema. Contacte con el vendedor si no es suyo")
            logging.debug("Track registrado: " + str(message.text))


# Abre un canal cuando se conecta a RabbitMQ
def on_connected(connection):
    connection.channel(1, on_channel_open)
    logging.info("Conectado a RabbitMQ")


# Cuando el canal está abierto, registra las colas
def on_channel_open(new_channel):
    """Called when our channel has opened"""
    global channel
    channel = new_channel
    channel.queue_declare(queue='process_queue')
    channel.queue_declare(queue='processed_queue')
    channel.queue_declare(queue='errors_queue')
    logging.info("RabbitMQ listo")


# Función principal del lector
def main():
    # Sube el ultimo log
    dropbox_api.subir_log("reader-app.log")

    # Configura el log
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler("{0}/{1}.log".format(".", "reader-app")),
            logging.StreamHandler()
        ])

    # Abre la conexión
    parameters = pika.ConnectionParameters('localhost')
    connection = pika.SelectConnection(parameters, on_connected)

    # Se utiliza para que la conexión con el servidor RabbitMQ no se cierre mientras la aplicación está en ejecución
    Thread(target=connection.ioloop.start).start()

    # Mantiene el programa a la espera de un mensaje de telegram
    logging.info("Bot arrancado")
    bot.polling()


if __name__ == '__main__':
    main()
