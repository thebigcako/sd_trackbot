# /usr/bin/env python

import dropbox
import logging
import time
import os


# Código que se encarga de subir los logs a dropbox


def subir_log(archivo):
    # Configura el log
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler("{0}/{1}.log".format(".", "reader-app")),
            logging.StreamHandler()
        ])

    try:
        # Lee el log
        with open(archivo, 'rb') as f:
            data = f.read()

        # Conecta a dropbox
        client = dropbox.Dropbox("token")
        print("Cuenta dropbox: " + str(client.users_get_current_account()))

        # Sube el archivo
        client.files_upload(data, "/" + str(time.time()) + "-" + archivo)

        logging.info("Subido archivo: " + archivo)

        os.remove(archivo)

    except:
        # Si ocurre algún error, ignora
        pass
