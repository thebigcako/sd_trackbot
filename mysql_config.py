#!/usr/bin/env python
import mysql.connector
import logging
import time

mydb = None
dbcursor = None


# Inicializa las conexiones a mysql. Se llama cada vez que se quiere acceder,
# si se deja abierta los datos se "cachean", principalmente por no cerrar el cursor
def mysql_init():
    global mydb, dbcursor, connection
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="sd",
            passwd="sd1819",
            database="sd",
        )

        dbcursor = mydb.cursor()
        mydb.autocommit = True

    except Exception as e:
        logging.error("Error al conectar al servidor mysql: " + str(e))
        exit(1)

    logging.info("Conexión a servidor mysql correcta")


# Cierra el cursor y la conexión a mysql
def mysql_end():
    if mydb.is_connected():
        mydb.close()

        dbcursor.close()


# Comprueba si un tracking está registrado
def comprobar_tracking(track, usuario=None):
    mysql_init()

    if usuario == None:
        sql = "SELECT COUNT(*) FROM trackings WHERE tracking = %s"
        dbcursor.execute(sql, (track,))
    else:
        sql = "SELECT COUNT(*) FROM trackings WHERE tracking = %s AND chat_id = %s"
        dbcursor.execute(sql, (track, usuario))

    # Devuelve una array de tuplas de un solo elemento, pero cada tupla tambien tiene solo un elemento :)
    cuenta = dbcursor.fetchall()[0][0] == 0

    mysql_end()

    return cuenta


# Añade un tracking a la base datos, con el id del usuario que lo inserta
def anadir_track(chat_id: int, track):
    # Comprueba primero que no se haya registrado
    if (comprobar_tracking(track)):
        logging.info("Añadiendo " + track + " a la base de datos")
        try:
            # Registra el tracking en la bd
            mysql_init()
            sql = "INSERT INTO trackings (chat_id, tracking) VALUES (%s, %s)"
            val = (chat_id, track)
            dbcursor.execute(sql, val)

            mydb.commit()
            mysql_end()
            logging.info("Añadido " + track)
            return True
        except:
            # Muestra error de track
            logging.error("No se pudo añadir el track " + track + " (chat_id: " + str(chat_id) + " )")
    else:
        logging.warn("Track ya añadido")
        return False


# Obtiene el chat_id de un usuario dado un track registrado por él
def obtener_chatid_track(track):
    logging.info("Cargando chat_id del track" + str(track))
    mysql_init()

    sql = "SELECT chat_id FROM trackings WHERE tracking = %s"
    dbcursor.execute(sql, (track,))

    chat_id = dbcursor.fetchall()


    if (len(chat_id) == 0):
        raise Exception("No existe el track")

    mysql_end()

    return chat_id[0][0]


# Obtiene todos los trackings de un usuario
def obtener_trackings_chat_id(chat_id):
    trackings = None
    mysql_init()
    logging.info("Cargando los trackings del chat_id " + str(chat_id))

    try:

        sql = "SELECT tracking FROM trackings WHERE chat_id = %s"
        dbcursor.execute(sql, (chat_id,))

        tr = dbcursor.fetchall()
        # Si no hay tracking, salta
        if (len(tr) == 0):
            raise Exception("No hay trackings registrados")

        trackings = tr


        mysql_end()

    except Exception as e:
        pass
    finally:
        mysql_end()

    return trackings


# Obtiene el id de un tracking
def obtener_track_id(track):
    mysql_init()

    sql = "SELECT id FROM trackings WHERE tracking = %s"
    dbcursor.execute(sql, (track,))

    if (dbcursor.rowcount == 0):
        return "No hay estado registrado aún"

    track_id = dbcursor.fetchall()[0][0]

    mysql_end()

    return track_id


# Obtiene el último estado de un tracking
def obtener_ultimo_estado(track):
    track_id = obtener_track_id(track)

    mysql_init()

    sql = "SELECT fecha_actualizacion, estado FROM tracking_status WHERE track_id = %s ORDER BY fecha DESC LIMIT 1 "
    dbcursor.execute(sql, (track_id,))

    tupla = dbcursor.fetchall()

    mysql_end()

    # Si no hay datos, devuelve no hay estado registrado aún
    if (len(tupla) == 0):
        return "No hay estado registrado aún"
    else:
        # La fecha de actualizacion puede ser nula
        fecha = tupla[0][0]
        if fecha == None:
            return tupla[0][1]
        else:
            return fecha + " - " + tupla[0][1]


# Obtiene los trackings desactualizados desde hace tiempo
def obtener_trackings_desactualizados(tiempo):
    tracks = None
    mysql_init()

    sql = "SELECT DISTINCT tracking FROM tracking_status t, trackings tr WHERE ( tr.id = t.id AND (SELECT fecha FROM tracking_status WHERE id = t.id ORDER BY fecha DESC LIMIT 1 ) < FROM_UNIXTIME(%s)) OR (tr.id NOT IN (SELECT track_id FROM tracking_status))"

    dbcursor.execute(sql, (int(time.time()) - tiempo,))

    tupla = dbcursor.fetchall()

    mysql_end()

    # Si hay trackings, los inserta
    if (len(tupla) > 0):
        tracks = [t[0] for t in tupla]

    return tracks


# Borra un tracking de la base de datos, suponiendo que existe
def borrar_tracking(track):
    logging.info("Añadiendo " + track + " a la base de datos")
    try:
        # Registra el tracking en la bd
        mysql_init()
        sql = "DELETE FROM trackings WHERE tracking = %s"
        dbcursor.execute(sql, (track,))

        mydb.commit()
        mysql_end()
        logging.info("Elimiando " + track)
        return True
    except:
        # Muestra error de track
        logging.error("No se pudo borrar el track " + track)
