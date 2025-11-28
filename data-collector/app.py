import time

from flask import Flask, jsonify, request
import os
import pymysql.cursors
import requests
import grpc
import user_service_pb2
import user_service_pb2_grpc
from datetime import datetime, timezone

import threading

import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
)

app = Flask(__name__)

LISTEN_PORT = int(os.getenv("LISTEN_PORT", 5002))

GRPC_HOST = os.getenv("GRPC_HOST")
GRPC_PORT= int(os.getenv("GRPC_PORT", 50051))

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USERNAME = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

OPENSKY_CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")

OPENSKY_DEPARTURE_ENDPOINT = "https://opensky-network.org/api/flights/departure"
OPENSKY_ARRIVAL_ENDPOINT = "https://opensky-network.org/api/flights/arrival"
OPENSKY_TOKEN_ENDPOINT = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

TIMEOUT_SECONDS = 10

USER_MANAGER_ADDRESS = f"{GRPC_HOST}:{GRPC_PORT}"

def get_connection():
    try:
        mysql_conn = pymysql.connect(host=MYSQL_HOST,
                                     user=MYSQL_USERNAME,
                                     password=MYSQL_PASSWORD,
                                     database=MYSQL_DATABASE,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        return mysql_conn

    except pymysql.MySQLError as e:
        print(f"[ERRORE] Impossibile connettersi a MySQL: {e}")
        return None

# Verifica se l'utente esiste comunicando tramite il canale gRPC con user-manager
def check_if_exists(email: str) -> bool:
    with grpc.insecure_channel(USER_MANAGER_ADDRESS) as channel:
        stub = user_service_pb2_grpc.UserServiceStub(channel)

        response = stub.CheckIfUserExists(user_service_pb2.UserCheckRequest(email=email))

        return response.exists

def get_opensky_token():
    payload = {
        "grant_type": "client_credentials",
        "client_id": OPENSKY_CLIENT_ID,
        "client_secret": OPENSKY_CLIENT_SECRET
    }

    try:
        response = requests.post(OPENSKY_TOKEN_ENDPOINT, data=payload, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()

        data = response.json()

        return data.get("access_token")

    except requests.exceptions.RequestException as e:
        print(f"[ERRORE]: Non è stato possibile recuperare il token: {e}")
        return None

def get_user_airports(mysql_conn, email: str) -> list[str]:
    try:
        with mysql_conn.cursor() as cursor:
            # Recupero gli aeroporti di interesse dell'utente
            sql_get_aeroporti = "SELECT icao_aeroporto FROM user_airports WHERE email_utente = %s"
            cursor.execute(sql_get_aeroporti, (email, ))

            rows = cursor.fetchall()

            icao_list = []
            for row in rows:
                icao = row["icao_aeroporto"]
                icao_list.append(icao)

            return icao_list

    except pymysql.MySQLError as e:
        return []

def get_begin_unix_time(days) -> int:
    current_time_utc = datetime.now(timezone.utc)

    current_time_timestamp = int(current_time_utc.timestamp())

    days_in_seconds = days * 24 * 60 * 60

    return current_time_timestamp - days_in_seconds

def get_end_unix_time() -> int:
    return int(time.time())

def update_flights(mysql_conn, email_utente, opensky_endpoint, token, days):
    if token:
        icao_list = get_user_airports(mysql_conn, email_utente)

        headers = {
            "Authorization": f"Bearer {token}"
        }

        begin = get_begin_unix_time(days)
        end = get_end_unix_time()

        for icao in icao_list:
            params = {
                "airport": icao,
                "begin": begin,
                "end": end
            }

            try:
                response = requests.get(opensky_endpoint, params=params, headers=headers)
                response.raise_for_status()

                data_flights = response.json()

                # Aggiorno i voli da ogni aeroporto
                if data_flights:
                    for flights in data_flights:
                        icao_aereo = flights.get("icao24")
                        first_seen = flights.get("firstSeen")
                        aeroporto_partenza = flights.get("estDepartureAirport")
                        last_seen = flights.get("lastSeen")
                        aeroporto_arrivo = flights.get("estArrivalAirport")

                        logging.info(f"Opensky {opensky_endpoint} risposta: {icao_aereo}, {first_seen}, {aeroporto_partenza}, {last_seen}, {aeroporto_arrivo}")

                        with mysql_conn.cursor() as cursor:
                            try:
                                sql_flights = ("INSERT IGNORE INTO flight (icao_aereo, first_seen, aeroporto_partenza, "
                                                "last_seen, aeroporto_arrivo) VALUES (%s, %s, %s, %s, %s)")
                                cursor.execute(sql_flights, (icao_aereo, first_seen, aeroporto_partenza, last_seen, aeroporto_arrivo))

                                mysql_conn.commit()

                            except pymysql.MySQLError as e:
                                mysql_conn.rollback()
                                logging.error(f"Errore MySQL {e}")
                                #return jsonify({"errore": f"MySQL: {e}"}), 500

            except requests.exceptions.RequestException as e:
                logging.error(f"Non è stato possibile recuperare i voli: {e}")
                #return jsonify({"errore": f"Non è stato possibile recuperare i voli: {e}"}), 502

    else:
        logging.error("Token OPENSKY non valido")
        #return jsonify({"errore": "token OPENSKY non valido"}), 401

def update_all_flights():
    mysql_conn = get_connection()

    if mysql_conn:
        token = get_opensky_token()

        if token:
            with mysql_conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT email_utente FROM user_airports")
                users = cursor.fetchall()

            for u in users:
                email = u["email_utente"]
                update_flights(mysql_conn, email, OPENSKY_DEPARTURE_ENDPOINT, token, 1)
                update_flights(mysql_conn, email, OPENSKY_ARRIVAL_ENDPOINT, token, 1)
        else:
            logging.error("Scheduler: impossibile connettersi al db")
            #return jsonify({"errore": "token OPENSKY non valido"}), 500

        mysql_conn.close()
    else:
        logging.error("Scheduler: impossibile connettersi al db")
        #return jsonify({"errore": "impossibile connettersi al db"}), 500

    logging.error("Scheduler: Voli aggiornati")
    #return jsonify({"message": "Voli aggiornati"}), 200

def scheduler_job():
    while True:
        print("[Scheduler] Aggiornamento voli in corso...")

        with app.app_context():
            update_all_flights()

        time.sleep(12 * 3600)   # ogni 12 orew

@app.route("/")
def home():
    return jsonify("Hello, Data Collector"), 200

@app.route("/user/interests", methods=["POST"])
def add_interest():
    data = request.json

    email_utente = data["email_utente"]
    aeroporti_icao = data['aeroporti_icao']

    try:
        if check_if_exists(email_utente):
            # Utente esiste
            mysql_conn = get_connection()

            # Inserisco gli aeroporti indicati dall'utente nella tabella airport
            if mysql_conn:
                try:
                    with mysql_conn.cursor() as cursor:
                        for icao in aeroporti_icao:

                            sql_interest = "INSERT IGNORE INTO user_airports (email_utente, icao_aeroporto) VALUES (%s, %s)"
                            cursor.execute(sql_interest, (email_utente, icao))

                    mysql_conn.commit()

                except pymysql.MySQLError as e:
                    mysql_conn.rollback()
                    return jsonify(f"[ERRORE] Non è stato possibile aggiornare gli interssi dell'utente: {e}")

                except Exception as e:
                    mysql_conn.rollback()
                    return jsonify(f"[ERRORE]: {e}")

                token = get_opensky_token()
                update_flights(mysql_conn, email_utente, OPENSKY_DEPARTURE_ENDPOINT, token, 1)#aggiorna le partenze entro 1 giorno
                update_flights(mysql_conn, email_utente, OPENSKY_ARRIVAL_ENDPOINT, token, 1)#aggiorna gli arrivi entro l'ultimo giorno

                mysql_conn.close()
            else:
                return jsonify({"errore": "impossibile connettersi al db"}), 500
        else:
            # Utente non esiste
            return jsonify({"errore": "L'utente non esiste"}), 404

    except grpc.RpcError:
        return jsonify({"errore": "Errore gRPC"}), 502

    return jsonify({"message": "Interessi dell'utente inseriti e voli aggiornati"}), 200

""" OLD VERSION
@app.route("/user/interests", methods=["POST"])
def add_interest():
    data = request.json

    email_utente = data["email_utente"]
    aeroporti_icao = data['aeroporti_icao']

    # Verifico che l'utente esista tramite il canale gRPC
    try:
        if check_if_exists(email_utente):
            # Utente esiste
            mysql_conn = get_connection()

            # Inserisco gli aeroporti indicati dall'utente nella tabella airport
            if mysql_conn:
                try:
                    with mysql_conn.cursor() as cursor:
                        for icao in aeroporti_icao:
                            #sql_aeroporto = "INSERT IGNORE INTO airport (icao) VALUES (%s)"
                            #cursor.execute(sql_aeroporto, (icao, ))

                            sql_interest = "INSERT IGNORE INTO user_airports (email_utente, icao_aeroporto) VALUES (%s, %s)"
                            cursor.execute(sql_interest, (email_utente, icao))

                    mysql_conn.commit()

                except pymysql.MySQLError as e:
                    mysql_conn.rollback()
                    return jsonify(f"[ERRORE] Non è stato possibile aggiornare gli interssi dell'utente: {e}")

                except Exception as e:
                    mysql_conn.rollback()
                    return jsonify(f"[ERRORE]: {e}")

                token = get_opensky_token()

                if token:
                    icao_list = get_user_airports(mysql_conn, email_utente)

                    headers = {
                        "Authorization": f"Bearer {token}"
                    }

                    begin = get_begin_unix_time()
                    end = get_end_unix_time()

                    for icao in icao_list:
                        params = {
                            "airport": icao,
                            "begin": begin,
                            "end": end
                        }

                        try:
                            response = requests.get(OPENSKY_DEPARTURE_ENDPOINT, params=params, headers=headers)
                            response.raise_for_status()

                            data_departures = response.json()

                            # Aggiorno i voli in partenza da ogni aeroporto
                            if data_departures:
                                for departure in data_departures:
                                    icao_aereo = departure.get("icao24")
                                    first_seen = departure.get("firstSeen")
                                    aeroporto_partenza = departure.get("estDepartureAirport")
                                    last_seen = departure.get("lastSeen")
                                    aeroporto_arrivo = departure.get("estArrivalAirport")

                                    with mysql_conn.cursor() as cursor:
                                        try:
                                            sql_partenza = ("INSERT IGNORE INTO flight (icao_aereo, first_seen, aeroporto_partenza, "
                                                            "last_seen, aeroporto_arrivo) VALUES (%s, %s, %s, %s, %s)")
                                            cursor.execute(sql_partenza, (icao_aereo, first_seen, aeroporto_partenza, last_seen, aeroporto_arrivo))

                                            mysql_conn.commit()

                                        except pymysql.MySQLError as e:
                                            mysql_conn.rollback()
                                            return jsonify(f"[ERRORE] MySQL: {e}")

                        except requests.exceptions.RequestException as e:
                            return jsonify(f"[ERRORE]: Non è stato possibile recuperare i voli in partenza: {e}"), 400

                        #voli di arrivo
                        try:
                            response = requests.get(OPENSKY_ARRIVAL_ENDPOINT, params=params, headers=headers)
                            response.raise_for_status()

                            data_arrivals = response.json()

                            # Aggiorno i voli in arrivo da ogni aeroporto
                            if data_arrivals:
                                for arrival in data_arrivals:
                                    icao_aereo = arrival.get("icao24")
                                    first_seen = arrival.get("firstSeen")
                                    aeroporto_partenza = arrival.get("estDepartureAirport")
                                    last_seen = arrival.get("lastSeen")
                                    aeroporto_arrivo = arrival.get("estArrivalAirport")

                                    with mysql_conn.cursor() as cursor:
                                        try:
                                            sql_arrivo = ("INSERT IGNORE INTO flight (icao_aereo, first_seen, aeroporto_partenza, "
                                                            "last_seen, aeroporto_arrivo) VALUES (%s, %s, %s, %s, %s)")
                                            cursor.execute(sql_arrivo, (icao_aereo, first_seen, aeroporto_partenza, last_seen, aeroporto_arrivo))

                                            mysql_conn.commit()

                                        except pymysql.MySQLError as e:
                                            mysql_conn.rollback()
                                            return jsonify(f"[ERRORE] MySQL: {e}")

                        except requests.exceptions.RequestException as e:
                            return jsonify(f"[ERRORE]: Non è stato possibile recuperare i voli in arrivo: {e}")

                mysql_conn.close()
            else:
                return jsonify("Errore: impossibile connettersi al db"), 500
        else:
            # Utente non esiste
            return jsonify("L'utente non esiste"), 404

    except grpc.RpcError:
        return jsonify("Errore gRPC")

    return jsonify("Interessi dell'utente inseriti e voli aggiornati"), 200
    """

"""@app.route("/<email>/fights", methods=["GET"])
def get_flights(email):
    mysql_conn = get_connection()

    if mysql_conn:
        with mysql_conn.cursor() as cursor:
            try:
                sql_get_flights = "SELECT * FROM flights f JOIN user_airports u WHERE ua.email_utente=%s"
                cursor.execute(sql_get_flights, (email, ))

                rows = cursor.fetchall()

                return jsonify(rows)

            except pymysql.MySQLError as e:
                mysql_conn.rollback()
                return jsonify(f"[ERRORE] MySQL: {e}")
"""

@app.route("/airport/<icao>/last", methods=["GET"])
def get_last_flight(icao):
    mysql_conn = get_connection()
    if not mysql_conn:
        return jsonify({"error": "Errore: impossibile connettersi al db"}), 500

    try:
        with mysql_conn.cursor() as cursor:
            sql_last_departure = "SELECT * FROM flight WHERE aeroporto_partenza = %s ORDER BY last_seen DESC LIMIT 1"
            cursor.execute(sql_last_departure, (icao,))
            last_departure = cursor.fetchone()

            sql_last_arrival = "SELECT * FROM flight WHERE aeroporto_arrivo = %s ORDER BY last_seen DESC LIMIT 1"
            cursor.execute(sql_last_arrival, (icao,))
            last_arrival = cursor.fetchone()
    finally:
        mysql_conn.close()

    if not last_departure and not last_arrival:
        return jsonify({"error": "Nessun volo trovato"}), 404

    return jsonify({
        "last_departure": last_departure,
        "last_arrival": last_arrival
    }), 200


@app.route("/airport/<icao>/media", methods=["GET"])
def get_media_voli(icao):
    days = int(request.args.get("giorni", 7))  # default 7 giorni se non c'è argomento nella get es GET /airport/LIRF/media?giorni=10
    now = get_end_unix_time()
    start = now - days * 86400 #86400 secondi in un giorno

    mysql_conn = get_connection()
    if mysql_conn:
        with mysql_conn.cursor() as cursor:
            sql_media_voli = "SELECT COUNT(*) AS totale FROM flight WHERE (aeroporto_partenza = %s OR aeroporto_arrivo = %s) AND first_seen >= %s"
            cursor.execute(sql_media_voli, (icao, icao, start))

            totale = cursor.fetchone()["totale"]
        mysql_conn.close()
        media = totale / days
        return jsonify({"media_voli_giornaliera": media}), 200
    else:
        return jsonify("Errore: impossibile connettersi al db"), 500

if __name__ == "__main__":
    threading.Thread(target=scheduler_job, daemon=True).start()
    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=True)