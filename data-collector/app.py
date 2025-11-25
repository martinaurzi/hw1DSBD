from flask import Flask, jsonify, request
import os
import pymysql.cursors
import requests
import grpc
import user_service_pb2
import user_service_pb2_grpc
from datetime import datetime, timezone

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

OPENSKY_DEPARTURE_ENDPOINT = "https://opensky-network.org/api//flights/departure"
OPENSKY_ARRIVAL_ENDPOINT = "https://opensky-network.org/api//flights/arrival"
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
        return jsonify(f"[ERRORE]: Non è stato possibile recuperare il token: {e}")

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
        mysql_conn.rollback()
        return []

def get_begin_unix_time() -> int:
    current_time_utc = datetime.now(timezone.utc)

    current_time_timestamp = int(current_time_utc.timestamp())

    seven_days_in_seconds = 7 * 24 * 60 * 60

    return current_time_timestamp - seven_days_in_seconds

def get_end_unix_time() -> int:
    current_time_utc = datetime.now(timezone.utc)

    return int(current_time_utc.timestamp())

@app.route("/")
def home():
    return jsonify("Hello, Data Collector"), 200

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
                            sql_aeroporto = "INSERT IGNORE INTO airport (icao) VALUES (%s)"
                            cursor.execute(sql_aeroporto, (icao, ))

                            sql_interest = "INSERT IGNORE INTO user_airports (email_utente, icao) VALUES (%s, %s)"
                            cursor.execute(sql_interest, (email_utente, icao))

                    mysql_conn.commit()

                except pymysql.MySQLError as e:
                    mysql_conn.rollback()
                    return jsonify(f"[ERRORE] MySQL: {e}")

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

                                    with mysql_conn as cursor:
                                        try:
                                            sql_partenza = ("INSERT IGNORE INTO flight (icao_aereo, first_seen, aeroporto_partenza, "
                                                            "last_seen, aeroporto_arrivo) VALUES (%s, %s, %s, %s, %s)")
                                            cursor.execute(sql_partenza, (icao_aereo, first_seen, aeroporto_partenza, last_seen, aeroporto_arrivo))

                                            mysql_conn.commit()

                                        except pymysql.MySQLError as e:
                                            mysql_conn.rollback()
                                            return jsonify(f"[ERRORE] MySQL: {e}")

                        except requests.exceptions.RequestException as e:
                            return jsonify(f"[ERRORE]: Non è stato possibile recuperare i voli in partenza: {e}")

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

                                    with mysql_conn as cursor:
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

@app.route("/airport/<icao>/last", methods=["GET"])
def get_last_flight(icao):
    mysql_conn = get_connection()
    if mysql_conn:
        with mysql_conn.cursor() as cursor:
            sql_last_flight = "SELECT * FROM flight WHERE aeroporto_partenza = %s OR aeroporto_arrivo = %s ORDER BY last_seen DESC LIMIT 1"
            cursor.execute(sql_last_flight, (icao, icao))
            volo = cursor.fetchone()
        mysql_conn.close()
        return jsonify(volo), 200 if volo else (jsonify("Nessun volo trovato"), 404)
    else:
        return jsonify("Errore: impossibile connettersi al db"), 500

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
    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=True)