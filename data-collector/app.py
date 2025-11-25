from flask import Flask, jsonify, request
import os
import pymysql.cursors
import requests
import grpc
import user_service_pb2
import user_service_pb2_grpc

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

OPENSKY_DEPARTURE_ENDPOINT = "https://opensky-network.org/api//flights/departure?"
OPENSKY_ARRIVAL_ENDPOINT = "https://opensky-network.org/api//flights/arrival?"
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
                            sql_aeroporto = "INSERT IGNORE INTO `airport` (`icao`) VALUES (%s)"
                            cursor.execute(sql_aeroporto, (icao, ))

                            sql_interest = "INSERT IGNORE INTO `user_airports` (`email_utente`, `icao`) VALUES (%s, %s)"
                            cursor.execute(sql_interest, (email_utente, icao))

                    mysql_conn.commit()

                except pymysql.MySQLError as e:
                    mysql_conn.rollback()
                    return jsonify(f"[ERRORE] MySQL: {e}")
                    raise

                except Exception as e:
                    mysql_conn.rollback()
                    return jsonify(f"[ERRORE]: {e}")
                    raise

            # Deve essere chiamata ogni 30 minuti
            token = get_opensky_token()

            #... Fare chiamata a opensky una volta ottenuto il token

            mysql_conn.close()
        else:
            # Utente non esiste
            return jsonify("L'utente non esiste"), 404

    except grpc.RpcError:
        return jsonify("Errore gRPC")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=True)