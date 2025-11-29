import logging

from flask import Flask, jsonify, request
import os
import pymysql
import bcrypt
import time

import grpc
from concurrent import futures
import threading

import user_service_pb2
import user_service_pb2_grpc

app = Flask(__name__)

LISTEN_PORT = int(os.getenv("LISTEN_PORT", 5003))
LISTEN_PORT_GRPC = int(os.getenv("LISTEN_PORT_GRPC", 50051))

GRPC_HOST = os.getenv("GRPC_HOST")
GRPC_SEND_PORT = int(os.getenv("GRPC_SEND_PORT", 50052))

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_USERNAME = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

DATA_COLLECTOR_ADDRESS = f"{GRPC_HOST}:{GRPC_SEND_PORT}"

cache_message_ids = {}

# Funzione per la connessione al database MySQL
def get_connection():
    try:
        mysql_conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USERNAME,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            cursorclass=pymysql.cursors.DictCursor
        )

        # Eseguo una query di ping
        mysql_conn.ping(reconnect=True)

        print(f"Connessione MySQL stabilita su {MYSQL_HOST}:{MYSQL_PORT}, DB={MYSQL_DATABASE}")

        return mysql_conn

    except pymysql.MySQLError as e:
        print(f"ERRORE: Impossibile connettersi a MySQL. Dettagli: {e}")
        return None

# Svuota la cache ogni 10 minuti
def clear_cache():
    cache_message_ids.clear()

    threading.Timer(600, clear_cache).start()

@app.route("/")
def home():
    return jsonify(message="Hello"), 200

@app.route("/users", methods=["POST"])
def create_user():
    data = request.json

    message_id = data.get("messageID") # per at-most-once
    email = data.get("email")
    nome = data.get("nome")
    cognome = data.get("cognome")
    password = data.get("password")

    # Verificare se message_id si trova nella cache
    if message_id and message_id in cache_message_ids:
        return jsonify({"error": "Utente già registrato [At-most-once]"}), 400
    else:
        if not email or not password:
            return jsonify({"error": "Email e password obbligatorie"}), 400

        # Inserisco il messaggio nella cache
        cache_message_ids[message_id] = {
            "email": email,
            "timestamp": time.time()
        }

        mysql_conn = get_connection()
        if mysql_conn:
            with mysql_conn.cursor() as cursor:
                # Verfico se l'utente esiste
                cursor.execute("SELECT * FROM users WHERE email=%s", (email,))
                existing = cursor.fetchone()

                if existing:
                    return jsonify({"error": f"Email {email} già in uso"}), 400

                hashed_pw = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())

                try:
                    # Inserisco l'utente nella tabella users
                    cursor.execute(
                        "INSERT INTO users (email, nome, cognome, password_hash) VALUES (%s, %s, %s, %s)",
                        (email, nome, cognome, hashed_pw)
                    )

                    mysql_conn.commit()

                except pymysql.MySQLError as e:
                    mysql_conn.rollback()
                    return jsonify({"error": f"Errore DB: {e}"}), 500

            mysql_conn.close()

            return jsonify({"message": f"Utente {email} registrato con successo"}), 200
        else:
            return jsonify({"error": "MySQl non connesso"}), 503

@app.route("/users/<email>", methods=["DELETE"])
def delete_user(email):
    mysql_conn = get_connection()

    if mysql_conn:
        with mysql_conn.cursor() as cursor:
            cursor.execute("SELECT * FROM users WHERE email=%s", (email,))

            user = cursor.fetchone()

            if not user:
                return jsonify({"error": "Utente non trovato"}), 404

            try:
                cursor.execute("DELETE FROM users WHERE email=%s", (email,))

                mysql_conn.commit()

                logging.info(f"Utente {email} cancellato con successo")

                mysql_conn.close()

                # Comunico tramite il canale gRPC col data-collector per eliminare le righe corrispondenti
                # all'utente eliminato dalla tabella user-airports
                with grpc.insecure_channel(DATA_COLLECTOR_ADDRESS) as channel:
                    stub = user_service_pb2_grpc.DataServiceStub(channel)

                    response = stub.DeleteUserInterests(user_service_pb2.UserCheckRequest(email=email))

                    if response.deleted:
                        return jsonify({"message": f"Interessi dell'utente {email} cancellati con successo"}), 200
                    else:
                        return jsonify({"error": f"Errore nell'eliminazinoe degli nteressi dell'utente {email}"}), 500

            except pymysql.MySQLError as e:
                mysql_conn.rollback()
                mysql_conn.close()
                return jsonify({"error": f"Impossibile eliminare l'utente {email}: {e}"}), 500
    else:
        return jsonify({"error": "MySQL non connesso"}), 503

class UserManagerService(user_service_pb2_grpc.UserServiceServicer):
    def CheckIfUserExists(self, request, context):
        email = request.email

        mysql_conn = get_connection()

        if mysql_conn:
            with mysql_conn.cursor() as cursor:
                cursor.execute("SELECT * FROM users WHERE email=%s", (email,))
                user = cursor.fetchone()

                exists = user is not None

            mysql_conn.close()

            return user_service_pb2.UserCheckResponse(
                exists=exists,
                message="Utente trovato" if exists else "Utente non trovato"
            )
        else:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("MySQL non connesso")

            return user_service_pb2.UserCheckResponse(
                exists=False,
                message="Errore: MySQL non connesso"
            )

def serve():
    # Creo un server grpc con thread pool di 10 worker threads
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    user_service_pb2_grpc.add_UserServiceServicer_to_server(UserManagerService(), server)

    server.add_insecure_port(f'[::]:{LISTEN_PORT_GRPC}') # Lego il server alla porta

    server.start()

    print(f"UserService è pronto ed in ascolto sulla porta {LISTEN_PORT_GRPC}")

    server.wait_for_termination()

if __name__ == "__main__":
    # Avvia gRPC in un thread separato
    threading.Thread(target=serve, daemon=True).start()

    # Thread per pulire la cache
    threading.Thread(target=clear_cache, daemon=True).start()

    # Avvia Flask
    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=True)

