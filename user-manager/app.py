from flask import Flask, jsonify, request
import os
import pymysql
import bcrypt

import grpc
from concurrent import futures
import threading

import user_service_pb2
import user_service_pb2_grpc

app = Flask(__name__)

LISTEN_PORT = int(os.getenv("LISTEN_PORT", 5003))
LISTEN_PORT_GRPC = int(os.getenv("LISTEN_PORT_GRPC"))

# configurazione variabili di ambiente per connessione a MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_USERNAME = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

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
        #eseguo una query di ping
        mysql_conn.ping(reconnect=True)
        print(f"Connessione a MySQL stabilita su {MYSQL_HOST}:{MYSQL_PORT}, DB={MYSQL_DATABASE}")
        return mysql_conn
    except Exception as e:
        print(f"ERRORE: Impossibile connettersi a MySQL. Dettagli: {e}")
        return None

@app.route("/")
def home():
    # Passa i dati JSON come parola chiave (kwargs)
    # L'oggetto serializzato sarà {"message": "Hello"}
    return jsonify(message="Hello"), 200

@app.route("/users", methods=["POST"])
def create_user():
    data = request.json
    email = data.get("email")
    nome = data.get("nome")
    cognome = data.get("cognome")
    password = data.get("password")

    if not email or not password:
        return jsonify({"error": "Email e password obbligatorie"}), 400

    mysql_conn = get_connection()
    if mysql_conn:
        with mysql_conn.cursor() as cursor:
            cursor.execute("SELECT * FROM users WHERE email=%s", (email,))
            existing = cursor.fetchone() #recupera una sola riga, se esiste significa che l'email esiste gia
            if existing:
                return jsonify({"error": "Utente già registrato (at-most-once policy)"}), 400

            hashed_pw = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
            try:
                cursor.execute(
                    "INSERT INTO users (email, nome, cognome, password_hash) VALUES (%s, %s, %s, %s)",
                    (email, nome, cognome, hashed_pw)
                )
                mysql_conn.commit() #conferma la transazione
            except Exception as e:
                mysql_conn.rollback() #annulla la transazione
                return jsonify({"error": f"Errore DB: {e}"}), 500
        mysql_conn.close()
        return jsonify({"message": "Utente registrato con successo", "email": email}), 201
    else:
        return jsonify({"error": "MySQl non connesso"}), 503

@app.route("/users/<email>", methods=["DELETE"])
def delete_user(email):
    mysql_conn = get_connection()
    if mysql_conn:
        with mysql_conn.cursor() as cursor:
            cursor.execute("SELECT * FROM users WHERE email=%s", (email,)) #(email,) è una tupla con un solo elemento e le query con cursor richiedono una lista o tupla
            user = cursor.fetchone()
            if not user:
                return jsonify({"error": "Utente non trovato"}), 404
            try:
                cursor.execute("DELETE FROM users WHERE email=%s", (email,))
                mysql_conn.commit()
            except Exception as e:
                mysql_conn.rollback()
                return jsonify({"error": f"Errore DB: {e}"}), 500
        mysql_conn.close()
        return jsonify({"message": f"Utente {email} cancellato con successo"}), 201 #f prima di " serve per far si che python sostituisca a email il suo valore
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
                exists = user is not None  # booleano True/False
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
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10)) #creo server grpc con thread pool di 10 worker threads

    user_service_pb2_grpc.add_UserServiceServicer_to_server(UserManagerService(), server)

    server.add_insecure_port(f'[::]:{LISTEN_PORT_GRPC}') #leghiamo server alla porta

    server.start()
    print(f"UserService è pronto ed in ascolto sulla porta {LISTEN_PORT_GRPC}")

    server.wait_for_termination()


if __name__ == "__main__":
    # Avvia gRPC in un thread separato
    threading.Thread(target=serve, daemon=True).start()
    # Avvia Flask
    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=True)

