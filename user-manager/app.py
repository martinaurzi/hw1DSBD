from flask import Flask, jsonify, request
import os
import pymysql
import bcrypt

app = Flask(__name__)

LISTEN_PORT = int(os.getenv("LISTEN_PORT"))

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

@app.route("/create", methods=["POST"])
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=True)
