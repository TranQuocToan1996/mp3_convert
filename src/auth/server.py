import jwt, datetime, os
from flask import Flask, request
from flask_mysqldb import MySQL

server = Flask(__name__)
server.config["MYSQL_HOST"] = os.environ.get("MYSQL_HOST", "localhost")
server.config["MYSQL_USER"] = os.environ.get("MYSQL_USER", "root")
server.config["MYSQL_PASSWORD"] = os.environ.get("MYSQL_PASSWORD", "password")
server.config["MYSQL_DB"] = os.environ.get("MYSQL_DB", "auth")
server.config["MYSQL_PORT"] = int(os.environ.get("MYSQL_PORT", "3306"))
mysql = MySQL(server)

@server.route("/validate", methods=["POST"])
def validate():
    encode_jwt = request.headers["Authorization"]
    if not encode_jwt:
        return "Missing credentials", 401
    encode_jwt = encode_jwt.split(" ")[1]
    
    try:
        decoded = jwt.decode(encode_jwt, os.environ.get("JWT_SECRET"), algorithms=["HS256"])
    except:
        return "Not authorized", 403
    
    return decoded, 200

@server.route("/login", methods=["POST"])
def login():
    auth = request.authorization
    if not auth:
        return "Missing credentials", 401
    
    cur = mysql.connection.cursor()
    res = cur.execute(
        "SELECT email, password from user where email = %s", (auth.username,)
    )
    if res == 0:
        return "Invalid credentials", 401
    
    user_row  =cur.fetchone()
    email = user_row[0]
    password = user_row[1]

    if auth.username != email or password != auth.password:
        return "Invalid credentials", 401
    else:
        return createJWT(auth.username, os.environ.get("JWT_SECRET"), True)

def createJWT(username, secret, admin = True):
    return jwt.encode(
    {
        "username": username,
        "exp":  datetime.datetime.utcnow() + datetime.timedelta(days=1),
        "iat": datetime.datetime.utcnow(),
        "admin": admin,
    }, 
    secret,
    algorithm="HS256",
    )

if __name__ == "__main__":
    server.run(host="0.0.0.0", port=5000)