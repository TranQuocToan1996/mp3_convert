import os, gridfs, pika, json
from flask import Flask, request, send_file
from flask_pymongo import PyMongo
from auth import validate
from auth_svc import access
from storage import util
from bson.objectid import ObjectId

server = Flask(__name__)
server.config["MONGO_URI"] = "mongodb://host.minikube.internal:27017/videos"

mongo = PyMongo(server)

fs = gridfs.GridFS(mongo.db)

connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
channel = connection.channel()

@server.route("/login", methods = ["POST"])
def login():
    token, err = access.login(request)
    if err:
        server.logger.error(err)
        return err
    return token

@server.route("/upload", methods = ["POST"])
def upload():
    access, err = validate.token(request)
    if err:
        server.logger.error(err)
        return err
    payload = json.loads(access)
    if not payload['admin']:
        return "Not authorize!", 401
    if len(request.files) != 1:
        return "Required exactly 1 file", 400
    for _, f in request.files.items():
        err = util.upload(f, fs, channel, payload)
        if err:
            server.logger.error(err)
            return err
    return "Upload success!", 200

@server.route("/download", methods = ["POST"])
def download():
    pass

if __name__ == "__main__":
    server.run(host="0.0.0.0", port=8080)