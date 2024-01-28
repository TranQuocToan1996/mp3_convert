import os, gridfs, pika, json
from flask import Flask, request, send_file
from flask_pymongo import PyMongo
from auth import validate
from auth_svc import access
from storage import util
from bson.objectid import ObjectId

server = Flask(__name__)

mongo_video = PyMongo(
    server,
    uri=os.environ.get("MONGO_URI_VIDEO", "mongodb://mongo:27017/videos")
)

mongo_mp3 = PyMongo(
    server,
    uri=os.environ.get("MONGO_URI_MP3", "mongodb://mongo:27017/mp3s")
)

fs_videos = gridfs.GridFS(mongo_video.db)
fs_mp3s = gridfs.GridFS(mongo_mp3.db)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host="rabbitmq",
        heartbeat=0,
        blocked_connection_timeout=300
                            ))
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
        err = util.upload(f, mongo_video, channel, payload)
        if err:
            server.logger.error(err)
            return err
    return "Upload success!", 200

@server.route("/download", methods = ["GET"])
def download():
    access, err = validate.token(request)
    if err:
        server.logger.error(err)
        return err
    payload = json.loads(access)
    if not payload['admin']:
        return "Not authorize!", 401
    fid = request.args.get("fid")
    if not fid:
        return "Empty fid!", 400
    try:
        audio_data = fs_mp3s.get(ObjectId(fid))
        return send_file(audio_data, f"{fid}.mp3")
    except Exception as err:
        server.logger.error(err)
    return "Internal error", 500

if __name__ == "__main__":
    server.run(host="0.0.0.0", port=8080)