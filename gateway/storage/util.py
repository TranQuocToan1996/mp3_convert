import pika, json

def upload(f, fs, channel, payload):
    # upload f to mongo
    try:
        fid = fs.put(f)
    except:
        print("upload file to mongo fail: ", f.name)
        return "internal server error", 500

    # if upload ok, push message to rabbitmq
    message = {
        "video_fid": str(fid),
        "mp3_fid": None,
        "username": payload["username"]
    }
    try:
        channel.basic_publish(
            exchange="", #default exchange
            routing_key="video", # video queue
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ),
        )
    except Exception as err:
        print("send upload message to Rabbitmq fail: ", err)
        fs.delete(fid)
        return "internal server error", 500

    return None