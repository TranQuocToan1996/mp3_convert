import pika, sys, os, time
from pymongo import MongoClient
import gridfs
from convert import to_mp3

def main():
    client = MongoClient("host.minikube.internal", 27017)
    db_videos = client.videos
    db_mp3s = client.mp3s
    # gridfs
    fs_videos = gridfs.GridFS(db_videos)
    fs_mp3s = gridfs.GridFS(db_mp3s)

    def callback(ch, method, properties, body):
        err = to_mp3.start(body, fs_videos, fs_mp3s, ch)
        if err:
            # negative acknowledge, so that the message remain in the queueu, another consumer will process it
            ch.basic_nack(delivery_tag=method.delivery_tag) 
            print(err)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag) 

    # rabbitmq connection
    rabbitServiceName = "rabbitmq"
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitServiceName)
    )
    channel = connection.channel()
    channel.basic_consume(
        queue=os.environ.get("VIDEO_QUEUE", ""), 
        on_message_callback=callback
    )

    channel.start_consuming()
    print("Waiting for messages. To exit press CTRL+C")

# gracefully shutdown
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

