import pika, json, tempfile, os
from bson.objectid import ObjectId
import moviepy.editor

def start(message, fs_videos, fs_mp3s, channel):
    message = json.loads(message)
    video_data = fs_videos.get(ObjectId(message["video_fid"]))
    tf = tempfile.NamedTemporaryFile()
    tf.write(video_data.read())
    audio = moviepy.editor.VideoFileClip(tf.name).audio
    tf.close()

    tf_path = tempfile.gettempdir() + f"/{message['video_fid']}.mp3"
    audio.write_audiofile(tf_path)
    
    f = open(tf_path, "rb")
    data = f.read()
    mp3_fid = fs_mp3s.put(data)
    f.close()
    os.remove(tf_path)

    try:
        channel.basic_publish(
            exchange="",
            routing_key=os.environ.get("MP3_QUEUE"),
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ),
        )
    except Exception as err:
        fs_mp3s.delete(mp3_fid)
        return "failed to publish message" + err
