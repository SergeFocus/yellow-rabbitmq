import os
import time

from inspect import currentframe, getframeinfo
from dotenv import load_dotenv
from pathlib import Path  # python3 only

# соединие, точка обмена, источник, очередь, подписчик
from kombu import Connection, Exchange, Producer, Queue, Consumer

filename = getframeinfo(currentframe()).filename

# пароли храним в файле окружения или в docker
env_path = Path(filename).resolve().parent / '.env'
load_dotenv(dotenv_path=env_path,verbose=True)

user = os.getenv('USERAMQP')
passw = os.getenv('PASSAMQP')


# полный адрес сервера
rabbit_url = "amqp://{0}:{1}@antelope.rmq.cloudamqp.com/mqnlxzth".format(user, passw)

# соединие сервера - живет на время передачи событий
conn = Connection(rabbit_url)

# канал для публикации
channel = conn.channel()

def process_message(body, message):
    print("Тело сообщения {}".format(body))
    message.ack()

exchange = Exchange("dictionary_announce.1c", type="fanout")

queue = Queue(name="dictionary_announce.1c.queue", exchange=exchange)

try:
    while True:
        with Consumer(conn, queues=queue, callbacks=[process_message]):  
            conn.drain_events()
except KeyboardInterrupt:
    print('')
    print('подписка на события прервана!')
