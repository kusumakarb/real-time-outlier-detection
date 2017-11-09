from kafka import KafkaConsumer
import tornado
import json
import tornado.ioloop
import tornado.web
import tornado.websocket
import tornado.template

consumer = KafkaConsumer('output',
                         group_id='realTimeViz',
                         bootstrap_servers=['localhost:9092'])


def get_graph_data():
  graphData = {
    'x': [],
    'y': []
  }

  for message in consumer:
      messageJson = json.loads(message.value)
      print(messageJson)
      transactionTime = messageJson['trans_time']
      transactionAmount = messageJson['trans_amt']
      graphData['x'].append(transactionTime)
      graphData['y'].append(transactionAmount)
      return json.dumps(graphData)



class MainHandler(tornado.web.RequestHandler):
  def get(self):
    loader = tornado.template.Loader(".")
    self.write(loader.load("./resources/templates/graph.html").generate())

class WSHandler(tornado.websocket.WebSocketHandler):

  def open(self):
    pass

  def on_message(self, message):
      self.write_message(get_graph_data())

  def on_close(self):
    print('Connection closed.')


def startWebApplication():
  return tornado.web.Application([
  (r'/ws', WSHandler),
  (r"/", MainHandler),
  (r"/(.*)", tornado.web.StaticFileHandler, {"path": "./resources"}),
])


# To consume latest messages and auto-commit offsets
KafkaConsumer(auto_offset_reset='latest', enable_auto_commit=False)


application = startWebApplication()

application.listen(9091)
tornado.ioloop.IOLoop.instance().start()

