from threading import *
import urllib2
import urllib
import time
import traceback
import logging

from tia.trad.tools.net.websocket_client import create_connection
from tia.trad.tools.errf import eReport


LOGGER_NAME = "rl." + __file__.split("/")[-1]
logger = logging.getLogger(LOGGER_NAME)  # don't change!

class SocketIO:
  def __init__(S, url, callback):
      try:
          S.url = url
          S.callback = callback
      except Exception: raise

  def connect(S):
    try:
      data = urllib.urlencode({})
      req = urllib2.Request('https://' + S.url + "/1", data)
      print 'https://' + S.url + "/1"
      response = urllib2.urlopen(req)
      r = response.read().split(':')
      S.heartbeat_interval = int(r[1])
      print 'heartbeat: ', S.heartbeat_interval
      if 'websocket' in r[3].split(','):
        print "good: transport 'websocket' supported by socket.io server ", S.url
        S.id = r[0]
        print "id: ", S.id

      S.thread = Thread(target = S.thread_func)
      S.thread.setDaemon(True)
      S.thread.start()
    except Exception: raise

  def stop(S):
      try:
          S.run = False
          S.thread.join(timeout=1)
          S.keepalive_thread.join(timeout=1)
      except Exception: raise

  def thread_func(S):
      try:
          print 'SocketIO: websocket thread started'

          my_url = 'wss://' + S.url + "/1/websocket/" + S.id

          S.ws = create_connection(my_url)

          #S.ws = WebSocket(my_url, version=0)
          S.run = True
          S.ws.send('1::/mtgox')

          # start keepalive thread
          S.keepalive_thread = Thread(target = S.keepalive_func)
          S.keepalive_thread.setDaemon(True)
          S.keepalive_thread.start()

          msg = S.ws.recv()
          while msg is not None and S.run:
              #print 'SocketIO msg: ', msg
              if msg[:10] == "4::/mtgox:":
                  S.callback(msg[10:])
              elif msg[:3] == "2::":
                  True
              else:
                  print "SocketIO: dont know how to handle msg: ", msg
              msg = S.ws.recv()
          S.ws.close()
      except Exception: raise

      
  def keepalive_func(S):
    while S.run:
      try:
        S.ws.send('2::');
      except Exception as ex:
        eReport(logger, "keepalive_func:ex: %s", ex)
        if S.run:
          print 'error sending keepalive socket.io: %s. trying reconnect' % ex
          #S.connect()
          # close socket, let mtgox_stream handle reconnections (has to send orderbook etc.)
          S.stop()
        else:
          print 'exiting socket.io keepalive thread'
      time.sleep(S.heartbeat_interval)
      
def test_callback(msg):
  print 'msg: ', msg

# testcase
if False:
  sio = SocketIO('socketio.mtgox.com/socket.io', test_callback)
  sio.connect()
  time.sleep(100)

