# from http://www.voidspace.org.uk/python/articles/urllib2.shtml
import urllib
import urllib2
import time

url = "https://bitcoin-24.com/api/user_api.php"
apiKey = "ScNi3uBhY46OpSU3BazNwqYb8IiKAIV7"


values = {"user": "terryww",
          "key": apiKey,
          "api": "open_orders"
          }
values = {"user": "terryww",
          "key": apiKey,
          "api": "open_orders",
          #"amount": str(_Order.amount.quantize(_Market.pipAmount)),
          #"price": str(_Order.price.quantize(_Market.pip)),
          #"cur": "EUR"
           }
t1 = time.time()
data = urllib.urlencode(values)
req = urllib2.Request(url, data)
response = urllib2.urlopen(req)
the_page = response.read()
t2 = time.time()

print the_page
print t2 - t1
