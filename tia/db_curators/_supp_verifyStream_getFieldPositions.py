"""
Find on what positions various field types are.
"""

import sys
from lib.old.tr_chan_specCheck import DYNAMIC_STRING_SET
from lib.old.tr_chan_specCheck import DYNAMIC_STRING_SET_EXCLUDE
STRING_ITEMS_OF_INTEREST = ["price", "currency", "item", "amount", 
                            "data", "primary", "properties", "tid",
                            "type", "avg", "buy", "sell", "high",
                            "low", "last", "last_all", "last_local",
                            "last_orig", "vol", "vwap", "date",
                            "price_currency", "mixed_currency",
                            "properties"]

#cannels
TICKER = ['{', 'channel', '', '', 'd5f06780-30a8-4a48-a2f8-7ed181b4a13f', '', '', 'op', '', '', 'private', '', '', 'origin', '', '', 'broadcast', '', '', 'private', '', '', 'ticker', '', '', 'ticker', '', '{', 'avg', '', '{', 'currency', '', '', 'USD', '', '', 'display', '', '', '$11.36104', '', '', 'display_short', '', '', '$11.36', '', '', 'value', '', '', '11.36104', '', '', 'value_int', '', '', '1136104', '}', '', 'buy', '', '{', 'currency', '', '', 'USD', '', '', 'display', '', '', '$11.14323', '', '', 'display_short', '', '', '$11.14', '', '', 'value', '', '', '11.14323', '', '', 'value_int', '', '', '1114323', '}', '', 'high', '', '{', 'currency', '', '', 'USD', '', '', 'display', '', '', '$12.14999', '', '', 'display_short', '', '', '$12.15', '', '', 'value', '', '', '12.14999', '', '', 'value_int', '', '', '1214999', '}', '', 'last', '', '{', 'currency', '', '', 'USD', '', '', 'display', '', '', '$11.20000', '', '', 'display_short', '', '', '$11.20', '', '', 'value', '', '', '11.20000', '', '', 'value_int', '', '', '1120000', '}', '', 'last_all', '', '{', 'currency', '', '', 'USD', '', '', 'display', '', '', '$11.20000', '', '', 'display_short', '', '', '$11.20', '', '', 'value', '', '', '11.20000', '', '', 'value_int', '', '', '1120000', '}', '', 'last_local', '', '{', 'currency', '', '', 'USD', '', '', 'display', '', '', '$11.20000', '', '', 'display_short', '', '', '$11.20', '', '', 'value', '', '', '11.20000', '', '', 'value_int', '', '', '1120000', '}', '', 'last_orig', '', '{', 'currency', '', '', 'USD', '', '', 'display', '', '', '$11.20000', '', '', 'display_short', '', '', '$11.20', '', '', 'value', '', '', '11.20000', '', '', 'value_int', '', '', '1120000', '}', '', 'low', '', '{', 'currency', '', '', 'USD', '', '', 'display', '', '', '$10.52000', '', '', 'display_short', '', '', '$10.52', '', '', 'value', '', '', '10.52000', '', '', 'value_int', '', '', '1052000', '}', '', 'sell', '', '{', 'currency', '', '', 'USD', '', '', 'display', '', '', '$11.20000', '', '', 'display_short', '', '', '$11.20', '', '', 'value', '', '', '11.20000', '', '', 'value_int', '', '', '1120000', '}', '', 'vol', '', '{', 'currency', '', '', 'BTC', '', '', 'display', '', '', '131', '944.37649580\\u00a0BTC', '', '', 'display_short', '', '', '131', '944.38\\u00a0BTC', '', '', 'value', '', '', '131944.37649580', '', '', 'value_int', '', '', '13194437649580', '}', '', 'vwap', '', '{', 'currency', '', '', 'USD', '', '', 'display', '', '', '$11.27877', '', '', 'display_short', '', '', '$11.28', '', '', 'value', '', '', '11.27877', '', '', 'value_int', '', '', '1127877', '}}}']
#old ticker
TICKER2 = ['{', 'channel', '', '', 'd5f06780-30a8-4a48-a2f8-7ed181b4a13f', '', '', 'oldticker', '', '{', 'avg', '', '6.374583003', '', 'buy', '', '6.41042', '', 'high', '', '6.89', '', 'last', '', '6.4299', '', 'last_all', '', '6.4299', '', 'last_local', '', '6.4299', '', 'low', '', '6.001', '', 'sell', '', '6.4299', '', 'vol', '', '114407', '', 'vwap', '', '6.374366148}', '', 'op', '', '', 'private', '', '', 'origin', '', '', 'broadcast', '', '', 'private', '', '', 'oldticker', '}']
DEPTH = ['{', 'channel', '', '', '24e67e0d-1cad-4cc0-9e7a-f8523ef460fe', '', '', 'depth', '', '{', 'currency', '', '', 'USD', '', '', 'item', '', '', 'BTC', '', '', 'now', '', '', '1326186945415528', '', '', 'price', '', '', '6.20684', '', '', 'price_int', '', '', '620684', '', '', 'total_volume_int', '', '', '0', '', '', 'type', '', '2', '', 'type_str', '', '', 'bid', '', '', 'volume', '', '', '-31.35112174', '', '', 'volume_int', '', '', '-3135112174', '}', '', 'op', '', '', 'private', '', '', 'origin', '', '', 'broadcast', '', '', 'private', '', '', 'depth', '}']
TRADE = ['{', 'channel', '', '', 'dbf1dee9-4f2e-4a08-8cb7-748919a71b21', '', '', 'op', '', '', 'private', '', '', 'origin', '', '', 'broadcast', '', '', 'private', '', '', 'trade', '', '', 'trade', '', '{', 'amount', '', '0.0649706', '', 'amount_int', '', '', '6497060', '', '', 'date', '', '1326190390', '', 'item', '', '', 'BTC', '', '', 'price', '', '6.411', '', 'price_currency', '', '', 'USD', '', '', 'price_int', '', '', '641100', '', '', 'primary', '', '', 'Y', '', '', 'properties', '', '', 'limit', '', '', 'tid', '', '', '1326190390610575', '', '', 'trade_type', '', '', 'ask', '', '', 'type', '', '', 'trade', '}}']
#mixed currency
TRADE2 = ['{', 'channel', '', '', 'dbf1dee9-4f2e-4a08-8cb7-748919a71b21', '', '', 'op', '', '', 'private', '', '', 'origin', '', '', 'broadcast', '', '', 'private', '', '', 'trade', '', '', 'trade', '', '{', 'amount', '', '3', '', 'amount_int', '', '', '300000000', '', '', 'date', '', '1326227399', '', 'item', '', '', 'BTC', '', '', 'price', '', '4.28345', '', 'price_currency', '', '', 'GBP', '', '', 'price_int', '', '', '428345', '', '', 'primary', '', '', 'Y', '', '', 'properties', '', '', 'limit', 'mixed_currency', '', '', 'tid', '', '', '1326227399764944', '', '', 'trade_type', '', '', 'bid', '', '', 'type', '', '', 'trade', '}}']
DEPRECATED = ['{', 'message', '', '', 'This websocket access is deprecated', ' please use socket.io client library to connect to https', '//socketio.mtgox.com/mtgox', '', '', 'op', '', '', 'remark', '}']


def getFieldIndexes(channel):
    numberFields = []
    stringFields = []
    dynStringFields = []
    index = 0
    
    for field in channel:
        try: #find positions of numbers
            if float(field):
                numberFields.append(index)
            elif float(field) == 0:
                numberFields.append(index)            
        except:
            #if not a number, it's a string
            #field can be also mylst=[''] and then mylist[0] doesn't make sense
            if len(field) != 0:
                #check if special field _number_\u00a0BTC present
                if len(field) > 9:
                    if field[len(field) - 9:len(field)] == "\u00a0BTC":
                        #since it's hard to parse, don't include this 
                        #field in validation!
                        pass
                #test if field of the form "$_number_"
                elif field[0] == "$":
                    #since $_number_ is hard to parse, don't include this 
                    #field in validation!
                    pass
                #too many currencies!
                elif field in DYNAMIC_STRING_SET_EXCLUDE:
                    pass
                #check if other known dynamic fields present
                elif field in DYNAMIC_STRING_SET:
                    dynStringFields.append(index)
                #otherwise it's a static string field
                else:
                    stringFields.append(index)
            #field can be '' in which case it's a string field
            else:
                stringFields.append(index)
        index = index + 1
        
        
    print "static_str fields at: ", stringFields
    print "dynamic_str fields at: ", dynStringFields
    print "numerical fields at: %s\n\n" %numberFields


    #do some printing for tr_market_variables
    print "String items of interest:"
    for i in xrange(len(channel)):
        if channel[i] in STRING_ITEMS_OF_INTEREST:
            print "%s at %s: %s" %(channel[i], i, channel[i-2 : i + 40])
    
    print "\nNumber fields context:"
    for i in numberFields:
        print "At %s %s" % (i, channel[i - 30 : i + 3])
    

def checkChanString():
    try:
        if "ticker" not in TICKER:
            print "'ticker' not in list ticker"
            raise RuntimeError
        elif "depth" not in DEPTH:
            print "'depth' not in list depth"
            raise RuntimeError
        elif "trade" not in TRADE:
            print "'trade' not in list trade"
            raise RuntimeError
        
    except RuntimeError:
        print "Check if channel correct!"
        raise KeyboardInterrupt
    except:
        e = sys.exc_info()[1]
        print("Error: ", e)
        raise KeyboardInterrupt 
    
    
if __name__ == "__main__":  
    #choose which channel to get indexes from  
    testThisChannel = TICKER
    
    try:
        print testThisChannel
        checkChanString()
        getFieldIndexes(testThisChannel)
    
    except:
        e = sys.exc_info()[1]
        print("Error: ", e)
        raise KeyboardInterrupt