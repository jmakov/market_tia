AVAILABLE_ACTIONS = ["sim", "record", "live"]

DIR_DB = "db/"
FN_REPORT = DIR_DB + "report.json"
FN_DB = DIR_DB + "events.json"
FN_DB_SIM = DIR_DB + "sortedEvents.json"
FN_PLOT_IMAGE = DIR_DB + "report.png"

DIR_LOG = "/tmp/bc_tia/logs/"
DIR_STREAMS = DIR_LOG + "streams/"

FN_CRASH_REPORT = DIR_LOG + "crashReport.log"
FN_LOG_TMP = DIR_LOG + "tmp.log"
FN_LOG_VALIDATE_STREAMS = DIR_LOG + "validate_streams.log"

DIR_STATE = "state/"
FN_CREDENTIALS = DIR_STATE + "credo.json"
FN_TM = DIR_STATE + "tm.json"
FN_LTID = DIR_STATE + "ltid.json"


STREAMS = ["recorder", "stream_apis", "stream_intrsng", "stream_mtgox", "stream_mtgoxAPI", "stream_btc24Api", "syncher", "recorder"]
STREAMERS = ["intrsng", "mtgox"]

UNIVERSE = "btc24EUR"   # "market_name" or "" for entire universe
