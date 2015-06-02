import tia.configuration as Mfn
Mfn.DB_FILENAME = "db/streams.h5"
Mfn.PROCESS_NAME = "validate_streams"
import tia.trad.tools.ipc.processLogger as log
import tia.trad.tools.io.db as Mdb
from tia.trad.tools.errf import eCritical


if __name__ == "__main__":
    try:
        logger = log.loggerInit(Mfn.FN_LOG_VALIDATE_STREAMS)
        # expected message types are
        # "orderbook", "depth", "trade", "ping"
        #h5file = tables.openFile("db/streams.h5", mode="r", filters=tables.Filters(complevel=9, complib='blosc', shuffle=1))

        db = Mdb.DB()
        db.validateData()
    except Exception as ex: eCritical(logger, "main:ex: %s", ex)
