import logging
import logging.handlers
import tia.configuration as config
from tia.trad.tools.io.file_handlers import createDir
PROCESS_NAME = "main"   # changed by processes


def loggerInit(_filename):
    try:
        global PROCESS_NAME
        #bring up the logger
        logger_name = _filename.split("/")[-1] #"rl"
        mylog = logging.getLogger(logger_name)    #name of the head logger, other loggers in modules must be named as "root_logeger.__name__"
        mylog.setLevel(logging.DEBUG)

        # create logging dir
        #createDir(config.DIR_LOG)
        # create streams dir
        if _filename in config.STREAMS:
            _filename = config.DIR_STREAMS + _filename
            createDir(config.DIR_STREAMS)

        fh = logging.handlers.RotatingFileHandler(_filename, maxBytes=1000000, backupCount=1)    #file handler for the logger
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(name)s;%(message)s")
        fh.setFormatter(formatter)
        mylog.addHandler(fh)

        return mylog
    except Exception: raise

