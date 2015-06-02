import sys
import tia.trad.tools.ipc.processLogger as pl
import tia.configuration as conf
from tia.trad.tools.errf import eReport


class Process:
    def __init__(self, _modulePath, **kwargs):
        try:
            validate = type(_modulePath); assert validate == str, validate

            moduleName = _modulePath.split(".")[-1]
            # set process name
            conf.PROCESS_NAME = moduleName
            #TODO: set Proces logger name before import...
            # set logging file for that process
            pl.PROCESS_NAME = moduleName + "."
            self.processLogger = pl.loggerInit(moduleName)
            # import a module whose name is only known at runtime
            myMod = __import__(_modulePath, globals(), locals(), [moduleName], -1)
            # pass args and start the process
            kwargs["processLogger"] = self.processLogger
            myMod.run(**kwargs)
        except KeyboardInterrupt:
            print "%s received KeyboardInterrupt, shutting down" % moduleName
            myMod.shutdown()
            sys.exit()
        except Exception: eReport(moduleName)



