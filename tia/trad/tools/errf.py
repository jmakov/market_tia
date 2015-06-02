import traceback
import tia.configuration as conf
import tia.trad.tools.io.file_handlers as fh

def eReport(_processName):
    try:
        filename = conf.FN_CRASH_REPORT
        with fh.FileLock(filename) as lock:
            with open(filename, "a") as f:
                trace = traceback.format_exc()
                f.write(_processName + ":\n" +  trace + "\n")
        print "%s: crashed :(" % _processName
    except Exception: raise
