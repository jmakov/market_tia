import multiprocessing
import sys
from tia.trad.tools.ipc.naming_conventions import IPC

import tia.trad.tools.ipc.process as process


if __name__ == "__main__":
    try:
        dummyQExternal = multiprocessing.Queue()

        job = multiprocessing.Process(target=process.Process, args=("tia.trad.streams.streamer",),
                                                  kwargs={IPC.QExternal: dummyQExternal})
        job.start()

        while 1:
            try:
                dummyItem = None
                dummyItem = dummyQExternal.get(block=True)
            except KeyboardInterrupt:
                """
                 @attention: multiprocessing sends KeyboardInterrupt to all processes started
                 in this or subprocesses
                """
                sys.stderr.write("\n%s: received KeyboardInterrupt. Passing it to all subprocesses.\n" % (__file__))
                # wait for subprocesses to shut down
                job.join()
                sys.exit()
            except Exception as ex:
                sys.stderr.write("\nrecord_streams: while:ex: %s" % ex)
    except Exception as ex:
        sys.stderr.write("record_streams: main:ex: %s" % ex)
        job.join()
        sys.exit()