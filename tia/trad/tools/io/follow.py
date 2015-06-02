#@author: http://www.dabeaz.com/generators/follow.py
# follow.py
#
# Follow a file like tail -f.
import time
import sys


# is an iterator
def followSim(thefile):
    #thefile.seek(0,2)
    while True:
        line = thefile.readline()
        if not line:
            #time.sleep(1)
            #continue
            raise StopIteration
        yield line

# Example use
# Note : This example requires the use of an apache log simulator.
# 
# Go to the directory run/foo and run the program 'logsim.py' from
# that directory.   Run this program as a background process and
# leave it running in a separate window.  We'll write program
# that read the output file being generated
# 
def followMonitor(thefile, _figure):
    thefile.seek(0,2)
    while True:
        line = thefile.readline()
        if not line:
            _figure.canvas.draw()
            time.sleep(1)
            continue
        yield line

if __name__ == '__main__':
    logfile = open(sys.argv[1], "r")
    loglines = followSim(logfile)
    for line in loglines:
        print line,

