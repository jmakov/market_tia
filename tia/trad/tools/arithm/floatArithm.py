"""
assume ALL (-exceptions) prices are broadcasted to MAX 5 decimal places
and amounts to MAX 8 decimal places
"""
import sys
interpreter = sys.version
if "PyPy" in interpreter: import decimal as cd
else: import cdecimal as cd

exceptions = {"mtgoxJPY: 3"}    #which market broadcast at different accuracy

D = cd.Decimal
#ED6 = D("1000000")
QP = D("1.00000")
QA = D("1.00000000")
cd.getcontext().prec = 18


"""
def div(x, y, fp=TWOPLACES):
     return (x / y).quantize(fp)
"""