import tia.trad.tools.arithm.floatArithm as Mfa


class exactFloat:

    def __init__(self, _head, _tail):
        self.head = _head
        self.tail = _tail
    def __repr__(self): return str(self.head) + str(self.tail)
    def __add__(self, other):pass
    def __sub__(self, other):pass
    def __mul__(self, other):pass
    def __div__(self, other):pass

def mul(int1, pr1, int2, pr2, return_pr):
    try:
        p1 = str(10**pr1); f1 = Mfa.D(str(int1)) / Mfa.D(p1)
        p2 = str(10**pr2); f2 = Mfa.D(str(int2)) / Mfa.D(p2)
        fRes = f1 * f2
        fretPr = str(10**return_pr)
        res_ = int(fRes * Mfa.D(fretPr))
        return res_
    except Exception: raise

def div(int1, pr1, int2, pr2, return_pr):
    try:
        p1 = str(10**pr1); f1 = Mfa.D(str(int1)) / Mfa.D(p1)
        p2 = str(10**pr2); f2 = Mfa.D(str(int2)) / Mfa.D(p2)
        fRes = f1 / f2
        fretPr = str(10**return_pr)
        res_ = int(fRes * Mfa.D(fretPr))
        return res_
    except Exception: raise