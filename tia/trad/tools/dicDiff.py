import tia.trad.tools.ipc.naming_conventions as names

class DictDiff(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)
    def added(self):
        return self.set_current - self.intersect
    def removed(self):
        return self.set_past - self.intersect
    def changed(self):
        return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])
    def unchanged(self):
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])

def orderbookComparison(_currOrdb, _prevOrdb):
    try:
        diff_ = {names.asks: {names.added: None, names.removed: None, names.changed: None},
                   names.bids: {names.added: None, names.removed: None, names.changed: None}}
        # get deltas
        tmpAC = {}; tmpAP = {}; tmpBC = {}; tmpBP = {}
        for tpl in _currOrdb[names.asks]: tmpAC[tpl[0]] = tpl[1]
        for tpl in _prevOrdb[names.asks]: tmpAP[tpl[0]] = tpl[1]

        for tpl in _currOrdb[names.bids]: tmpBC[tpl[0]] = tpl[1]
        for tpl in _prevOrdb[names.bids]: tmpBP[tpl[0]] = tpl[1]


        diffAsks = DictDiff(tmpAC, tmpAP)
        diffBids = DictDiff(tmpBC, tmpBP)

        decisionD = {names.asks: diffAsks, names.bids: diffBids}
        for side in decisionD:
            diff_[side][names.added] = decisionD[side].added()
            diff_[side][names.removed] = decisionD[side].removed()
            diff_[side][names.changed] = decisionD[side].changed()

        diff_[names.asks]["curr"] = tmpAC; diff_[names.asks]["prev"] = tmpAP
        diff_[names.bids]["curr"] = tmpBC; diff_[names.bids]["prev"] = tmpBP
        return diff_
    except Exception: raise
