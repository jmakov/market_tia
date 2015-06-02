import inspect

def getSubclasses(module):
    """
    returns NON-initiated subclasses
    """
    try:
        # get all classes
        classes = []
        for name, member in inspect.getmembers(module):
            if inspect.isclass(member):
                classes.append(member)
                # filter subclasses
        subclassesObjD_ = {}
        for classItem in classes:
            # since exchenges are a subclass of market, we destinguish them with first lower/upper cases
            name = classItem.__name__
            if name[0].islower(): subclassesObjD_[name] = classItem
        return subclassesObjD_
    except Exception: raise