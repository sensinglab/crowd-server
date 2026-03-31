def apply(metric):
    mtype = metric.tags.get("type")
    if mtype:
        metric.name = mtype
        metric.tags.pop("type")
    return metric
