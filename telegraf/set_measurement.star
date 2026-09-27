def apply(metric):
    mtype = metric.tags.get("type")

    if mtype == "replay_lora":
        metric.name = "replay_lora"
    elif mtype:
        metric.name = mtype

    if mtype:
        metric.tags.pop("type")

    return metric