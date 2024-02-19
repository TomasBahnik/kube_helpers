from kubernetes.constants import RESOURCE_RE, units_conversion


def resource_value(value: str | float | int) -> float | int | None:
    if not value or value is None:
        return None
    if type(value) is not str:
        return value
    match = RESOURCE_RE.match(value)
    # only value
    if match.lastindex == 1:
        v = float(match.group(1))
        return v
    # both value and units
    if match.lastindex > 1:
        v = float(match.group(1))
        u = RESOURCE_RE.match(value).group(2)
        # missing unit => no change
        f = float(units_conversion[u]) if u else 1
        return v * f
    else:
        return None
