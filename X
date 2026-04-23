from decimal import Decimal, InvalidOperation

def to_decimal(value, default="0"):
    if value is None or value == "":
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError):
        return Decimal(default)


def quantize_money(value):
    return value.quantize(Decimal("0.01"))
