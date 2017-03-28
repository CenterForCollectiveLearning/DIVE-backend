def row_to_dict(r, custom_fields=[]):
    d = { c.name: getattr(r, c.name) for c in r.__table__.columns }
    if custom_fields:
        for custom_field in custom_fields:
            d[custom_field] = getattr(r, custom_field)
    return d
