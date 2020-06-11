def prettify(value, decimals=2):
    if type(value) is float:
        fmt = '{0:.%df}' % decimals
    elif type(value) is int:
        fmt = '{0:,}'
    else:
        fmt = '{0}'
    return fmt.format(value)
