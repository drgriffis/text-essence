from io import BytesIO
import base64

def prettify(value, decimals=2):
    if type(value) is float:
        fmt = '{0:.%df}' % decimals
    elif type(value) is int:
        fmt = '{0:,}'
    else:
        fmt = '{0}'
    return fmt.format(value)

def renderImage(func, args, kwargs):
    stream = BytesIO()
    func(*args, outf=stream, **kwargs)
    stream.seek(0)
    base64_data = base64.b64encode(stream.getvalue())
    return base64_data.decode('utf8')
