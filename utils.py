import hashlib


def md5_hash(tup):
    m = hashlib.md5()
    for element in tup:
        element = str(element)
        element = element.encode('utf-8')
        m.update(element)
    return int(m.hexdigest()[:8], 16)