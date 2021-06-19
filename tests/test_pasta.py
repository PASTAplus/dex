import io

import dex.pasta


def test_1000(rid):
    buf = io.BytesIO()
    dex.pasta.download_data_entity(buf, 'https://google.com')
    buf_str = buf.getvalue()
    print(buf_str)
    print(len(buf_str))
