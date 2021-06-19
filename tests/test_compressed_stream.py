import pathlib

import lzma

def test_1000():
    with pathlib.Path('~/dex-data/tst.html.xz').expanduser().open('rb') as f:
        with lzma.LZMAFile(f) as z:
            for line in z:
                print(line)
            for line in z:
                print(line)

def test_1010():
    with pathlib.Path('~/dex-data/tst.html.xz').expanduser().open('rb') as f:
        with lzma.LZMAFile(f) as z:
            z.seek(100)
            print(z.read(100))
            print(z.tell())
