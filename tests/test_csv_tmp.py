import pathlib
import time

import dex.csv_tmp


def test_1000(config, tmpdir):
    """_limit_cache_size()
    - Limits to configured size
    - Removes oldest files
    """
    p = config['TMP_CACHE_ROOT'] = pathlib.Path(tmpdir)
    limit = config['TMP_CACHE_LIMIT'] = 3
    p.mkdir(exist_ok=True)

    for i in range(limit * 2):
        (p / f'test_{i}.txt').write_text(f'{i}')
        # Ensure that each file gets a separate timestamp
        time.sleep(0.1)

    assert len(list(p.iterdir())) == limit * 2

    dex.csv_tmp._limit_cache_size()
    assert len(list(p.iterdir())) == limit

    idx_list = sorted([int(p2.read_text()) for p2 in p.iterdir()])
    assert idx_list == [3, 4, 5]
