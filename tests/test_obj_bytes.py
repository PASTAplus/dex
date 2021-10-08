import pathlib
import time

import dex.obj_bytes
import dex.pasta


def mk_data_url(scope_str, id_int, ver_int, entity_str):
    return (
        f'https://pasta.lternet.edu/package/data/eml/'
        f'{scope_str}/{id_int}/{ver_int}/{entity_str}'
    )


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

    dex.obj_bytes._limit_cache_size()
    assert len(list(p.iterdir())) == limit

    idx_list = sorted([int(p2.read_text()) for p2 in p.iterdir()])
    assert idx_list == [3, 4, 5]


def test_1010():
    data_url = mk_data_url('knb-lter-jrn', 210548066, 3, '80fd39c29af98f1158cab58c2c598a67')
    entity_tup = dex.pasta.get_entity_by_data_url(data_url)
    obj_stream = dex.obj_bytes._open_obj(entity_tup, is_eml=False)
    print(obj_stream)
