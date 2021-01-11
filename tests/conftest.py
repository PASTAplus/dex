import os
import tempfile

import flask
import pytest

from webapp.main import app


@pytest.fixture(scope='function', autouse=True)
def client():
    # db_fd, app.config['DATABASE'] = tempfile.mkstemp()
    try:
        app.config['TESTING'] = True
        app.config["CSV_ROOT_DIR"] = '/tmp/test/root'

        with app.test_client() as client:
            with app.app_context():
            #     app.init_db()
                yield client

    finally:
        # os.close(db_fd)
        # os.unlink(app.config['DATABASE'])
        pass
