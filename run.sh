#!/usr/bin/env bash

export FLASK_RUN_PORT=5000
export FLASK_RUN_HOST=127.0.0.1
export PYTHONPATH='${PYTHONPATH}:.:./webapp'
export FLASK_ENV='development'
export FLASK_DEBUG='1'
export WERKZEUG_DEBUG_PIN='off'

flask run
