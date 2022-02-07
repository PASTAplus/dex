"""View for syntax highlighted and pretty printed EML
"""

import logging
import pprint

import flask

import dex.db
import dex.csv_cache
import dex.debug
import dex.eml_cache
import dex.pasta

log = logging.getLogger(__name__)

eml_blueprint = flask.Blueprint("eml", __name__, url_prefix="/dex/eml")

"""
{# https://dex.edirepository.org/ #}


GET : /metadata/eml/{scope}/{identifier}/{revision}
	
Read Metadata operation, specifying the scope, identifier, and revision of the EML document to be read in the URI.

Revision may be specified as "newest" or "oldest" to retrieve the newest or oldest revision, respectively.

Requests:

Message Body 	MIME type 	Sample Request
none 	none 	curl -i -X GET https://pasta.lternet.edu/package/metadata/eml/knb-lter-lno/1/1
none 	none 	curl -i -X GET https://pasta.lternet.edu/package/metadata/eml/knb-lter-lno/1/newest
none 	none 	curl -i -X GET https://pasta.lternet.edu/package/metadata/eml/knb-lter-lno/1/oldest

# /data/eml/{scope}/{identifier}/{revision}/{entityId
"""


@eml_blueprint.route("/<rid>")
def view(rid):
    eml_html, eml_css = dex.eml_cache.get_eml_as_highlighted_html(rid)

    # dbg = dex.eml_cache.get_breakdown(rid)
    # dbg = ''

    return flask.render_template(
        "eml.html",
        t1=dex.eml_cache.get_datetime_columns(rid),
        g_dict={eml_html: eml_html, eml_css: eml_css},
        eml_html=eml_html,
        eml_css=eml_css,
        # For the base template, should be included in all render_template() calls.
        rid=rid,
        entity_tup=dex.db.get_entity_as_dict(rid),
        csv_name=dex.eml_cache.get_csv_name(rid),
        dbg=dex.debug.debug(rid),
        portal_base=None,
        note_list=[],
    )
