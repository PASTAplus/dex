"""

"""

import logging

import flask

import db
import dex.csv_cache
import dex.eml_cache
import dex.pasta

log = logging.getLogger(__name__)

eml_blueprint = flask.Blueprint("eml", __name__, url_prefix="/dex/eml")

"""
{#  https://dex.edirepository.org/#}


GET : /metadata/eml/{scope}/{identifier}/{revision}
	
Read Metadata operation, specifying the scope, identifier, and revision of the EML document to be read in the URI.

Revision may be specified as "newest" or "oldest" to retrieve the newest or oldest revision, respectively.
Requests:
Message Body 	MIME type 	Sample Request
none 	none 	curl -i -X GET https://pasta.lternet.edu/package/metadata/eml/knb-lter-lno/1/1
none 	none 	curl -i -X GET https://pasta.lternet.edu/package/metadata/eml/knb-lter-lno/1/newest
none 	none 	curl -i -X GET https://pasta.lternet.edu/package/metadata/eml/knb-lter-lno/1/oldest


# /data/eml/{scope}/{identifier}/{revision}/{entityId}

"""


@eml_blueprint.route("/<rid>")
def view(rid):
    eml_html, eml_css = dex.eml_cache.get_eml_highlighted_html(rid)

    col_name_list = dex.csv_cache.get_ref_col_list(rid)

    return flask.render_template(
        "eml.html",
        t1=dex.eml_cache.get_datetime_columns(rid),
        # base_url=base_url,
        # id_str=id_str,
        # scope_str=scope_str,
        # ver_str=ver_str,
        # rid=rid,
        rid=rid,
        entity_tup=db.get_entity_as_dict(rid),
        eml_html=eml_html,
        eml_css=eml_css,
    )

    # cache_path = flask.current_app.config['CACHE_ROOT_DIR'] / rid
    # if cache_path.exists():
    #     iframe_src = f'/dex/profile/doc/{rid}'
    # else:
    #     iframe_src = f'/dex/profile/generate/{rid}'
    # return flask.render_template(
    #     'profile.html', rid=rid, iframe_src=iframe_src
    # )
    #
