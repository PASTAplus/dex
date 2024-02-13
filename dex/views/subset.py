import hashlib
import io
import json
import logging
import math
import pathlib
import re
import zipfile

import flask
import pandas as pd

import dex.csv_cache
import dex.csv_parser
import dex.db
import dex.debug
import dex.eml_cache
import dex.eml_extract
import dex.eml_subset
import dex.pasta
import dex.util
import dex.views.util

log = logging.getLogger(__name__)

subset_blueprint = flask.Blueprint("subset", __name__, url_prefix="/dex/subset")

DEFAULT_DISPLAY_ROW_COUNT = 10


@subset_blueprint.route("/<rid>", methods=["GET"])
def subset(rid):
    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    datetime_col_dict = dex.csv_cache.get_datetime_col_dict(csv_df, eml_ctx)
    cat_col_map = {d['col_name']: d for d in dex.eml_cache.get_categorical_columns(rid)}
    # Copy the fields that we need to transfer to the client, excluding fields that
    # cannot be represented in JSON.
    column_list = [
        dict(
            col_idx=d['col_idx'],
            col_name=d['col_name'],
            pandas_type=d['pandas_type'],
        )
        for d in eml_ctx['column_list']
    ]

    note_list = []
    if len(csv_df) == flask.current_app.config['CSV_MAX_CELLS'] // len(column_list):
        note_list.append('Due to size, only the first part of this table is available in DeX')

    # Create an empty HTML table to fill in dynamically.
    empty_df = pd.DataFrame(
        columns=csv_df.columns,
    )
    empty_df.columns.name = 'Index'
    csv_html = empty_df.to_html(
        table_id="csv-table",
        classes="datatable cell-border",
        index=True,
        index_names=True,
        border=0,
    )
    return flask.render_template(
        "subset.html",
        dist_url=dex.db.get_entity(rid).dist_url,
        g_dict=dict(
            rid=rid,
            pkg_id=dex.eml_cache.get_pkg_id_dict(rid),
            row_count=len(csv_df),
            column_list=column_list,
            cat_col_map=cat_col_map,
            filter_not_applied_str='Filter not applied',
            datetime_col_dict=datetime_col_dict,
        ),
        # Generate a table with just populated headers. The rows are filled in
        # dynamically.
        csv_html=csv_html,
        column_list=column_list,
        filter_not_applied_str='Filter not applied',
        datetime_col_dict=datetime_col_dict,
        cat_col_map=cat_col_map,
        # For the base template, should be included in all render_template() calls.
        rid=rid,
        data_url=dex.db.get_data_url(rid),
        pkg_id=dex.eml_cache.get_pkg_id_dict(rid),
        csv_name=dex.eml_cache.get_csv_name(rid),
        portal_base=dex.pasta.get_portal_base(dex.db.get_dist_url(rid)),
        note_list=note_list,
        is_on_pasta=dex.pasta.is_on_pasta(dex.db.get_meta_url(rid)),
        dbg=dex.debug.debug(rid),
    )


def get_raw_filtered_by_query(csv_df, query_str=None):
    # Filter the full CSV by the query string.
    if not query_str:
        return N(
            csv_df=csv_df,
            status_str='Query not applied',
            query_is_ok=True,
        )
    try:
        query_df = csv_df.query(query_str)
    except Exception as e:
        return N(
            csv_df=csv_df,
            status_str=f'Query error: {e.__class__.__name__}: {str(e)}',
            query_is_ok=False,
        )
    return N(
        csv_df=query_df,
        status_str=f'Query OK: Selected {len(query_df)} of {len(csv_df)} rows',
        query_is_ok=True,
    )


@subset_blueprint.route("/fetch-browse/<rid>")
def csv_fetch(rid):
    """
    http://127.0.0.1:5000/dex/csv_fetch?
        draw=1
        columns[0][data]=0
        columns[0][name]=
        ...
        columns[1][data]=1
        columns[1][name]=0
        ...
        order[0][column]=0
        order[0][dir]=asc
        start=0
        length=10
        search[value]=
        search[regex]=false
        _=1597606359676
    """
    # log.debug(pprint.pformat(flask.request.args, indent=2, sort_dicts=True))

    args = flask.request.args

    draw_int = args.get("draw", type=int)
    start_int = args.get("start", type=int)
    row_count = args.get("length", type=int)
    query_str = args.get("search[value]")

    sort_col_idx = args.get("order[0][column]", type=int)
    is_ascending = args.get("order[0][dir]") == "asc"

    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    query_result = get_raw_filtered_by_query(csv_df, query_str)
    csv_df = query_result.csv_df

    # For the remainder of this function, we deal only with raw_df, which contains
    # unparsed strings.

    raw_df = raw_df.iloc[csv_df.index, :]

    # Create page of filtered result for display (selected with the [1], [2]... buttons).
    # Sort the rows according to selection
    if not sort_col_idx:
        raw_df = raw_df.sort_index(ascending=is_ascending)
    else:
        raw_df = raw_df.rename_axis("__Index").sort_values(
            by=[raw_df.columns[sort_col_idx - 1], "__Index"],
            ascending=is_ascending,
        )

    page_df = raw_df[start_int : start_int + row_count]

    # Create table of cells for which to show the parse error notice.
    bad_list = []
    # Rows
    for i in range(page_df.shape[0]):
        c = []
        # Columns
        for j in range(page_df.shape[1]):
            raw_v = page_df.iat[i, j]
            parsed_v = csv_df.iat[start_int + i, j]
            # A cell has failed parsing if the parsed value is NaN while the raw value
            # is not in the EML Missing Code set or the set of common known NaN codes.
            if (
                raw_v in eml_ctx['column_list'][j]['missing_code_list']
                or raw_v in flask.current_app.config['CSV_NAN_SET']
            ):
                is_invalid = False
            else:
                is_invalid = parsed_v is None or (
                    isinstance(parsed_v, float) and math.isnan(parsed_v)
                )
            c.append(is_invalid)
        bad_list.append(c)

    j = json.loads(page_df.to_json(orient="split", index=True))
    row_list = [(a, *b) for a, b in zip(j["index"], j["data"])]

    for i in range(len(row_list), DEFAULT_DISPLAY_ROW_COUNT):
        row_list.append(('', *[''] * len(raw_df.columns)))
        bad_list.append([False] * len(raw_df.columns))

    row_dict_list = [{'val': v} for v in row_list]

    result_dict = {
        # DataTable
        "draw": draw_int,
        "recordsTotal": len(csv_df),
        "recordsFiltered": len(raw_df),
        "data": row_list,
        "bad": bad_list,
        # "newdata": row_dict_list,
        # DeX
        "queryResult": query_result.status_str,
        "queryIsOk": query_result.query_is_ok,
    }

    # util.logpp(result_dict, 'Returning to client', log.debug)

    return json.dumps(
        result_dict,
        cls=dex.util.DatetimeEncoder,
    )


@subset_blueprint.route("/<rid>", methods=["POST"])
def download(rid):
    filter_dict = json.loads(flask.request.data)
    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    unfiltered_row_count = len(csv_df)
    csv_df = dex.views.util.create_subset(rid, csv_df, filter_dict)
    # Return the raw CSV rows that correspond to the rows we have filtered using the parsed CSV.
    csv_df = raw_df.iloc[csv_df.index, :]
    # Filter columns from the raw df
    csv_df = dex.views.util.filter_columns(csv_df, filter_dict)
    #
    csv_bytes = csv_df.to_csv(
        index=filter_dict["column_filter"]['index'],
        index_label='Index',
    ).encode('utf-8')

    # Prepare JSON doc containing the subset params
    json_bytes = json.dumps(
        filter_dict,
        indent=2,
        # sort_keys=True,
        cls=dex.util.DatetimeEncoder,
    )

    # Simulate large obj/slow server
    # import time
    # time.sleep(5)

    eml_str = dex.eml_subset.create_subset_eml(
        rid,
        row_count=len(csv_df),
        byte_count=len(csv_bytes),
        md5_checksum=hashlib.md5(csv_bytes).hexdigest(),
        col_list=filter_dict['column_filter']['selected_columns'],
    )

    log.debug(
        f'Subset created successfully. '
        f'unfiltered_row_count={unfiltered_row_count} '
        f'subset_row_count={len(csv_df)}'
    )

    unsafe_name_str = dex.eml_cache.get_csv_name(rid)
    safe_name_str = re.sub('[^a-zA-Z0-9._-]+', '-', unsafe_name_str)
    safe_base_path = pathlib.Path(safe_name_str)

    return send_zip(
        zip_name=dex.eml_cache.get_pkg_id_str(rid),
        zip_dict={
            safe_base_path.with_suffix('.csv').name: csv_bytes,
            safe_base_path.with_suffix('.subset.json').name: json_bytes,
            safe_base_path.with_suffix('.eml.xml').name: eml_str.encode('utf-8', errors='replace'),
        },
    )


def send_zip(zip_name, zip_dict):
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, mode='w') as z:
        for f_name, f_bytes in zip_dict.items():
            z.writestr(f_name, f_bytes)
    zip_bytes.seek(0)
    return flask.send_file(
        zip_bytes,
        mimetype='application/zip',
        as_attachment=True,
        # attachment_filename=f'{zip_name}.zip',
        download_name=f'{zip_name}.zip',
    )


# noinspection PyTypeChecker
@subset_blueprint.route("/fetch-category/<rid>/<col_idx>")
def fetch_category(rid, col_idx):
    """Return a list of the unique values in a column. This will only be called for
    columns that have already been determined to be categorical.
    """
    res_list = dex.csv_cache.get_categories_for_column(rid, col_idx)

    log.debug('res_list', res_list)

    # Simulate large obj/slow server
    # import time
    # time.sleep(5)

    json_str = json.dumps(list(res_list))
    # json_str = json.dumps(list(res_list), cls=util.DatetimeEncoder)
    log.debug('json_str', json_str)
    return json_str


def get_package_id(purl: str) -> str:
    path_frags = purl.split("/")
    scope = path_frags[-4]
    identifier = path_frags[-3]
    revision = path_frags[-2]
    package_id = f"{scope}.{identifier}.{revision}"
    return package_id
