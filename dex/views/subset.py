import datetime
import hashlib
import io
import json
import logging
import math
import pprint
import zipfile

import flask
import flask.json
import pandas as pd
from flask import current_app as app

import dex.csv_cache
import dex.csv_parser
import dex.db
import dex.debug
import dex.eml_cache
import dex.eml_subset
import dex.eml_types
import dex.pasta
import dex.util

log = logging.getLogger(__name__)

subset_blueprint = flask.Blueprint("subset", __name__, url_prefix="/dex/subset")

DEFAULT_DISPLAY_ROW_COUNT = 10


@subset_blueprint.route("/<rid>", methods=["GET"])
def subset(rid):
    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    datetime_col_dict = dex.csv_cache.get_datetime_col_dict(csv_df)
    cat_col_map = {d['col_name']: d for d in dex.eml_cache.get_categorical_columns(rid)}

    note_list = []
    if len(csv_df) == app.config['CSV_MAX_CELLS'] // len(eml_ctx['column_list']):
       note_list.append('Due to size, only the first part of this table is available in DeX')

    return flask.render_template(
        "subset.html",
        data_url=dex.db.get_entity(rid).data_url,
        g_dict=dict(
            rid=rid,
            entity_tup=dex.db.get_entity_as_dict(rid),
            row_count=len(csv_df),
            column_list=eml_ctx['column_list'],
            cat_col_map=cat_col_map,
            filter_not_applied_str='Filter not applied',
            datetime_col_dict=datetime_col_dict,
        ),
        # Generate a table with just populated headers. The rows are filled in
        # dynamically.
        csv_html=pd.DataFrame(columns=csv_df.columns).to_html(
            table_id="csv-table",
            classes="datatable row-border",
            index=True,
            index_names=False,
            border=0,
        ),
        column_list=eml_ctx['column_list'],
        filter_not_applied_str='Filter not applied',
        datetime_col_dict=datetime_col_dict,
        cat_col_map=cat_col_map,
        # For the base template, should be included in all render_template() calls.
        rid=rid,
        entity_tup=dex.db.get_entity_as_dict(rid),
        csv_name=dex.eml_cache.get_csv_name(rid),
        dbg=dex.debug.debug(rid),
        portal_base=dex.pasta.get_portal_base_by_entity(dex.db.get_entity(rid)),
        note_list=note_list,
    )


def get_raw_filtered_by_query(csv_df, eml_ctx, query_str=None):
    # Filter the full CSV by the search string. A row is included if one or more of the
    # cells in the row have text matching the search string.
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
    query_result = get_raw_filtered_by_query(csv_df, eml_ctx, query_str)
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
            # c.append(x and not v)

            # A cell has failed parsing if the parsed value is NaN while the raw value
            # is set and is not in the EML Missing Code List.

            if raw_v and raw_v in eml_ctx['missing_code_set']:
                is_invalid = False
            else:
                is_invalid = (
                    parsed_v is None
                    or parsed_v == ''
                    or (isinstance(parsed_v, float) and math.isnan(parsed_v))
                )

            c.append(is_invalid)
        bad_list.append(c)

    j = json.loads(page_df.to_json(orient="split", index=True))
    row_list = [(a, *b) for a, b in zip(j["index"], j["data"])]

    for i in range(len(row_list), 10):
        row_list.append(('', *[''] * len(raw_df.columns)))

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

    log.debug("=" * 100)
    log.debug(pprint.pformat({"rid": rid, "filter_dict": filter_dict}))
    log.debug("=" * 100)

    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    unfiltered_row_count = len(csv_df)

    # Filter rows
    a, b = map(lambda x: x - 1, filter_dict["row_filter"].values())
    if a > 0 or b < unfiltered_row_count - 1:
        log.debug(f"Filtering by rows: {a} - {b}")
        csv_df = csv_df[a : b + 1]

    # Filter by category
    for col_idx, cat_list in filter_dict["cat_map"]:
        idx_map = dex.csv_cache.get_categories_for_column(rid, col_idx)
        cat_set = set(cat_list)
        bool_ser = csv_df.iloc[:, col_idx].isin(cat_set)
        csv_df = csv_df.loc[bool_ser]

    # Filter by date range
    date_filter = filter_dict["date_filter"]
    col_name, begin_str, end_str = (
        date_filter['col_name'],
        date_filter['start'],
        date_filter['end'],
    )
    if col_name == '':
        log.debug(f'Date range filter not specified')
    else:
        begin_date, end_date = [
            datetime.datetime.strptime(v, "%Y-%m-%d") if v else None for v in (begin_str, end_str)
        ]
        log.debug(
            f'Filtering by date range: '
            f'{begin_date.isoformat() if begin_date else "<unset>"} - '
            f'{end_date.isoformat() if end_date else "<unset>"}'
        )
        if begin_date and end_date:
            csv_df = csv_df[
                [(begin_date <= x.tz_localize(None) <= end_date) for x in csv_df[col_name]]
            ]
        elif begin_date:
            csv_df = csv_df[[(begin_date <= x.tz_localize(None)) for x in csv_df.iloc[col_name]]]
        elif end_date:
            csv_df = csv_df[[(x.tz_localize(None) <= end_date) for x in csv_df.iloc[col_name]]]


    query_result = get_raw_filtered_by_query(csv_df, eml_ctx, filter_dict['query_filter'])
    csv_df = query_result.csv_df

    # Filter columns
    col_list = filter_dict["col_filter"][1:]
    if col_list:
        log.error(f'Filtering by columns: {", ".join(map(str, col_list))}')
        csv_df = csv_df.iloc[:, col_list]

    # Return the raw CSV rows that correspond to the rows we have filtered using the parsed CSV.
    csv_df = raw_df.iloc[csv_df.index, :]

    csv_df.index.name = 'index'

    # csv_bytes = csv_df.to_csv(index=filter_dict["col_filter"][0]).encode('utf-8')
    csv_bytes = csv_df.to_csv().encode('utf-8')

    # Prepare JSON doc containing the subset params
    json_bytes = flask.json.dumps(
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
        col_list=col_list,
    )

    log.debug(
        f'Subset created successfully. '
        f'unfiltered_row_count={unfiltered_row_count} '
        f'subset_row_count={len(csv_df)}'
    )

    return send_zip(rid, csv=csv_bytes, json=json_bytes, eml=eml_str)


def send_zip(zip_name, **zip_dict):
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, mode='w') as z:
        for f_name, f_bytes in zip_dict.items():
            z.writestr(f_name, f_bytes)
    zip_bytes.seek(0)
    return flask.send_file(
        zip_bytes,
        mimetype='application/zip',
        as_attachment=True,
        attachment_filename=f'{zip_name}.zip',
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
