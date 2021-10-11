import datetime
import io
import json
import logging
import pprint
import zipfile

import flask
import flask.json

import dex.csv_cache
import dex.csv_parser
import dex.db
import dex.debug
import dex.eml_cache
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
        csv_html=csv_df.iloc[:DEFAULT_DISPLAY_ROW_COUNT].to_html(
            table_id="csv-table",
            classes="datatable row-border",
            index=True,
            index_names=False,
            border=0,
        ),
        rid=rid,
        entity_tup=dex.db.get_entity_as_dict(rid),
        csv_name=dex.eml_cache.get_csv_name(rid),
        column_list=eml_ctx['column_list'],
        filter_not_applied_str='Filter not applied',
        datetime_col_dict=datetime_col_dict,
        dbg=dex.debug.debug(rid),
        portal_base=dex.pasta.get_portal_base_by_entity(dex.db.get_entity(rid)),
        cat_col_map=cat_col_map,
    )


def get_raw_filtered_by_query(csv_df, raw_df, eml_ctx, query_str=None):
    # Filter the full CSV by the search string. A row is included if one or more of the
    # cells in the row have text matching the search string.
    if not query_str:
        return N(
            raw_df=raw_df,
            status_str='Query not applied',
            query_is_ok=True,
        )
    try:
        query_df = csv_df.query(query_str)
        # log.debug('QUERY_DF')
        # log.debug(query_str)
        # log.debug(len(csv_df))
        # log.debug(len(query_df))
    except Exception as e:
        return N(
            raw_df=raw_df,
            status_str=f'Query error: {e.__class__.__name__}: {str(e)}',
            query_is_ok=False,
        )
    raw_df = raw_df.iloc[query_df.index, :]
    return N(
        raw_df=raw_df,
        status_str=f'Query OK: Selected {len(raw_df)} of {len(csv_df)} rows',
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

    csv_df, _raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    query_result = get_raw_filtered_by_query(csv_df, _raw_df, eml_ctx, query_str)

    # Create page of filtered result for display (selected with the [1], [2]... buttons).
    # Sort the rows according to selection
    if not sort_col_idx:
        query_result.raw_df = query_result.raw_df.sort_index(ascending=is_ascending)
    else:
        query_result.raw_df = query_result.raw_df.rename_axis("__Index").sort_values(
            by=[query_result.raw_df.columns[sort_col_idx - 1], "__Index"],
            ascending=is_ascending,
        )
    page_df = query_result.raw_df[start_int : start_int + row_count]

    j = json.loads(page_df.to_json(orient="split", index=True))
    row_list = [(a, *b) for a, b in zip(j["index"], j["data"])]

    for i in range(len(row_list), 10):
        row_list.append(('', *[''] * len(query_result.raw_df.columns)))

    result_dict = {
        # DataTable
        "draw": draw_int,
        "recordsTotal": len(csv_df),
        "recordsFiltered": len(query_result.raw_df),
        "data": row_list,
        # Dex
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
    args = flask.request.args

    log.debug("=" * 100)
    log.debug(pprint.pformat({"rid": rid, "filter_dict": filter_dict}))
    log.debug("=" * 100)

    draw_int = args.get("draw", type=int)
    start_int = args.get("start", type=int)
    row_count = args.get("length", type=int)
    query_str = args.get("search[value]")

    sort_col_idx = args.get("order[0][column]", type=int)
    is_ascending = args.get("order[0][dir]") == "asc"

    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    query_result = get_raw_filtered_by_query(csv_df, raw_df, eml_ctx, query_str)

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

    # Return the raw CSV rows that correspond to the rows we have filtered using the parsed CSV.
    csv_df = query_result.raw_df.iloc[csv_df.index, :]

    # Filter columns
    col_list = filter_dict["col_filter"][1:]
    if col_list:
        log.error(f'Filtering by columns: {", ".join(map(str, col_list))}')
        # col_name_list = [csv_df.columns[c] for c in col_list]
        csv_df = csv_df.iloc[:, col_list]

    csv_bytes = csv_df.to_csv(index=filter_dict["col_filter"][0])

    # Prepare JSON doc containing the subset params
    json_bytes = flask.json.dumps(
        filter_dict,
        indent=2,
        # sort_keys=True,
        cls=dex.util.DatetimeEncoder,
    )

    log.debug(
        f'Subset created successfully. '
        f'unfiltered_row_count={unfiltered_row_count} '
        f'subset_row_count={len(csv_df)}'
    )

    # Simulate large obj/slow server
    # import time
    # time.sleep(5)

    return send_zip(rid, csv=csv_bytes, json=json_bytes)


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
