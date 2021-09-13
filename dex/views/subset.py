import datetime
import io
import json
import logging
import pathlib
import pprint
import zipfile

import flask
import flask.json
import pandas as pd

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
    ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    csv_df = ctx['csv_df']
    formatter_dict = dex.csv_parser.get_formatter_dict(ctx['derived_dtypes_list'])
    g_dict = dict(
        rid=rid,
        entity_tup=dex.db.get_entity_as_dict(rid),
        row_count=len(csv_df),
        derived_dtypes_list=ctx['derived_dtypes_list'],
        cat_col_map={d['col_name']: d for d in dex.eml_cache.get_categorical_columns(rid)},
        filter_not_applied_str='Filter not applied',
    )
    return flask.render_template(
        "subset.html",
        data_url=dex.db.get_entity(rid).data_url,
        g_dict=g_dict,
        csv_html=csv_df.iloc[:DEFAULT_DISPLAY_ROW_COUNT].to_html(
            table_id="csv-table",
            classes="datatable row-border",
            formatters=formatter_dict,
            index=True,
            index_names=False,
            border=0,
        ),
        rid=rid,
        entity_tup=dex.db.get_entity_as_dict(rid),
        csv_name=dex.eml_cache.get_csv_name(rid),
        derived_dtypes_list=ctx['derived_dtypes_list'],
        filter_not_applied_str='Filter not applied',
        dbg=dex.debug.debug(rid),
        portal_base=dex.pasta.get_portal_base_by_entity(dex.db.get_entity(rid)),
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
    search_str = args.get("search[value]")

    sort_col_idx = args.get("order[0][column]", type=int)
    is_ascending = args.get("order[0][dir]") == "asc"

    ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    csv_df = ctx['csv_df']

    # csv_df = dex.csv_cache.get_full_csv(rid)
    total_count = len(csv_df)

    # Filter the full CSV by the search string. A row is included if one or more of the
    # cells in the row have text matching the search string.

    filtered_count = len(csv_df)
    query_is_ok = True
    if search_str:
        try:
            csv_df = csv_df.query(search_str)
        except Exception as e:
            result_str = f'{e.__class__.__name__}: {str(e)}'
            query_is_ok = False
        else:
            filtered_count = len(csv_df)
            result_str = f'Query OK -- Subset contains {filtered_count} rows'
    else:
        result_str = 'Filter not applied'

    # Sort the rows according to selection
    if not sort_col_idx:
        csv_df = csv_df.sort_index(ascending=is_ascending)
    else:
        csv_df = csv_df.rename_axis("__Index").sort_values(
            by=[csv_df.columns[sort_col_idx - 1], "__Index"],
            ascending=is_ascending,
        )

    # Get the requested page of results (selected with the [1], [2]... buttons).
    csv_df = csv_df[start_int : start_int + row_count]

    # formatter_dict = dex.csv_parser.get_formatter_dict(ctx.derived_dtypes_list)
    # csv_df.style.format(formatter_dict)

    # csv_df = csv_df.set_index('Name').sort_index(ascending=is_ascending)
    # csv_df = csv_df.sort_values(
    #     csv_df.columns[sort_col_idx], ascending=is_ascending
    # )
    # derived_dtype_list = dex.csv_cache.get_derived_dtype_list(rid)
    # for i, (col_name, col) in enumerate(csv_df.iteritems()):
    #     csv_df.iloc[:, i] = col.apply(dex.eml_types.get_formatter(derived_dtype_list[i]))

    # for c in csv_df.columns:
    # csv_df = csv_df.apply(v for i, v in enumerate(format_mapping.values()))

    # csv_df.style.format(format_mapping)

    # Apply the EML derived formatting to the section of the CSV that will be displayed.
    new_df = dex.csv_parser.apply_formatters(csv_df, ctx['derived_dtypes_list'])

    j = json.loads(csv_df.to_json(orient="split", index=True))
    d = [(a, *b) for a, b in zip(j["index"], j["data"])]

    for i in range(len(d), 10):
        d.append(('', *[''] * len(csv_df.columns)))

    result_dict = {
        # DataTable
        "draw": draw_int,
        "recordsTotal": total_count,
        "recordsFiltered": filtered_count,
        "data": d,
        # Dex
        "queryResult": result_str,
        "queryIsOk": query_is_ok,
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

    csv_df = dex.csv_cache.get_full_csv(rid)
    unfiltered_row_count = len(csv_df)
    derived_dtype_list = dex.csv_parser.get_derived_dtype_list(rid)

    # Filter rows
    a, b = map(lambda x: x - 1, filter_dict["row_filter"].values())
    if a > 0 or b < unfiltered_row_count - 1:
        log.debug(f"Filtering by rows: {a} - {b}")
        csv_df = csv_df[a : b + 1]

    # Filter by category
    for col_idx, cat_list in filter_dict["cat_map"]:
        idx_map = dex.csv_cache.get_categories_for_column(rid, col_idx)
        # cat_set = {idx_map[i] for i in cat_list}
        cat_set = set(cat_list)
        bool_ser = csv_df.iloc[:, col_idx].isin(cat_set)
        csv_df = csv_df.loc[bool_ser]

    # # Filter by category
    # for col_idx, cat_list in filter_dict["cat_map"]:
    #     idx_map = dex.csv_cache.get_categories_for_column(rid, col_idx)
    #     cat_set = {idx_map[i] for i in cat_list}
    #     csv_df = csv_df.iloc[:, col_idx].isin(cat_set)

    # Filter by date range
    date_filter = filter_dict["date_filter"]
    col_idx, begin_str, end_str = (
        date_filter['col_idx'],
        date_filter['start'],
        date_filter['end'],
    )
    if col_idx == -1:
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
        if begin_date or end_date:
            csv_df.iloc[:, col_idx] = pd.to_datetime(
                csv_df.iloc[:, col_idx].apply(str),
                # infer_datetime_format=True,
                errors='ignore',
                format=derived_dtype_list[col_idx].c_date_fmt_str,
            )
            # csv_df.iloc[:, col_idx] = pd.tz_localize(None)

        if begin_date and end_date:
            csv_df = csv_df[
                [(begin_date <= x.tz_localize(None) <= end_date) for x in csv_df.iloc[:, col_idx]]
            ]
        elif begin_date:
            csv_df = csv_df[[(begin_date <= x.tz_localize(None)) for x in csv_df.iloc[:, col_idx]]]
        elif end_date:
            csv_df = csv_df[[(x.tz_localize(None) <= end_date) for x in csv_df.iloc[:, col_idx]]]

    # Filter columns
    col_list = filter_dict["col_filter"][1:]
    if col_list:
        log.debug(f'Filtering by columns: {", ".join(map(str, col_list))}')
        # col_name_list = [csv_df.columns[c] for c in col_list]
        csv_df = csv_df.iloc[:, col_list]

    log.debug(
        f'Subset created successfully. '
        f'unfiltered_row_count={unfiltered_row_count} '
        f'subset_row_count={len(csv_df)}'
    )

    # Simulate large obj/slow server
    # import time
    # time.sleep(5)

    csv_bytes = csv_df.to_csv(index=filter_dict["col_filter"][0])
    # json_bytes = flask.json.htmlsafe_dumps(filter_dict)
    json_bytes = flask.json.dumps(
        filter_dict,
        indent=2,
        # sort_keys=True,
        cls=dex.util.DatetimeEncoder,
    )

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


# def parse_date(s):
#     try:
#         x = maya.parse(s).epoch
#         # print(x)
#         return x
#     except ValueError:
#         return 0.0


def get_package_id(purl: str) -> str:
    path_frags = purl.split("/")
    scope = path_frags[-4]
    identifier = path_frags[-3]
    revision = path_frags[-2]
    package_id = f"{scope}.{identifier}.{revision}"
    return package_id


# def cell_formatter(x):
#     """Formatter that is applied to each cell in a DataFrame when rendering to HTML"""
#     print('1'*100)
#     print(x)
#     if isinstance(x, np.float):
#         return f"{x:.02f}"
#     else:
#         return str(x)
