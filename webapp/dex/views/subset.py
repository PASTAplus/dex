import datetime
import json
import logging
import pprint

import flask
import flask.json
import pandas as pd

import db
import dex.csv_cache
import dex.csv_parser
import dex.debug
import dex.eml_cache
import dex.eml_types
import dex.pasta
import util

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
        entity_tup=db.get_entity_as_dict(rid),
        row_count=len(csv_df),
        derived_dtypes_list=ctx['derived_dtypes_list'],
        cat_col_map={
            d['col_name']: d for d in dex.eml_cache.get_categorical_columns(rid)
        },
        filter_not_applied_str='Filter not applied',
    )
    return flask.render_template(
        "subset.html",
        data_url=db.get_entity(rid).data_url,
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
        entity_tup=db.get_entity_as_dict(rid),
        derived_dtypes_list=ctx['derived_dtypes_list'],
        filter_not_applied_str='Filter not applied',
        dbg=dex.debug.debug(rid),
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

    json_str = json.dumps(
        {
            # DataTable
            "draw": draw_int,
            "recordsTotal": total_count,
            "recordsFiltered": filtered_count,
            "data": d,
            # Dex
            "queryResult": result_str,
            "queryIsOk": query_is_ok,
        },
        indent=2,
        sort_keys=True,
    )

    # log.debug('Returning rows to client:')
    # print(json_str)

    return json_str


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
