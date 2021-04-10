import datetime
import json
import logging
import pprint

import flask
import maya
import numpy as np
import pandas as pd

import db
import dex.csv_cache
import dex.eml_cache
import dex.pasta

log = logging.getLogger(__name__)

subset_blueprint = flask.Blueprint("subset", __name__, url_prefix="/dex/subset")


@subset_blueprint.route("/<rid>", methods=["GET"])
def subset(rid):
    csv_df = dex.csv_cache.get_full_csv(rid)
    return flask.render_template(
        "subset.html",
        rid=rid,
        entity_tup=(db.get_entity_as_dict(rid)),
        row_count=len(csv_df),
        ref_col_list=dex.csv_cache.get_ref_col_list(rid),
        datetime_col_list=dex.eml_cache.get_datetime_columns(rid),
        cat_col_map=dex.csv_cache.get_categorical_columns(rid),
        prof_type_list=json.dumps(dex.eml_cache.get_profiling_types(rid)),
        data_url=db.get_entity(rid).data_url,
        csv_html=csv_df.iloc[:10].to_html(
            table_id="csv-table",
            classes="datatable row-border",
            formatters=[cell_formatter for _ in csv_df.columns + ["u"]],
            index=True,
            index_names=False,
            border=0,
        ),
    )


@subset_blueprint.route("/<rid>", methods=["POST"])
def download(rid):
    filter_dict = json.loads(flask.request.data)

    log.debug("=" * 100)
    log.debug(pprint.pformat({"rid": rid, "filter_dict": filter_dict}))
    log.debug("=" * 100)

    csv_df = dex.csv_cache.get_full_csv(rid)
    unfiltered_row_count = len(csv_df)

    # Filter rows
    a, b = map(lambda x: x - 1, filter_dict["row_filter"])
    if a > 0 or b < unfiltered_row_count - 1:
        log.debug(f"Filtering by rows: {a} - {b}")
        csv_df = csv_df[a : b + 1]

    # Filter by category
    for col_idx, cat_list in filter_dict["cat_map"]:
        idx_map = dex.csv_cache.get_categories_for_column(rid, col_idx)
        cat_set = {idx_map[i] for i in cat_list}
        csv_df = csv_df.loc[csv_df[col_idx].isin(cat_set)]

    # Filter by date range
    date_filter = filter_dict["date_filter"]
    col_idx, start_str, end_str = (
        date_filter['col_idx'],
        date_filter['start'],
        date_filter['end'],
    )
    if col_idx == -1:
        log.debug(f'Date range filter not specified')
    else:
        start_date, end_date = [
            datetime.datetime.strptime(v, "%Y-%m-%d") if v else None
            for v in (start_str, end_str)
        ]
        log.debug(
            f'Filtering by date range: '
            f'{start_date.isoformat() if start_date else "<unset>"} - '
            f'{end_date.isoformat() if end_date else "<unset>"}'
        )
        if start_date or end_date:
            csv_df.iloc[:, col_idx] = pd.to_datetime(
                csv_df.iloc[:, col_idx].apply(str),
                infer_datetime_format=True,
                errors='ignore',
            )
            # csv_df.iloc[:, col_idx] = pd.tz_localize(None)

        if start_date and end_date:
            csv_df = csv_df[
                [
                    (start_date <= x.tz_localize(None) <= end_date)
                    for x in csv_df.iloc[:, col_idx]
                ]
            ]
        elif start_date:
            csv_df = csv_df[
                [(start_date <= x.tz_localize(None)) for x in csv_df.iloc[:, col_idx]]
            ]
        elif end_date:
            csv_df = csv_df[
                [(x.tz_localize(None) <= end_date) for x in csv_df.iloc[:, col_idx]]
            ]

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

    return flask.Response(
        csv_df.to_csv(index=filter_dict["col_filter"][0]),
        mimetype="text/csv",
        headers={"Content-disposition": f"attachment; filename={rid}.csv"},
    )


@subset_blueprint.route("/fetch-browse/<rid>")
def csv_fetch(rid):
    """
    http://127.0.0.1:5000/dex/csv_fetch?
        draw=1

        columns[0][data]=0
        columns[0][name]=
        columns[0][searchable]=true
        columns[0][orderable]=true
        columns[0][search][value]=
        columns[0][search][regex]=false

        columns[1][data]=1
        columns[1][name]=
        columns[1][searchable]=true
        columns[1][orderable]=true
        columns[1][search][value]=
        columns[1][search][regex]=false

        ...

        order[0][column]=0
        order[0][dir]=asc

        start=0
        length=10

        search[value]=
        search[regex]=false

        _=1597606359676
    """
    # return

    # log.info(pprint.pformat(flask.request.args, indent=2, sort_dicts=True))

    args = flask.request.args
    draw_int = args.get("draw", type=int)
    start_int = args.get("start", type=int)
    row_count = args.get("length", type=int)
    search_str = args.get("search[value]")
    sort_col_idx = args.get("order[0][column]", type=int)
    is_ascending = args.get("order[0][dir]") == "asc"

    csv_df = dex.csv_cache.get_full_csv(rid)
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
    csv_df = csv_df[start_int : len(csv_df) if not row_count else start_int + row_count]

    # csv_df = csv_df.set_index('Name').sort_index(ascending=is_ascending)
    # csv_df = csv_df.sort_values(
    #     csv_df.columns[sort_col_idx], ascending=is_ascending
    # )

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

    # Simulate large obj/slow server
    # import time
    # time.sleep(5)

    return json.dumps(res_list)


def parse_date(s):
    try:
        x = maya.parse(s).epoch
        # print(x)
        return x
    except ValueError:
        return 0.0


def get_package_id(purl: str) -> str:
    path_frags = purl.split("/")
    scope = path_frags[-4]
    identifier = path_frags[-3]
    revision = path_frags[-2]
    package_id = f"{scope}.{identifier}.{revision}"
    return package_id


def cell_formatter(x):
    """Formatter that is applied to each cell in a DataFrame when rendering to HTML"""
    if isinstance(x, np.float):
        return f"{x:.02f}"
    else:
        return str(x)
