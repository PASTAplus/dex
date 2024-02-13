import datetime
import pprint

import dex.csv_cache
import dex.csv_parser
import dex.views.subset


def create_subset(rid, csv_df, filter_dict):
    dex.views.subset.log.debug("=" * 100)
    dex.views.subset.log.debug(pprint.pformat({"rid": rid, "filter_dict": filter_dict}))
    dex.views.subset.log.debug("=" * 100)

    # csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    unfiltered_row_count = len(csv_df)

    # Filter rows
    a, b = map(lambda x: x - 1, filter_dict["row_filter"].values())
    if a > 0 or b < unfiltered_row_count - 1:
        dex.views.subset.log.debug(f"Filtering by rows: {a} - {b}")
        csv_df = csv_df[a : b + 1]

    # Filter by category
    for col_idx, cat_list in filter_dict["category_filter"]:
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
        dex.views.subset.log.debug(f'Date range filter not specified')
    else:
        begin_date, end_date = [
            datetime.datetime.strptime(v, "%Y-%m-%d") if v else None for v in (begin_str, end_str)
        ]
        dex.views.subset.log.debug(
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

    query_result = dex.views.subset.get_raw_filtered_by_query(csv_df, filter_dict['query_filter'])
    csv_df = query_result.csv_df

    # Return the raw CSV rows that correspond to the rows we have filtered using the parsed CSV.
    # csv_df = raw_df.iloc[csv_df.index, :]

    csv_df = filter_columns(csv_df, filter_dict)

    # csv_df.index.name = 'index'
    # csv_bytes = csv_df.to_csv(index=filter_dict["column_filter"][0]).encode('utf-8')
    # csv_bytes = csv_df.to_csv().encode('utf-8')

    # return col_list, csv_bytes, csv_df, unfiltered_row_count
    return csv_df


def filter_columns(csv_df, filter_dict):
    col_list = filter_dict['column_filter']['selected_columns']
    if not col_list:
        return csv_df
    dex.views.subset.log.debug(f'Filtering by columns: {", ".join(col_list)}')
    return csv_df.loc[:, col_list]
