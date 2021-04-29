import logging
import io
import pandas as pd

import dex.csv_cache
import dex.csv_parser
import dex.eml_cache
import dex.eml_types
import util

# from flask import current_app as app
import flask

log = logging.getLogger(__name__)

WATCH_SET = {
    'TYPE_NUM',
    'TYPE_DATE',
    'TYPE_NUM',
    'TYPE_INT',
    'TYPE_CAT',
    *dex.eml_types.DATE_FORMAT_DICT.keys(),
    *dex.eml_types.DATE_FORMAT_DICT.values(),
    *dex.eml_types.DATE_INT_FORMAT_DICT.keys(),
    *dex.eml_types.DATE_INT_FORMAT_DICT.values(),
    'real',
    'integer',
    'whole',
    'natural',
    'float',
    'floating-point',
    'integer',
}


def debug(rid):
    if not flask.g.get('debug_panel', False):
        return dict(
            debug_panel='',
        )

    ctx1 = dex.csv_parser.get_raw_csv_with_context(rid)
    # csv_df = ctx1.csv_df,
    # csv_path = ctx1.csv_path,
    # header_row_idx = ctx1.header_row_idx,
    # header_list = ctx1.header_list,
    # raw_line_count = ctx1.raw_line_count,

    ctx2 = dex.csv_parser.get_csv_context(rid)
    # csv_dialect = ctx2.csv_dialect,
    # csv_path = ctx2.csv_path,
    # derived_dtypes_list = ctx2.derived_dtypes_list,
    # header_list = ctx2.header_list,
    # header_row_idx = ctx2.header_row_idx,
    # parser_dict = ctx2.parser_dict,

    attr_dict = dex.eml_cache.get_attributes_as_highlighted_html(rid)
    attr_list = [
        (ctx2['header_list'][k], css_str, html_str)
        for k, (html_str, css_str) in attr_dict.items()
    ]
    # attr_html = pd.DataFrame.from_dict({'': attr_dict}).to_html(escape=False)

    csv_df = ctx1['csv_df']
    head_df = csv_df.iloc[:100, :]
    tail_df = csv_df.iloc[-100:, :]

    # tuple(tuple(d.items()) + tuple(((None, None),) * (maxlen - len(d))) for d in x)
    # padded_list = tuple(tuple(d.items()) + tuple(((None, None),) * (maxlen - len(d))) for d in derived_dtype_list)
    # dtypes_df = pd.DataFrame.from_dict(padded_list)

    def is_all_scalar(o):
        if isinstance(o, dict):
            return all(isinstance(s, str) for s in o.values())
        elif isinstance(o, list):
            return all(isinstance(s, str) for s in o)
        elif isinstance(o, str):
            return True
        return False

    def to_html(**kv):
        """With an index, pandas uses dict keys for column names and dict values for
        each of the index values.

        {column -> {index -> value}}
        """
        util.logpp(kv)
        return (
            pd.DataFrame()
            .from_dict(kv)
            .to_html(escape=False)
            .replace(r'\n', '')
            .strip()
        )

    def expose_keys(d_in):
        out_d = {}
        for i, d in d_in.items():
            for k, v in d.items():
                out_d.setdefault(k, []).append(v)
        return out_d

    buf = io.StringIO()
    csv_df.info(buf=buf)
    col_info_txt = buf.getvalue()

    dbg = {
        **{
            **ctx1,
            **ctx2,
        },
        **{
            'head_html': head_df.to_html(),
            'head_row_count': len(head_df),
            'tail_html': tail_df.to_html(),
            'tail_row_count': len(tail_df),
            'total_row_count': len(csv_df),
            'debug_panel': flask.g.get('debug_panel'),
            'source_html': to_html(
                csv_path={'': ctx1['csv_path'].as_posix()},
                raw_line_count={'': ctx1['raw_line_count']},
            ),
            'csv_dialect': to_html(
                csv_dialect=dex.csv_parser.get_dialect_as_dict(ctx2['csv_dialect'])
            ),
            'derived_dtype_html': pd.DataFrame.from_dict(
                {
                    d['col_name']: d
                    for d in dex.csv_parser.get_derived_dtypes_from_eml(rid)
                }
            )
            .style.applymap(highlight_types)
            .render(),
            'col_info_txt': col_info_txt,
            'parsers_html': to_html(
                **expose_keys(
                    {ctx1['header_list'][k]: v for k, v in ctx2['parser_dict'].items()}
                ),
            ),
            'formatters_html': to_html(
                **expose_keys(
                    dex.csv_parser.get_formatter_dict(ctx2['derived_dtypes_list'])
                )
            ),
            'attr_list': attr_list,
        },
    }
    return dbg


def highlight_types(val):
    if val in WATCH_SET:
        bg = 'darkgreen'
    else:
        bg = '#202020'

    return f'background-color: {bg}'


def get_info_as_html(df):
    buf = io.StringIO()
    df.info(buf=buf)
    df_info = pd.DataFrame(columns=['DF INFO'], data=buf.getvalue().split('\n'))
    return df_info.to_html()
