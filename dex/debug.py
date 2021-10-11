import contextlib
import logging
import io
import pandas as pd

import dex.csv_cache
import dex.csv_parser
import dex.eml_cache
import dex.eml_types
import dex.util

# from flask import current_app as app
import flask

log = logging.getLogger(__name__)


def debug(rid):
    # return

    if not flask.g.debug_panel:
        return

    _csv_df, raw_df, ctx = dex.csv_parser.get_parsed_csv_with_context(rid)

    ctx = N(**ctx)
    # ctx: csv_df, eml_ctx, dialect, column_list, header_line_count, footer_line_count, parser_dict, col_name_list

    attr_dict = dex.eml_cache.get_attributes_as_highlighted_html(rid)
    attr_list = [
        (ctx.col_name_list[k], css_str, html_str) for k, (html_str, css_str) in attr_dict.items()
    ]

    head_df = raw_df.iloc[:100, :]
    tail_df = raw_df.iloc[-100:, :]

    def to_html(**kv):
        """With an index, pandas uses dict keys for column names and dict values for
        each of the index values: {column -> {index -> value}}
        """
        dex.util.logpp(kv, msg="Created HTML from DF", logger=log.debug)
        return pd.DataFrame().from_dict(kv).to_html(escape=False).replace(r'\n', '').strip()

    def expose_keys(d_in):
        out_d = {}
        for i, d in d_in.items():
            for k, v in d.items():
                out_d.setdefault(k, []).append(v)
        return out_d

    buf = io.StringIO()
    col_info_txt = buf.getvalue()

    # flask_g_html = dict_to_kv_html(dict(flask.g))
    flask_g_html = dict_to_html(flask.current_app.config, 'Config', 'Value')

    type_list = [d['pandas_type'] for d in ctx.column_list]
    type_count = count_unique(type_list)

    csv_row_count = dict(
        header_line_count=ctx.header_line_count,
        footer_line_count=ctx.footer_line_count,
    )

    dbg = {
        'flask_g_html': flask_g_html,
        # 'column_list': to_html(),
        # ctx: ctx,
        **{
            'head_html': head_df.to_html(),
            'head_row_count': len(head_df),
            'tail_html': tail_df.to_html(),
            'tail_row_count': len(tail_df),
            'total_row_count': len(raw_df),
            'debug_panel': flask.g.get('debug_panel'),
            'source_html': to_html(
                source_csv={
                    # 'csv_path': ctx.csv_path,
                    # 'raw_line_count': ctx.raw_line_count,
                }
            ),
            'dialect': to_html(
                dialect=dex.csv_parser.get_dialect_as_dict(ctx.dialect),
            ),
            'derived_dtype_html': pd.DataFrame.from_dict(
                {d['col_name']: d for d in ctx.column_list}
            )
            .style.applymap(highlight_types)
            .render(),
            'type_count_html': dict_to_html(type_count, 'Type', 'Count'),
            # 'number_type_count_html': dict_to_html(number_type_count, 'Type', 'Count'),
            'csv_row_count': dict_to_html(csv_row_count, 'Value', 'Count'),
        },
        'col_info_txt': col_info_txt,
        'attr_list': attr_list,
    }

    return dbg


def count_unique(val_list):
    val_list = [str(v) for v in val_list]
    unique_list = list(set(val_list))
    return {k: val_list.count(k) for k in sorted(unique_list)}


def dict_to_html(d, key_name='Key', val_name='Value'):
    d = {key_name: d.keys(), val_name: d.values()}
    d = {k: v for k, v in sorted(d.items())}
    return pd.DataFrame.from_dict(d).to_html()


def highlight_types(val):
    # # bg = '#202020'
    # with contextlib.suppress(Exception):
    #     if isinstance(val, str) and val in HIGHLIGHT_SET:
    #         bg = 'lightgreen'
    #         return f'background-color: {bg}'
    pass


def get_info_as_html(df):
    buf = io.StringIO()
    df_info = pd.DataFrame(columns=['DF INFO'], data=buf.getvalue().split('\n'))
    return df_info.to_html()
