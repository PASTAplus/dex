/*
  Misc utilities
 */

// TYPE_CAT            - categorical
// TYPE_BOOL           - boolean
// TYPE_NUM            - numeric
// TYPE_DATE           - date
// TYPE_URL            - URL
// TYPE_COMPLEX        -
// TYPE_PA             - Absolute path
// TYPE_FILE           - File (existing path)
// TYPE_IMAGE          - Image
// S_TYPE_UNSUPPORTED  - unsupported
// const derived_dtypes_list = g.derived_dtypes_list

// dtype_dict = {
//     'col_idx': col_idx,
//     'col_name': col_name,
//     'type_str': 'S_TYPE_UNSUPPORTED',
//     'storage_type': storage_type,
//     'date_fmt_str': date_fmt_str,
//     'c_date_fmt_str': None,
//     'number_type': number_type,
//     'numeric_domain': numeric_domain,
//     'ratio': ratio,
//     'missing_value_list': missing_value_list,
//     'col_agg_dict': col_agg_dict,
// }

export async function get_selected_str(sel_el)
{
  return sel_el.find('option:selected').text();
  // return $('#time-period-filter option:selected').text();
}

// Return array of column indexes for selected columns
export async function get_selected_arr(sel_el)
{
  return sel_el.find('option:selected').toArray().map(item => parseInt(item.value));
}

export function get_categorical_columns()
{
  return _get_columns_by_type('TYPE_CAT')
}

export function get_numeric_columns()
{
  return _get_columns_by_type('TYPE_NUM')
}

export function get_datetime_columns()
{
  return _get_columns_by_type('TYPE_DATE')
}

// Also converts a list of dict to a dict of dicts. Keys in the outer dict are column index integers.

function _get_columns_by_type(type_str)
{
  return Object.fromEntries(
    Object.entries(g.derived_dtypes_list).filter(([k, v]) => v
      ['type_str'] === type_str
    )
  );
  // return g.derived_dtypes_list.filter(
  //     dtype_dict => dtype_dict['type_str'] === type_str
  // );
}

// Return a new dict in which an attribute of the value is the key. Keys in the
// source dict are discarded.
export function rekey(d, new_key) {
  return Object.fromEntries(Object.values(d).map(x => [x[new_key], x]));
}

// Return list of indexes of the columns that are categorical.
export function get_col_idx_dict()
{
  let cat_col_dict = get_categorical_columns()
  // console.debug('get_col_idx_dict', cat_col_dict);
  let col_idx_list = Object.values(cat_col_dict).map(dtype_dict => dtype_dict.col_idx);
//   console.debug('col_idx_list', col_idx_list)
  return col_idx_list;
}

// Restrict input for the set of matched elements to the given inputFilter function.
// https://stackoverflow.com/a/995193
(async function ($) {
  $.fn.setNumeric = async function (setNumeric) {
    return this.on('input keydown keyup mousedown mouseup select contextmenu drop',
      async function () {
        if (setNumeric(this.value)) {
          this.oldValue = this.value;
          this.oldSelectionStart = this.selectionStart;
          this.oldSelectionEnd = this.selectionEnd;
        }
        else if (this.hasOwnProperty('oldValue')) {
          this.value = this.oldValue;
          this.setSelectionRange(this.oldSelectionStart, this.oldSelectionEnd);
        }
        else {
          this.value = '';
        }
      });
  };
}(jQuery));


export function clamp(n, min, max) {
    return Math.min(Math.max(parseInt(n), min), max);
}
