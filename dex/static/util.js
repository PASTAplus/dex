/*
  Misc utilities
 */

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
  return _get_columns_by_type('CATEGORY')
}

export function get_numeric_columns()
{
  return _get_columns_by_type('NUMERIC')
}

export function get_datetime_columns()
{
  return _get_columns_by_type('DATETIME')
}

// Also converts a list of dict to a dict of dicts. Keys in the outer dict are column index integers.

function _get_columns_by_type(pandas_type)
{
  return Object.fromEntries(
    Object.entries(g.column_list).filter(([k, v]) => v
      ['pandas_type'] === pandas_type
    )
  );
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
