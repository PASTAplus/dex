//
// Row index filter
//

import * as util from './util.js'


export async function create()
{
  await register_event_listeners()
  await update_row_filter_msg();
}


export async function get_row_filter()
{
  let sa = $('#row-start').val();
  let sb = $('#row-end').val();
  let a = sa === '' ? 1 : util.clamp(sa, 1, g.row_count);
  let b = sb === '' ? g.row_count : util.clamp(sb, 1, g.row_count);
  if (a > b) {
    [b, a] = [a, b];
  }
  return {a, b};
}


// Local

async function register_event_listeners()
{
  $('#row-start, #row-end').keyup(async () => {await update_row_filter_msg()});
}


async function update_row_filter_msg()
{
  let {a, b} = await get_row_filter();
  let rows_int = b - a + 1;
  let msg_el = $('#row-filter-msg');
  if (a === 1 && b >= g.row_count) {
    msg_el.text('Subset is unmodified');
  }
  else {
    let percent_str = Math.round((rows_int / g.row_count) * 100);
    msg_el.text(
      `Subset may include only rows ${a} to ${b} (${rows_int} rows, ` +
      `${percent_str}% of the table)`);
  }
}
