/*
  Filter by time period
*/
import * as util from './util.js'

'use strict';  // jshint ignore:line

let $ = jQuery.noConflict();
let time_period_filter_el = $('#time-period-filter');
const datetime_col_list = util.get_datetime_columns();

export async function create()
{
  register_event_listeners();
}

export async function get_time_period_filter()
{
  if (!time_period_filter_el[0].selectedIndex) {
    return {col_name: '', start: null, end: null};
  }

  let col_name = time_period_filter_el.val();
  let time_period_start = $('#time-period-start');
  let time_period_end = $('#time-period-end');
  try {
    $.datepicker.parseDate('yy-mm-dd', time_period_start.val());
    $.datepicker.parseDate('yy-mm-dd', time_period_end.val());
  }
  catch (_e) {
    throw (
      'To create this subset, please ensure that the dates in the ' +
      'Time Period Filter are on the form "YYYY-MM-DD".'
    );
  }
  return {
    col_name: col_name,
    start: time_period_start.val(),
    end: time_period_end.val()
  };
}


// Local

function register_event_listeners()
{
  time_period_filter_el.on('change', async function () {
    await update_time_period_filter($(this));
  }).trigger('change');
  // Event delegation, time_period_filter_el is the static parent, with time-period-*
  // the dynamic elements.
  time_period_filter_el.on('change', '.dex-date-picker', function (_ev) {
    // Validate the date
    $(this).valid();
  });

}

async function update_time_period_filter(col_el)
{
  await update_datepicker_state(col_el, $('#time-period-start'), true);
  await update_datepicker_state(col_el, $('#time-period-end'), false);
  await update_time_period_filter_msg(col_el);
}

async function update_datepicker_state(col_el, dt_el, is_start)
{
  if (dt_el.hasClass('hasDatepicker')) {
    dt_el.datepicker('destroy');
    dt_el.removeClass('hasDatepicker');
  }
  let col_idx = parseInt(col_el.val());
  if (col_idx === -1) {
    dt_el.val('');
    dt_el.attr('disabled', true);
  }
  else {
    dt_el.datepicker('option', {'disabled': false});
    let col_name = time_period_filter_el.val();
    let dt_dict = g.datetime_col_dict[col_name];
    let date_str = is_start ? dt_dict.begin_yyyy_mm_dd_str : dt_dict.end_yyyy_mm_dd_str;
    dt_el.val(date_str);
    dt_el.attr('disabled', false);
    dt_el.datepicker({
      'dateFormat': 'yy-mm-dd',
      'defaultDate': date_str,
      'changeYear': true,
      'constrainInput': true,
    });
  }
}

async function update_time_period_filter_msg(col_el)
{
  let msg_el = $('#time-period-filter-msg');
  if (!col_el.prop('selectedIndex')) {
    msg_el.text('Subset is unmodified');
  }
  else {
//     let col_name = $('#time-period-filter option:selected').text();
    let col_name = $('#time-period-filter option:selected').text().trim();
    msg_el.text(
      `Subset may include only rows where "${col_name}" is within ` +
      `the selected range`);
  }
}
