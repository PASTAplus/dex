'use strict';  // jshint ignore:line

let $ = jQuery.noConflict();

let seconds;


$(document).ready(
  async function () {
    await create_y_list();
  }
)

// Handle all changes in the list of Y columns
// We use event delegation to automatically attach events to the dynamically created column select
// elements.
$('#y-block-list').on('change', '.y-select', async (_ev) => {
  create_y_list();
  update_plot_button_enabled();
});

$('#col-x').on('change', async (_ev) => {
  update_plot_button_enabled();
});


function update_plot_button_enabled()
{
  $('#plot-button')[0].disabled = !(get_y_list().length > 0 && $('#col-x').val() !== '-1');
}

function create_y_list()
{
  let sel_list = get_y_list();
  $('#y-block-list').empty();
  // Start with full list of options
  let cur_list = [...g.col_list];
  // Create dropdowns for selected options.
  for (let [sel_id, is_checked] of sel_list) {
    // if (sel_list !== -1) {
    create_y_block(cur_list, sel_id, is_checked, false);
    // }
    cur_list = remove(cur_list, sel_id);
  }
  // Create single dropdown for any remaining options.
  if (cur_list.length > 0) {
    create_y_block(cur_list, -1, false, true);
  }

  // Align all the widths to match up with X
  $('.y-select').width($('#col-x').width());
}


// Get list of currently selected from the dropdowns.
// The current selections are held only in the dropdowns (as opposed to having the dropdowns mirror
// a separately maintained list of selections).
function get_y_list()
{
  let sel_set = new Set();
  let sel_list = [];
  for (let block_el of $('#y-block-list').find('.y-block')) {
    let sel_el = $(block_el).find('select')[0];
    let sel_id = parseInt(sel_el.value);
    if (sel_id !== -1 && !sel_set.has(sel_id)) {
      let is_checked = $(block_el).find('#y-line-checkbox-id')[0].checked;
      sel_list.push([sel_id, is_checked]);
      sel_set.add(sel_id);
    }
  }
  // Remove any duplicates that are created if the user goes back to an earlier
  // dropdown and selects a value already used in a later one.
  // alert(sel_list);
  // let uniq_sel_list = []
  // for sel_tup in sel_list:
  return sel_list;
  // return [...new Set(sel_list)];
}

// Return new list with sel_id removed.
function remove(rem_list, sel_id)
{
  return rem_list.filter(opt_tup => opt_tup[0] !== sel_id);
}

// Add a dropdown selection box that has all the options in rem_list, and has the
// option in sel_id selected.
function create_y_block(rem_list, sel_id, is_checked, is_last)
{
  let y_template_el = $('#y-template').contents();
  let y_block_el = y_template_el.clone();
  let y_select_el = y_block_el.find('.y-select');
  let unselected_msg = is_last ? 'Select dependent value' : 'Remove this value';
  $(y_select_el).append(
    `<option value='-1' ${sel_id === -1 ? 'selected' : ''}>${unselected_msg}</option>`
  );
  for (let [opt_id, opt_str] of rem_list) {
    $(y_select_el).append(
      `<option value='${opt_id}' ${opt_id === sel_id ? 'selected' : ''}>${opt_str}</option>`
    );
  }
  let block_list_el = $('#y-block-list');
  y_block_el.find('.y-label').text(`Y${block_list_el.find('.y-block').length + 1}:`);
  y_block_el.find('#y-line-checkbox-id')[0].checked = is_checked;
  block_list_el.append(y_block_el);
}


$("#plot-button").click(async function () {
  // alert(1);

  let plot_el = $('#plot-container');

  while (plot_el[0].firstChild) {
    plot_el[0].removeChild(plot_el[0].firstChild);
  }

  // let $ = jQuery.noConflict();

  // $(this).css("display", "none");
  // $('#spinner-msg').css("display", "block");

  plot_el.Loading('Generating plot');

  let sel_dict = {
    x: parseInt($('#col-x').val()),
    y: get_y_list(),
  };

  let parm_uri = encodeURIComponent(JSON.stringify(sel_dict));
  // let subset_json = encodeURIComponent(JSON.stringify(g.subset_dict));
  let subset_json = encodeURIComponent(JSON.stringify(g.subset_dict));
  let response = await fetch(`/bokeh/xy-plot/${g.rid}/${parm_uri}?subset=${subset_json}`);
  let data = await response.json();

  plot_el.Destroy();

  Bokeh.embed.embed_item(data, 'plot-container');

  // Show warning if plot is subsampled for performance
  $('#column-description-msg').removeClass('hidden-msg');

  // clearInterval(h);

  // window.open(`/dex/profile/doc/${g.rid}`, "_self")

  // let response = await fetch(`/dex/profile-fetch/${g.rid}`);
  // document.open('text/html');
  // document.write(await response.text());
  // $('#profile').src = `/dex/profile/profile-fetch/${g.rid}`
  // $('#profile').load(function(){$(this).height($(this).contents().outerHeight());});
});
