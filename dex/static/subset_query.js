// Filter by Pandas Query

import * as util from './util.js'

export async function create()
{
  await create_table();
  await add_column_header_checkboxes();
  await add_toggle_checkbox();
  await register_event_listeners();

}

export async function get_query_filter()
{
  if (!query_is_valid) {
    throw (
      'The query in "Filter by Query" is invalid. To create this subset, ' +
      'please repair or clear the query.'
    );
  }
  return text_el.val()
}

export async function get_column_filter()
{
  let el_list = $('.pq-column-select-checkbox');
  let sel_arr = el_list.map(function () { return this.checked; }).get();
  // Somehow, the DataTable creates a duplicate row of the column select checkboxes. We slice
  // those off here.
  // return sel_arr.slice(0, sel_arr.length / 2);
  return sel_arr;
}

// Local

let auto_el;
let text_el;
let apply_el;
let html_table;
let query_is_valid;
let hidden_search_el;
let pq_table;
let col_sel_checkbox;

async function create_table()
{
  auto_el = $('#pq-auto');
  text_el = $('#pq-text');
  apply_el = $('#pq-apply');
  html_table = $('#csv-table');
  query_is_valid = true;
  col_sel_checkbox = $('pq-column-select-checkbox');

  pq_table = html_table
    .on('xhr.dt', async function (e, settings, json, _xhr) {
      apply_el.prop('disabled', false);
      $('#pq-result-msg').text(json.queryResult);
      query_is_valid = json.queryIsOk;
    })
    .on('preXhr.dt', async function (_e, _settings, _data) {
      $('#pq-result-msg').text('Running query...');
      apply_el.prop('disabled', true);
    })
    .on('draw.dt', async function () {
      await sync_column_selections();
      // $('#csv-table').DataTable().columns.adjust();
    })
    .DataTable({
      // Enable horizontal scrolling
      scrollX: '90vw',
      processing: true,
      serverSide: true,
      ajax: {url: `fetch-browse/${g.rid}`, dataSrc: 'data',},
      order: [[0, 'asc']],
      defaultContent: '',
      // .prop('checked',true); instead of .attr('checked',true);
      // "aoColumns": [
      // ],
      columnDefs: [{
        orderable: true,
        targets: 0,
        // Checkboxes in columns
        // className: 'select-checkbox',
      }],
      select: {
        items: 'column',
        // style: 'multi',
        // selector: 'td:first-child',
        toggleable: true,
        // Checkboxes in columns
        // style: 'os',
        // selector: 'td:first-child'
      },
      // Fix issue where table row width is different from header and footer
      bAutoWidth: false,
      // searching: true,
      // Time in ms after the last key press and automatically triggering search (if enabled).
      searchDelay: 2000,
    });

  hidden_search_el = $('.pq input[type=search]');
}

async function register_event_listeners()
{
  apply_el.on('click', (_ev) => {
    sync_query();
  })
    // After add the handler, we trigger it with a click because the browser may
    // have filled in query text from a previous instance of the page.
    .trigger('click');

  text_el.on('keyup', function (_ev) {
    if (auto_el[0].checked) {
      sync_query();
    }
  });

  $('#pq-toggle-all-input').on('change', async (e) => {
    await sync_column_selections();
    e.stopPropagation();
  });

  $('input[name="pq-toggle-single-input"]').on('change click', async (e) => {
    await sync_column_selections();
    e.stopPropagation();
  });

  // $('input[name="pq-toggle-single-input"]').on('click', async (e) => {
  //   // alert("111");
  //   // $('.pq-column-select-checkbox').prop('checked', $('#pq-toggle-all-input').prop('checked'));
  //   // await sync_column_selections();
  //   e.stopPropagation();
  // });

  // col_sel_checkbox.on('change', async function (e) {
  //   alert("2");
  //   // Prevent column sorting
  //   // e.stopPropagation();
  //   // await sync_column_selections();
  //   // e.stopPropagation();
  // });

  // col_sel_checkbox.on('click', async function (e) {
  //   // alert("3");
  //   // Prevent column sorting
  //   // e.stopPropagation();
  //   // let checkbox_el = $(e.target);
  //   // let parent_el = checkbox_el.parent();
  //   // let col_idx = parent_el.parent().children().index(parent_el);
  //   // $(`td:nth-child(${col_idx + 1})`).toggleClass('unselected', !checkbox_el.checked);
  // });
  //
  // // Listen for all clicks on the document
  // // document.addEventListener('click', function (event) {
  // //   // If the click happened inside the the container, bail
  // //   if (!event.target.closest('pq-column-select-checkbox')) {
  // //   }
  // // });
}

async function add_column_header_checkboxes()
{
  pq_table.columns().iterator('column', function (context, index) {
    $(pq_table.column(index).header())
      .find('.DataTables_sort_wrapper')
      // language=HTML
      .prepend(`
        <div class='pq-column-select-checkbox-container'>
          <div class='pq-column-select-checkbox-item'>
            <input name='pq-toggle-single-input' class='pq-column-select-checkbox' type='checkbox' checked='checked'/>
          </div>
        </div>`
      );
  });
}

// Add all/none toggle for column header checkboxes
async function add_toggle_checkbox()
{
  $('#csv-table_wrapper .fg-toolbar').first().children().first().after(`
  <div id='pq-toggle-all'>
    <input id='pq-toggle-all-input' name='pq-toggle-all-input' class='pq-column-all-checkbox'
           type='checkbox' checked='checked'/>
    <label class='control-label' for='pq-toggle-all-input'>Columns: Select all / none</label>
  </div>
`);
}

// The table header, footer and rows expand as needed, up to the width of the screen. For some
// reason, the column header row only matches the width of the other elements on initial page
// rendering, and must handled programmatically after that.
$(window).resize(function () {
  $('#csv-table').DataTable().columns.adjust();
});

async function sync_column_selections()
{
  for (const [col_idx, v] of (await get_column_filter()).entries()) {
    $(`td:nth-child(${col_idx + 1})`).toggleClass('unselected', !v);
  }
}

async function sync_query()
{
  hidden_search_el.val(text_el.val());
  hidden_search_el.trigger('input');
}
