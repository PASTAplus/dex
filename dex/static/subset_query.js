// Filter by Pandas Query

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
  let sel_arr = await get_column_checked_arr();
  return {
    'selected_columns': await get_selected_column_name_arr(sel_arr),
    'index': sel_arr[0]
  }
}

async function get_selected_column_name_arr(sel_arr)
{
  // If all columns are selected, we disable the column filter.
  if (sel_arr.every(Boolean)) {
    return [];
  }
  let selected_column_arr = [];
  let idx = 0;
  for (const is_selected of sel_arr.slice(1)) {
    if (is_selected) {
      selected_column_arr.push(g.column_list[idx].col_name)
    }
    ++idx;
  }
  return selected_column_arr;
}

// Return an array of booleans for the checked status of each column selection checkbox.
async function get_column_checked_arr()
{
  let el_list = $('.pq-column-select-checkbox');
  let sel_arr = el_list.map(function () { return this.checked; }).get();
  // Somehow, the DataTable creates a duplicate row of the column select checkboxes. We
  // slice those off here.
  return sel_arr.slice(0, sel_arr.length / 2);
}

// Return true if table has at least two selected plottable columns.
async function is_plottable()
{
  let sel_arr = await get_column_checked_arr();
  const plottable_arr = ['DATETIME', 'FLOAT', 'INT'];
  let plottable_count = 0;
  for (const [i, is_selected] of sel_arr.entries()) {
    if (is_selected) {
      // The index is never plottable
      if (!i) {
        // ++plottable_count;
      }
      else {
        if (plottable_arr.includes(g.column_list[i - 1]['pandas_type'])) {
          ++plottable_count;
        }
      }
    }
  }
  return plottable_count >= 2;
}

// Local

let auto_el = $('#pq-auto');
let text_el = $('#pq-text');
let apply_el = $('#pq-apply');
let hidden_search_el;
let query_is_valid = true;
let pq_table;

async function create_table()
{
  let html_table = $('#csv-table');
  let query_is_valid = true;
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
      // ajax: {url: `fetch-browse/${g.rid}`, dataSrc: 'data',},

      ajax: {
        url: `fetch-browse/${g.rid}`,

        dataSrc: function (json) {

          for (let i = 0; i < json.data.length; i++) {
            for (let j = 1; j < json.data[i].length; ++j) {
              if (json.bad[i][j - 1]) {
                json.data[i][j] = '<div class="parse-error">' + json.data[i][j] + '</div>';
              }
            }
          }

          return json.data;
        },

        // dataSrc: 'data',
      },

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

      // rowCallback: function(row, data, index) {
      //   // if (data.status === "Active") {
      //   //   $("td:eq(4)", row).addClass("active");
      //     $("td:eq(4)", row).addClass("parse-error");
      //   // }
      // }

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
    $('.pq-column-select-checkbox').prop('checked', $('#pq-toggle-all-input').prop('checked'));
    await sync_column_selections();
    e.stopPropagation();
  });

  $('input[name="pq-toggle-single-input"]').on('change click', async (e) => {
    await sync_column_selections();
    e.stopPropagation();
  });
}

async function add_column_header_checkboxes()
{
  pq_table.columns().iterator('column', function (context, index) {
    $(pq_table.column(index).header())
      .find('.DataTables_sort_wrapper')
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
  for (const [col_idx, v] of (await get_column_checked_arr()).entries()) {
    $(`td:nth-child(${col_idx + 1})`).toggleClass('unselected', !v);
  }

  $('#plot-button').toggleClass('disabled', !await is_plottable());
}

async function sync_query()
{
  hidden_search_el.val(text_el.val());
  hidden_search_el.trigger('input');
}
