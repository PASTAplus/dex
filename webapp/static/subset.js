'use strict'; // jshint ignore:line

const NOT_APPLIED_STR = 'Filter not applied';

let $ = jQuery.noConflict();

// This also captures failed console.assert().
async function register_global_error_handler()
{
}

// Restricts input for the set of matched elements to the given inputFilter function.
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


$(document).ready(
    async function () {
      register_global_error_handler();

    // Numeric input boxes
    $('.dex-numeric').setNumeric(value => /^\d*$/.test(value));

      // Row index filter
      $('#row-start, #row-end').keyup(() => {
        update_row_filter_msg();
      });
      update_row_filter_msg();

// Post a form without triggering a reload (not supported by regular form post).
async function post_form(v)
{
  $('#download-spinner').Loading('Creating CSV subset');
  $('#download-container').addClass('dex-hidden');

        let filename = (
            `${g.entity_tup.scope_str}.` +
            `${g.entity_tup.identifier_int}.` +
            `${g.entity_tup.version_int}.csv`
        );
        fetch(`#`, {
          mode: 'no-cors',
          method: 'POST',
          body: JSON.stringify(v),
          cache: 'no-cache',
        })
            .then(async (response) => {

      $('#download-spinner').Destroy();
      $('#download-container').removeClass('dex-hidden');

      // mode="no-cors" causes the body of the response to be unavailable, so we can only
      // check status.
      if (response.ok) {
        // TODO: Use stream instead.
        download(await response.blob(), filename, 'text/csv');
      }
      else {
        throw `Error ${response.status}: ${await response.text()}`;
      }
    })
    .catch(async (error) => {
      // console.error(`Creating subset failed with error: ${error.toString()}`)
      window.document.write(error);
    });
  // Prevent form submit
  return false;
}

      // Filter by Pandas Query

      let pq_auto_el = $('#pq-auto');
      let pq_text_el = $('#pq-text');
      let pq_apply_el = $('#pq-apply');
      let pq_html_table = $('#csv-table');
      let pq_query_is_valid = true;

      let pq_table = pq_html_table
          .on('xhr.dt', async function (e, settings, json, _xhr) {
            pq_apply_el.prop('disabled', false);
            $('#pq-result-msg').text(json.queryResult);
            pq_query_is_valid = json.queryIsOk;
          })
          .on('preXhr.dt', async function (e, settings, data) {
            $('#pq-result-msg').text('Running query...');
            pq_apply_el.prop('disabled', true);
          })
          .on('draw.dt', async function () {
            await pq_sync();
            // $('#csv-table').DataTable().columns.adjust();
          })
          .DataTable({
            // Enable horisontal scrolling
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
            searchDelay: 400,
          });

      // Add column header checkboxes
      // language=HTML
      pq_table.columns().iterator('column', function (context, index) {
        $(pq_table.column(index).header())
            .find('.DataTables_sort_wrapper')
            // language=HTML
            .prepend(`
              <div class='pq-column-select-checkbox-container'>
                <div class='pq-column-select-checkbox-item'>
                  <input class='pq-column-select-checkbox' type='checkbox' checked='checked'/>
                </div>
              </div>`
            );
      });

      // Listen for all clicks on the document
      document.addEventListener('click', function (event) {
        // If the click happened inside the the container, bail
        if (!event.target.closest('pq-column-select-checkbox')) {
        }
      });

      // Add all/none toggle for column header checkboxes
      // language=HTML
      $('#csv-table_wrapper .fg-toolbar').first().children().first().after(`
        <div id='pq-toggle-all'>
          <input id='pq-toggle-all-input' name='pq-toggle-all-input' class='pq-column-all-checkbox'
                 type='checkbox' checked='checked'/>
          <label class='control-label' for='pq-toggle-all-input'>Columns: Select all / none</label>
        </div>
      `);

      $('#pq-toggle-all-input').on('change', async () => {
        $('.pq-column-select-checkbox').prop('checked', $('#pq-toggle-all-input').prop('checked'));
        await pq_sync();
      });

      // The table header, footer and rows expand as needed, up to the width of the screen. For some
      // reason, the column header row only matches the width of the other elements on initial page
      // rendering, and must handled programmatically after that.
      $(window).resize(function () {
        $('#csv-table').DataTable().columns.adjust();
      });

      let pq_col_sel_checkbox = $('.pq-column-select-checkbox');

      pq_col_sel_checkbox.on('click', async function (e) {
        // Prevent column sorting
        e.stopPropagation();
        // let checkbox_el = $(e.target);
        // let parent_el = checkbox_el.parent();
        // let col_idx = parent_el.parent().children().index(parent_el);
        // $(`td:nth-child(${col_idx + 1})`).toggleClass('unselected', !checkbox_el.checked);
      });

      pq_col_sel_checkbox.on('change', async function (e) {
        // Prevent column sorting
        // e.stopPropagation();
        await pq_sync();
      });

      async function pq_sync()
      {
        for (const [col_idx, v] of (await get_column_filter()).entries()) {
          $(`td:nth-child(${col_idx + 1})`).toggleClass('unselected', !v);
        }
      }

      async function get_column_filter()
      {
        let el_list = $('.pq-column-select-checkbox');
        let sel_arr = el_list.map(function () { return this.checked; }).get();
        // Somehow, the DataTable creates a duplicate row of the column select checkboxes. We slice
        // those off here.
        return sel_arr.slice(0, sel_arr.length / 2);
      }

      async function get_query_filter()
      {
        if (!pq_query_is_valid) {
          throw (
              'The query in "Filter by Query" is invalid. To create this subset, ' +
              'please repair or clear the query.'
          );
        }
        return pq_text_el.val()
      }

      let pq_hidden_search_el = $('input[type=search]');

      async function pq_apply()
      {
        pq_hidden_search_el.val(pq_text_el.val());
        pq_hidden_search_el.trigger('input');
      }

      pq_text_el.on('keyup', function (_ev) {
        if (pq_auto_el[0].checked) {
          pq_apply();
        }
      });

      pq_apply_el.on('click', (_ev) => {
        pq_apply();
      })
          // After add the handler, we trigger it with a click because the browser may
          // have filled in query text from a previous instance of the page.
          .trigger('click');

      // Filter by time period

      let time_period_filter_el = $('#time-period-filter');

      // Event delegation, time_period_filter_el is the static parent, with time-period-*
      // the dynamic elements.
      time_period_filter_el.on('change', '.dex-date-picker', function (_ev) {
        // Validate the date
        $(this).valid();
      });

      async function get_time_period_filter()
      {
        let col_idx = parseInt(time_period_filter_el.val());
        // let col_idx = parseInt($('#time-period-filter').val());
        if (col_idx === -1) {
          return {col_idx: col_idx, start: null, end: null};
        }
        try {
          $.datepicker.parseDate('yy-mm-dd', $('#time-period-start').val());
          $.datepicker.parseDate('yy-mm-dd', $('#time-period-end').val());
        }
        catch (e) {
          throw (
              'To create this subset, please ensure that the dates in the ' +
              'Time Period Filter are on the form "YYYY-MM-DD".'
          );
        }
        return {
          col_idx: col_idx,
          start: $('#time-period-start').val(),
          end: $('#time-period-end').val()
        };
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
          let dt_dict = g.datetime_col_list[col_el.prop('selectedIndex') - 1];
          let date_str = is_start ? dt_dict.begin_dt : dt_dict.end_dt;
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
          // let col_name = $('#time-period-filter option:selected').text();
          let col_name = await get_selected_str(time_period_filter_el);
          msg_el.text(
              `Subset may include only rows where "${col_name}" is within ` +
              `the selected range`);
        }
      }

      time_period_filter_el.on('change', async function () {
        await update_time_period_filter($(this));
      }).trigger('change');

      // Row index filter

      async function get_row_filter()
      {
        let sa = $('#row-start').val();
        let sb = $('#row-end').val();
        let a;
        let b;
        if (sa === '') {
          a = 1;
        }
        else {
          a = parseInt(sa);
        }
        if (sb === '') {
          b = g.row_count;
        }
        else {
          b = parseInt(sb);
        }
        if (a < 1) {
          a = 1;
        }
        if (b < 1) {
          b = 1;
        }
        if (a > g.row_count) {
          a = g.row_count;
        }
        if (b > g.row_count) {
          b = g.row_count;
        }
        if (a > b) {
          let x = a;
          a = b;
          b = x;
        }
        return [a, b];
      }

      async function update_row_filter_msg()
      {
        let ab_arr = get_row_filter();
        let a = ab_arr[0];
        let b = ab_arr[1];
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

      // Category filter

      // Column block: Combination of column selector and category selector for that column. A
      // category filter is only in use when both a column and one or more categories have been
      // selected. Categories are downloaded on demand.

      let filterTemplate = $('#category-filter-template').contents();
      // Column df.iloc index to list of categories (unique values) in column.
      // We only look up categories as needed, so this starts out empty.
      let col_to_cat_dict = new Map();
      let cat_list_el = $('.cat-filter-list');
      let cat_descr_el = $('#category-description-msg');

      async function update_cat_filter_col(ev)
      {
        let block_el = await get_cat_block(ev);
        await fetch_and_fill_cat_select(block_el);
        await update_cat_filter_cat(ev);
      }

      async function update_cat_filter_cat(ev)
      {
        await remove_inactive_cat_blocks(ev);
        if ((await is_all_in_use())) {
          await add_remaining(new Set(g.cat_col_map.keys()));
        }
        await update_disabled_columns(ev);
        await update_cat_filter_msg();
      }

      async function update_cat_filter_msg(_ev)
      {
        let descr_str = await get_cat_filter_description();
        if (!descr_str) {
          cat_descr_el.html('Subset is unmodified');
        }
        else {
          cat_descr_el.html(`Subset may include only rows where:<p>${descr_str}`);
        }
      }

      // If there are any unused columns remaining,add a final column block with those columns.
      async function add_remaining(col_idx_set)
      {
        if (!col_idx_set.size) {
          return;
        }

        // console.debug(`add_remaining() colIdxSet=${[...Array.from(col_idx_set)]}`)

        let filterFragment = filterTemplate.clone();
        let col_el = filterFragment.find('.cat-col');

        col_el.append(`<option value='-1'>${NOT_APPLIED_STR}</option>`);

        // Set maintains insert order, so no need to sort it
        for (let col_idx of Array.from(col_idx_set)) {
          let col_name = g.cat_col_map.get(col_idx);
          col_el.append(`<option value='${col_idx}'>${col_name}</option>`);
        }

        await destroy_all_select2();

        cat_list_el.append(filterFragment);

        await create_all_select2();
      }

      // Remove inactive cat blocks
      async function remove_inactive_cat_blocks()
      {
        // console.debug('remove_inactive_cat_blocks()');

        let delete_list = [];

        let i = 0;
        for (let block_el of cat_list_el.find('.cat-filter-block')) {
          block_el = $(block_el);
          let focusedOrHasFocused = block_el[0].matches(':focus-within');
          if (focusedOrHasFocused) {
            // console.debug(`${i} skipped: Has focus`);
          }
          else if (await is_in_use(block_el)) {
            // console.debug(`${i} skipped: Is in use`);
          }
          else {
            delete_list.push(block_el);
            // console.debug(`${i} removed`);
          }
          ++i;
        }

        for (let el of delete_list) {
          el.remove();
        }

      }

      async function _assert(assert_bool)
      {
        // console.assert(assert_bool);
        if (!assert_bool) {
          throw Error('err');
        }
      }

      // async function get_free_col_list()
      // {
      //   let full_set = new Set(g.cat_col_map.keys());
      //   let sel_list = await get_sel_list();
      //   let sel_set = new Set(sel_list.map(x => x.col_idx));
      //   let free_col_list = [...full_set].filter(x => !sel_set.has(x));
      //   return free_col_list;
      // }

      async function get_sel_list()
      {
        let sel_list = [];
        for (let block_el of cat_list_el.find('.cat-filter-block')) {
          block_el = $(block_el);
          let sel_dict = await get_block_sel(block_el);
          sel_list.push(sel_dict);
        }
        return sel_list;
      }

      async function get_cat_filter_description()
      {
        let lineArr = [];
        let cat_map = await get_selected_category_filters();
        for (let x of (await cat_map).entries()) {
          if (typeof x[0] !== 'string') {
            continue;
          }
          let col_name = x[0];
          let cat_name_list = x[1];
          let cat_str;
          if (cat_name_list.length === 1) {
            cat_str = `"${cat_name_list[0]}"`;
          }
          else {
            cat_str = `any of ${cat_name_list.join(', ')}`;
          }
          lineArr.push(`Column "${col_name}" contains ${cat_str}`);
        }
        return lineArr.join('<br/>OR ');
      }

      async function get_selected_category_filters()
      {
        let cat_map = new Map();
        for (let {col_idx, cat_set} of await get_sel_list()) {
          let cached_cat = col_to_cat_dict.get(col_idx);
          let cat_name_list = [];
          for (let cat_idx of [...cat_set]) {
            if (cat_idx !== -1) {
              cat_name_list.push(cached_cat.get(cat_idx));
            }
          }
          if (cat_name_list.length) {
            let col_name = g.cat_col_map.get(col_idx);
            cat_map.set(col_idx, cat_name_list);
            cat_map.set(col_name, cat_name_list);
          }
        }
        return cat_map;
      }

      // Get a list of the selections for all category filters that have both col and cats selected.
      async function get_cat_filter()
      {
        let cat_arr = [];
        for (let {col_idx, cat_set} of await get_sel_list()) {
          if (col_idx === -1 || -1 in cat_set || !cat_set.size) {
            continue;
          }
          cat_arr.push([col_idx, Array.from(cat_set)]);
        }
        return cat_arr;
      }

      async function fetch_and_fill_cat_select(block_el)
      {
        _assert(block_el.hasClass('cat-filter-block'));
        let {col_idx, _cat_set} = await get_block_sel(block_el);
        let {_col_el, val_el} = await get_block_select_el(block_el);

        val_el.empty();

        if (col_idx === -1) {
          return;
        }

        let sel_el = block_el.find('.select2-search');
        // noinspection JSUnresolvedFunction
        sel_el.Loading();

        let response = await fetch(`/dex/subset/fetch-category/${g.rid}/${col_idx}`);
        sel_el.Destroy();

        // If response is not ok, show the Flask exception page when debugging, else what the server
        // sent.
        if (response.status !== 200) {
          let wnd = window.open('about:blank', '_blank');
          wnd.document.write(await response.text());
          wnd.document.close();
          return;
        }

        val_el.empty();

        let category_arr = await response.json();
        if (!category_arr.length) {
          return;
        }

        let cat_map = new Map();

        let cat_idx = 0;
        for (let category_str of category_arr) {
          val_el.append(`<option value='${cat_idx}'>${category_str}</option>`);
          cat_map.set(cat_idx, category_str);
          cat_idx++;
        }

        col_to_cat_dict.set(col_idx, cat_map);
      }

      // Get the div element that is the root of the block which has the column and category select
      // elements, one of which triggered {ev}.
      async function get_cat_block(ev)
      {
        let el = $(ev.target);
        let block_el = await el.closest('.cat-filter-block');
        // console.assert(block_el);
        return block_el;
      }

      // Get the cat-col and cat-val select elements that are children of {cat-filter-block}.
      async function get_block_select_el(block_el)
      {
        // console.assert(block_el.hasClass('cat-filter-block'));
        let d = {
          col_el: await block_el.find('.cat-col'),
          val_el: await block_el.find('.cat-val'),
        };
        return d;
      }

      // Get selection info for cat block.
      // cat-col always returns exactly one int, which is -1 for "no selection" and >= 0
      // for a selected column.
      // cat-val returns 0, 1 or more selections. It does not have a -1, "no selection" value.
      async function get_block_sel(block_el)
      {
        let sel_dict = await get_block_select_el(block_el);
        return await get_block_select_idx(sel_dict);
      }

      async function get_block_select_idx(sel_dict)
      {
        // With <select multiple="multiple"> elements, the .val() method returns an array
        // of all selected options.
        return {
          col_idx: parseInt(sel_dict.col_el.val()),
          cat_set: new Set(sel_dict.val_el.val().map((x) => {
            return parseInt(x);
          })),
        };
      }


      async function update_disabled_columns(ev)
      {
        let col_sel_set = await get_col_sel_set();
        for (let block_el of cat_list_el.find('.cat-filter-block')) {
          let sel_dict = await get_block_select_el($(block_el));
          sel_dict.col_el.find('option').removeAttr('disabled');
          let selected_col_idx = parseInt(sel_dict.col_el.val());
          sel_dict.col_el.find('option').each(function () {
            let opt_val = parseInt($(this).val());
            if (opt_val !== -1
                && opt_val !== selected_col_idx
                && col_sel_set.has(opt_val)) {
              $(this).attr('disabled', 'disabled');
            }
          });
        }
      }

      // Get set of columns selected in all blocks except for one block (the one we
      // are working on).
      async function get_col_sel_set()
      {
        let col_set = new Set();
        for (let sel_dict of await get_sel_list()) {
          col_set.add(sel_dict.col_idx);
        }
        return col_set;
      }

      // Return True if both column and categories have been selected in all existing blocks.
      async function is_all_in_use()
      {
        for (let block_el of cat_list_el.find('.cat-filter-block')) {
          if (!await is_in_use($(block_el))) {
            return false;
          }
        }
        return true;
      }

      // Return True if both column and categories have been selected in a cat block.
      async function is_in_use(block_el)
      {
        let {col_idx, cat_set} = await get_block_sel(block_el);
        return col_idx >= 0 && cat_set.size;
      }

      // select2 doesn't handle dynamic create and destroy. To avoid confusing select2, we need to
      // destroy and re-register all of them after dynamically creating the base select elements.
      async function destroy_all_select2()
      {
        $('.cat-col').each(function (_) {
          if ($(this).hasClass('select2-hidden-accessible')) {
            // noinspection JSUnresolvedFunction
            $(this).select2('destroy');
          }
        });
        $('.cat-val').each(function (_) {
          if ($(this).hasClass('select2-hidden-accessible')) {
            // noinspection JSUnresolvedFunction
            $(this).select2('destroy');
          }
        });
      }

      async function create_all_select2()
      {
        let cat_el = $('.cat-col');
        let val_el = $('.cat-val');
        // noinspection JSUnresolvedFunction
        cat_el.select2({
          width: '30em',
        });

        for (let e of val_el) {
          $(e).select2({
            width: '30em',
          });
        }
      }

      async function update_placeholder(ev)
      {
        let block_el = await get_cat_block(ev);
        let {col_el, val_el} = await get_block_select_el(block_el);
        let {col_idx, cat_set} = await get_block_sel(block_el);
        let select2_args = {width: '30em'};
        if (col_idx >= 0 && !cat_set.size) {
          select2_args.placeholder = 'Click to select';
        }
        val_el.select2(select2_args);
      }

      // Handle all changes in the category filter(s)
      // We use event delegation with cat_list_el as the static parent to automatically
      // attach events to the dynamically created column and category select elements.
      $(cat_list_el).on('change', '.cat-col', async (ev) => {
        // console.error(cat_list_el.find('.cat-filter-block').length);
        await update_cat_filter_col(ev);
        await update_placeholder(ev);
        ev.stopImmediatePropagation();
        return false;
      });

      $(cat_list_el).on('change', '.cat-val', async (ev) => {
        await update_cat_filter_cat(ev);
        ev.stopImmediatePropagation();
        return false;
      });

      await add_remaining(new Set(g.cat_col_map.keys()));
      await update_cat_filter_msg();

      $('#download').click(async () => {
        let subset_dict;
        try {
          subset_dict = await get_subset_dict();
        }
        catch (e) {
          alert(e);
          return;
        }
        await post_form(subset_dict);
      });

      async function get_subset_dict()
      {
        return {
          date_filter: await get_time_period_filter(),
          col_filter: await get_column_filter(),
          query_filter: await get_query_filter(),
          row_filter: await get_row_filter(),
          cat_map: [...(await get_cat_filter())],
        };
      }

      // Utils

      async function get_selected_str(sel_el)
      {
        return sel_el.find('option:selected').text();
        // return $('#time-period-filter option:selected').text();
      }

      // Return array of column indexes for selected columns
      async function get_selected_arr(sel_el)
      {
        return sel_el.find('option:selected').toArray().map(item => parseInt(item.value));
      }

// Color scheme

async function set_color_scheme(scheme)
{
  switch (scheme) {
    case 'dark':
      // alert('dark');
      break;
    case 'light':
      // alert('light');
      break;
    default:
      // alert('default');
      break;
  }
  return null;
}

async function get_preferred_color_scheme()
{
  if (window.matchMedia) {
    if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return 'dark';
    }
    else {
      return 'light';
    }
  }
  return 'light';
}

// async function get_cookies() {
//   document.cookie.split(';').reduce((cookies, cookie) => {
//     const [name, value] = cookie.split('=').map(c => c.trim());
//     cookies[name] = value;
//     return cookies;
//   }, {});
// }
