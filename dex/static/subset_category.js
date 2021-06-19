/*
Filter by category

Column block: Combination of column selector and category selector for that column. A
category filter is only in use when both a column and one or more categories have been
selected. Categories are downloaded on demand.
*/

import * as util from './util.js'

'use strict';  // jshint ignore:line

let $ = jQuery.noConflict();

let filter_template = $('#category-filter-template').contents();

// Column df.iloc index to list of categories (unique values) in column.
// We only look up categories as needed, so this starts out empty.
let col_to_cat_dict = new Map();
let cat_list_el = $('.cat-filter-list');
let cat_desc_el = $('#category-description-msg');

/* Column index to dtype_dict */
const idx_to_dtype_dict = util.rekey(g.cat_col_map, 'col_idx')

export async function create()
{
  let col_idx_dict = util.get_col_idx_dict();
  await add_remaining(col_idx_dict);
  await update_cat_filter_msg();
}

// Local

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
    await add_remaining(util.get_col_idx_dict());
  }
  await update_disabled_columns(ev);
  await update_cat_filter_msg();
}

async function update_cat_filter_msg(_ev)
{
  let desc_str = await get_cat_filter_description();
  if (!desc_str) {
    cat_desc_el.html('Subset is unmodified');
  }
  else {
    cat_desc_el.html(`Subset may include only rows where:<p>${desc_str}`);
  }
}

// If there are any unused columns remaining, add a final column block with those columns.
async function add_remaining(col_idx_dict)
{
  // console.log(col_idx_dict)
  if (!Object.keys(col_idx_dict).length) {
    return;
  }
  // console.debug(`add_remaining() colIdxSet=${[...Array.from(col_idx_dict)]}`)
  let filterFragment = filter_template.clone();
  let col_el = filterFragment.find('.cat-col');

  col_el.append(`<option value='-1'>${g.filter_not_applied_str}</option>`);

  // Set maintains insert order, so no need to sort it
  for (let col_idx of Object.values(col_idx_dict)) {
    let col_name = idx_to_dtype_dict[col_idx]['col_name'];
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

async function get_sel_list_str()
{
  let sel_list = [];
  for (let block_el of cat_list_el.find('.cat-filter-block')) {
    block_el = $(block_el);
    let sel_dict = await get_block_sel_str(block_el);
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
  let sel_list = await get_sel_list();
  for (let {col_idx, cat_set} of sel_list) {
    let cached_cat = col_to_cat_dict.get(col_idx);
    let cat_name_list = [];
    for (let cat_idx of [...cat_set]) {
      if (cat_idx !== -1) {
        cat_name_list.push(cached_cat.get(cat_idx));
      }
    }
    if (cat_name_list.length) {
      let col_name = g.derived_dtypes_list[col_idx]; //.cat_col_map.get(col_idx);
      cat_map.set(col_idx, cat_name_list);
      // cat_map.set(col_name['col_name'], cat_name_list);
      cat_map.set(col_name['col_name'], cat_name_list);
    }
  }
  return cat_map;
}

// Get a list of the selections for all category filters that have both col and cats selected.
// export async function get_cat_filter()
// {
//   let cat_arr = [];
//   for (let {col_idx, cat_set} of await get_sel_list()) {
//     if (col_idx === -1 || -1 in cat_set || !cat_set.size) {
//       continue;
//     }
//     cat_arr.push([col_idx, Array.from(cat_set)]);
//   }
//   return cat_arr;
// }
export async function get_cat_filter_str()
{
  let cat_arr = [];
  for (let {col_idx, cat_set} of await get_sel_list_str()) {
    if (col_idx === -1 || -1 in cat_set || !cat_set.size) {
      continue;
    }
    cat_arr.push([col_idx, Array.from(cat_set)]);
  }
  return cat_arr;
}

async function fetch_and_fill_cat_select(block_el)
{
  await _assert(block_el.hasClass('cat-filter-block'));
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

async function get_block_sel_str(block_el)
{
  let sel_dict = await get_block_select_el(block_el);
  return await get_block_select_str(sel_dict);
}

async function get_block_select_str(sel_dict)
{
  // With <select multiple="multiple"> elements, the .val() method returns an array
  // of all selected options.
  let sel_list = sel_dict.val_el.data().select2.data().map(function (elem) {
    return elem.text
  });
  return {
    col_idx: parseInt(sel_dict.col_el.val()),
    cat_set: new Set(sel_list),
  };
}


async function update_disabled_columns(_ev)
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
