import * as sub_time from './subset_time_period.js'
import * as sub_cat from './subset_category.js'
import * as sub_row from "./subset_row.js";
import * as sub_query from "./subset_query.js";
// import * as util from './util.js'

'use strict';  // jshint ignore:line

let $ = jQuery.noConflict();

// This also captures failed console.assert().
async function register_global_error_handler()
{
}


$(document).ready(
  async function () {
    await register_global_error_handler();

    // Numeric input boxes
    $('.dex-numeric').setNumeric(value => /^\d*$/.test(value));

    await sub_time.create();
    await sub_row.create();
    await sub_cat.create();
    await sub_query.create();

    if (window.matchMedia) {
      const colorSchemeQuery = window.matchMedia('(prefers-color-scheme: dark)');
      colorSchemeQuery.addEventListener('change',
        await set_color_scheme(get_preferred_color_scheme()));
    }
  });

// Post a form without triggering a reload (not supported by regular form post).
async function post_form(v)
{
  $('#download-spinner').Loading('Creating CSV subset');
  $('#download-container').addClass('dex-hidden');

  let filename = (
    `${g.entity_tup.scope_str}.${g.entity_tup.identifier_int}.${g.entity_tup.version_int}.zip`
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
    date_filter: await sub_time.get_time_period_filter(),
    col_filter: await sub_query.get_column_filter(),
    query_filter: await sub_query.get_query_filter(),
    row_filter: await sub_row.get_row_filter(),
    cat_map: [...(await sub_cat.get_cat_filter_str())],
  };
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
