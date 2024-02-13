'use strict';

let $ = jQuery.noConflict();

// Post a form without triggering a reload (not supported by regular form post).
async function post_form(v)
{
  let filename = (
      `${g.pkg_id.scope_str}.` +
      `${g.pkg_id.id_str}.` +
      `${g.pkg_id.ver_str}.csv`
  );
  fetch(`#`, {
    mode: 'no-cors',
    method: 'POST',
    body: JSON.stringify(v),
    cache: "no-cache",
  })
      .then(async (response) => {
        // mode="no-cors" causes the body of the response to be unavailable, so we can only
        // check status.
        if (response.ok) {
          // TODO: Use stream instead.
          download(await response.blob(), filename, 'text/csv');
          // $(".space-top").Destroy();
        }
        else {
          throw `Error ${response.status}: ${await response.text()}`;
        }
      })
      .catch(async (error) => {
        console.error(`Creating subset failed with error: ${error.toString()}`)
        window.document.write(error);
      });
  // Prevent form submit
  return false;
}

