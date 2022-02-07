$("#profile-doc").on('load', function () {
  let profile_el = $(this);
  profile_el.height(profile_el.contents().height());
  // profile_el.width(profile_el.contents().width());
  if (!g.is_cached) {
    $('#spinner').Destroy();
  }
});

// $("#profile-doc").on('error', function () { alert('error occurs'); });

$(document).ready(
  function () {
    // Catching onload from the iframe to destroy the spinner (which is displayed in the parent)
    // proved to be unreliable, as it's possible for the iframe's onload to fire before we get to
    // the point of installing the handler. To ensure that the onload event occurs after installing
    // the handler, we trigger the load here.
    $("#profile-doc").attr("src", `/dex/profile/doc/${g.rid}`);

    // g.is_cached has a race where the document could become cashed between the period it's set in
    // view and the time we use it here. But the only effect should be a brief flash of the spinner.
    if (!g.is_cached) {
      $('#spinner').Loading('Generating profile for CSV (may take several minutes)');
    }

    // To interact with the content inside an iframe from the parent, it's necessary to use a
    // step with find(), instead of a direct reference (dot).
    // if ($("#profile").contents().find("html").find('.container').length) {
  }
);
