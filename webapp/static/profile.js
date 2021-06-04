$(document).ready(
    function () {
      let spinner_el = $('#spinner');
      let timer_handle;
      // g.is_cached has a race where the document could become cashed between the period it's set
      // in view and the time we use it here. But the only effect should be a brief flash of the
      // spinner.
      log.debug(`Profile is_cached=${g.is_cached}`);
      if (!g.is_cached) {
        spinner_el.Loading('Generating profile for CSV (may take several minutes)');
        timer_handle = setInterval(spinner, 1000);
      }
      // Catching onload from the iframe to destroy the spinner (which is displayed in the parent)
      // proved to be unreliable, as it's possible for the iframe's onload to fire before we get to
      // the point of installing the handler.
      function spinner()
      {
        // To interact with the content inside an iframe from the parent, it's necessary to use a
        // step with find(), instead of a direct reference (dot).
        // $("#profile3").contents().find("html").html('<div>test3</div>');
        // if ($("#profile").contents().find("html").hasChildNodes()) {
        if ($("#profile").contents().find("html").find('.container').length) {
          log.debug('iframe loaded. removing spinner')
          spinner_el.Destroy();
          clearInterval(timer_handle);
        }
      }
    }
);
