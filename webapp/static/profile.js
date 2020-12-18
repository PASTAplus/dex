$(document).ready(
    async function () {
      if (!g.is_cached) {
        $('#generate-msg').Loading('Generating profile for CSV (may take several minutes)');
        $('#profile').on('load', function () {
          $('#generate-msg').Destroy();
        });
      }
    }
);
