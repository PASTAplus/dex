let seconds = 0;

$(document).ready(function () {
  $('#generate').click(async function () {
    $(this).css("display", "none");
    $('#spinner-msg').css("display", "block");

    setInterval(function () {
      seconds += 1;
      $('#counter-msg').text(`Elapsed: ${seconds}s`);
    }, 1000);

    window.open(`/dex/profile/doc/${g.rid}`, "_self")

    // let response = await fetch(`/dex/profile-fetch/${g.rid}`);
    // document.open('text/html');
    // document.write(await response.text());
    // $('#profile').src = `/dex/profile/profile-fetch/${g.rid}`
    // $('#profile').load(function(){$(this).height($(this).contents().outerHeight());});

  });
});

