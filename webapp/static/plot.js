let seconds;

$(document).ready(function () {


  // $("#testy").click(async function () {
  //   // alert('1');
  //   // let plot_el = $('#plot-container');
  //   let el = $("#testydiv");
  //
  //   el.Loading();
  //   setTimeout(function() {el.Destroy()}, 5000);
  //   // alert('2');
  //
  //
  // });
  //

  $("#plot").click(async function () {
    let plot_el = $('#plot-container');

    while (plot_el[0].firstChild) {
      plot_el[0].removeChild(plot_el[0].firstChild);
    }

    // let $ = jQuery.noConflict();

    // $(this).css("display", "none");
    $('#spinner-msg').css("display", "block");

    seconds = 0;
    let h = setInterval(function () {
      seconds += 1;
      $('#counter-msg').text(`Elapsed: ${seconds}s`);
    }, 1000);


    let x = $('#x-col option:selected').val()
    let y = $('#y-col option:selected').val()
    let response = await fetch(`/bokeh/xy-plot/${g.rid}/${x}/${y}`);
    let data = await response.json();

    Bokeh.embed.embed_item(data, 'plot-container');


    $('#spinner-msg').css("display", "none");

    clearInterval(h);

    // window.open(`/dex/profile/doc/${g.rid}`, "_self")

    // let response = await fetch(`/dex/profile-fetch/${g.rid}`);
    // document.open('text/html');
    // document.write(await response.text());
    // $('#profile').src = `/dex/profile/profile-fetch/${g.rid}`
    // $('#profile').load(function(){$(this).height($(this).contents().outerHeight());});
  });
});
