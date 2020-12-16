let start_ts;
let interval_handle;

function start()
{

}

// language=HTML
const INJECT_HTML = `
  <div class='dex-loading'>
    <div id='loading-msg'></div>
    <div id='counter-msg'></div>
  </div>
`;

// (function ($) {


// $.LoadingDots = function (el, options) {
//   var base = this;
//   base.$el = $(el);
//   base.$el.data("LoadingDots", base);
//
//   base.dotItUp = function ($element, maxDots) {
//     if ($element.text().length == maxDots) {
//       $element.text("");
//     } else {
//       $element.append(".");
//     }
//   };
//
//   base.stopInterval = function () {
//     clearInterval(base.theInterval);
//   };
//

// $.fn.LoadingDots = function (options) {
//   if (typeof (options) == "string") {
//     var safeGuard = $(this).data('LoadingDots');
//     if (safeGuard) {
//       safeGuard.stopInterval();
//       this[0].innerHTML = safeGuard.h;
//     }
//   }
//   else {
//     return this.each(function () {
//       (new $.LoadingDots(this, options));
//     });
//   }
// };


// Add the "Loading()" function to all elements. Calling the function replaces any
// current contents in the element with a message saying, "Loading..." and a display
// of elapsed time.
$.fn.Loading = function (msg_str = 'Loading...') {
  return this.each(function () {
    (new $.LoadingDots(this, options));
  })
};

$.fn.Destroy = function () {
  return this.each(function () {
    (new $.LoadingDots(this, options));
  })
};


if (typeof (options) == "string") {
  var safeGuard = $(this).data('LoadingDots');
  if (safeGuard) {
    safeGuard.stopInterval();
    this[0].innerHTML = safeGuard.h;
  }
}
else {
}



$.LoadingDots = function (el, options) {
  var base = this;
  base.$el = $(el);
  base.$el.data("LoadingDots", base);

  base.dotItUp = function ($element, maxDots) {
    if ($element.text().length === maxDots) {
      $element.text("");
    }
    else {
      $element.append(".");
    }
  };

  base.stopInterval = function () {
    clearInterval(base.theInterval);
  };

  base.init = function () {
    if (typeof (speed) === "undefined" || speed === null) {
      speed = 300;
    }
    if (typeof (maxDots) === "undefined" || maxDots === null) {
      maxDots = 3;
    }
    base.speed = speed;
    base.maxDots = maxDots;
    base.options = $.extend({}, $.LoadingDots.defaultOptions, options);
    // base.origHTML = base.$el.html();
    // alert(base.origHTML);
    // alert(base.$el.html());
    base.h = base.$el.html();

    base.$el.html(
        "<span class='select2-selection__rendered'>" + base.options.word + "<em></em></span>");
    base.$dots = base.$el.find("em");
    base.$loadingText = base.$el.find("span");
    base.$el.css("position", "relative");
    base.$loadingText.css({
      "position": "absolute",
      "top": (base.$el.outerHeight() / 2) - (base.$loadingText.outerHeight() / 2) + 2,
      "left": "8px",
      // "left": (base.$el.width() / 2) - (base.$loadingText.width() / 2)
    });
  };

  base.init();
};

$.LoadingDots.defaultOptions = {
  speed: 300,
  maxDots: 3,
  word: "Loading"
};


// function stop() {
//
// }

function start_timer()
{
  start_ts = Date.now();
  interval_handle = setInterval(function () {
    update_elapsed_sec();
  }, 1000);
}

function update_elapsed_sec()
{
  // Since this method is called at the full second mark, we could end up randomly
  // switching to displaying the new value just before or just after it has changed,
  // which would cause the counter stay at a value for an extra second or to skip
  // a value. To avoid that, we add 500 ms and then round the resulting elapsed time.
  $('#counter-msg').text(`Elapsed: ${Math.round(Date.now() - start_ts + 500)}s`);
}


// })(jQuery);

