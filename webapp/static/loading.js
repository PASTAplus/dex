// Display a simple animation indicating that we are waiting for a task to complete.
//
// This adds a function, `Loading()` to all elements. Calling Loading() on an element replaces any
// current contents in the element with a message and a count of elapsed time, in seconds. The
// message is "Loading" by default and includes a simple animation where trailing dots are added up
// to MAX_DOTS (5 by default), and then starts over.
//
// Typically, the animation is destroyed implicitly when the task for which we are waiting
// completes, and the current document is replaced or the elements are overwritten. But it's also
// possible to stop the animation and restore the element to its previous state by calling
// Destroy().

const INJECT_HTML = `
  <div id='loading-outer'>
    <div id='loading-msg'></div>
    <div id='loading-elapsed'></div>
  </div>
`;

$(document).ready(function () {
  // Add the "Loading()" function to all elements.
  $.fn.Loading = function (msg_str = 'Loading') {
    // Calling "Loading()" on an element starts the animation and adds a new function,
    // "Destroy()" (described above) to the element.
    $.fn.Destroy = function () {
      StopAnim($(this)[0]);
    }
    return this.each(function () {
      StartAnim(this, msg_str);
    })
  };
});

// Replace any existing contents in the element with the new, animated message. Keep
// the old content, for use in Destroy().
// $.StartAnim = function (host_el, msg_str) {
function StartAnim(host_el, msg_str) {
  let g = Object;
  host_el.g = g;
  g.origHtml = host_el.innerHTML;
  host_el.innerHTML = INJECT_HTML;

  g.msg_str = msg_str;
  g.start_ts = Date.now();
  g.interval_handle = setInterval(function () {
    update(g);
  }, 500);
  g.dot_count = 0;

  update(g);
}

// $.StopAnim = function (host_el) {
function StopAnim(host_el) {
  clearInterval(host_el.g.interval_handle);
  host_el.innerHTML = host_el.g.origHtml;
}

function update(base)
{
  if (base.dot_count++ === 5) {
    base.dot_count = 0;
  }
  $('#loading-msg').html(base.msg_str + '.'.repeat(base.dot_count));
  // Since this method is called at the full second mark, we could end up randomly
  // switching to displaying the new value just before or just after it has changed,
  // which would cause the counter stay at a value for an extra second or to skip
  // a value. To avoid that, we add 500 ms and then round the resulting elapsed time.
  $('#loading-elapsed').text(`Elapsed: ${Math.round((Date.now() - base.start_ts + 500) / 1000)}s`);
}
