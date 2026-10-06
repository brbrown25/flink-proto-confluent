// Mounts every <div class="cast" data-src="..."> as an asciinema player.
function mountCasts() {
  document.querySelectorAll("div.cast[data-src]").forEach(function (el) {
    if (el.dataset.mounted) return;
    el.dataset.mounted = "1";
    AsciinemaPlayer.create(el.dataset.src, el, {
      cols: parseInt(el.dataset.cols || "110", 10),
      rows: parseInt(el.dataset.rows || "28", 10),
      idleTimeLimit: 2,
      fit: "width",
      theme: "monokai",
    });
  });
}
if (typeof document$ !== "undefined") {
  document$.subscribe(mountCasts);
} else {
  document.addEventListener("DOMContentLoaded", mountCasts);
}
