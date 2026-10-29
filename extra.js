(function () {
  var hero = document.querySelector('.hero');
  var v = document.getElementById('heroVideo');
  if (!hero || !v) return;
  v.muted = true; v.defaultMuted = true; v.controls = false;
  v.removeAttribute('controls');
  function play() { var p = v.play(); if (p && p.catch) p.catch(function () {}); }
  var srcs = v.querySelectorAll('source');
  if (srcs.length) srcs[srcs.length - 1].addEventListener('error', function () { hero.classList.add('no-video'); });
  play();
  v.addEventListener('canplay', play);
  // Si el teléfono bloqueó la reproducción automática (modo ahorro de batería), arranca al primer toque o movimiento
  ['touchstart', 'touchend', 'scroll', 'click', 'pointerdown'].forEach(function (ev) {
    window.addEventListener(ev, function () { if (v.paused) play(); }, { passive: true });
  });
  document.addEventListener('visibilitychange', function () { if (!document.hidden && v.paused) play(); });
})();
