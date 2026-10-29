(function () {
  var hero = document.querySelector('.hero');
  var v = document.getElementById('heroVideo');
  if (!hero || !v) return;
  v.muted = true; v.defaultMuted = true;
  hero.classList.add('has-video');            // el poster ya muestra la hamburguesa
  function play() { var p = v.play(); if (p && p.catch) p.catch(function () {}); }
  v.addEventListener('error', function () { hero.classList.remove('has-video'); }, true);
  var src = v.querySelector('source');
  if (src) src.addEventListener('error', function () { hero.classList.remove('has-video'); });
  play();
  // Si el teléfono bloqueó la reproducción automática (ahorro de batería), arranca al primer toque o scroll
  ['touchstart', 'scroll', 'click'].forEach(function (ev) {
    window.addEventListener(ev, function once() { if (v.paused) play(); window.removeEventListener(ev, once); }, { passive: true });
  });
  document.addEventListener('visibilitychange', function () { if (!document.hidden && v.paused) play(); });
})();
