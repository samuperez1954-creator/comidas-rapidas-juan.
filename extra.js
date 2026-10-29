(function () {
  var hero = document.querySelector('.hero');
  if (!hero || matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  var v = document.createElement('video');
  v.className = 'hero-video';
  v.muted = true; v.defaultMuted = true; v.setAttribute('muted', '');
  v.autoplay = true; v.loop = true; v.playsInline = true;
  v.setAttribute('playsinline', ''); v.setAttribute('aria-hidden', 'true');
  v.preload = 'auto';
  v.src = '/hero-hd.mp4';
  v.addEventListener('playing', function () { hero.classList.add('has-video'); });
  var img = hero.querySelector('.hero-img');
  if (img) img.insertAdjacentElement('afterend', v); else hero.prepend(v);
  var p = v.play(); if (p && p.catch) p.catch(function () {});
})();
