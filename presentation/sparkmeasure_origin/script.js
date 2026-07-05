const progressBar = document.getElementById('progressBar');
const navDots = [...document.querySelectorAll('.nav-dot')];
const observedSections = [...document.querySelectorAll('.section-observed')];

function updateProgress() {
  const scrollTop = window.scrollY || document.documentElement.scrollTop;
  const docHeight = document.documentElement.scrollHeight - window.innerHeight;
  const progress = docHeight > 0 ? (scrollTop / docHeight) * 100 : 0;
  progressBar.style.width = `${progress}%`;
}

const observer = new IntersectionObserver((entries) => {
  entries.forEach((entry) => {
    if (entry.isIntersecting) {
      entry.target.classList.add('visible');
      const id = entry.target.dataset.section;
      navDots.forEach((dot) => dot.classList.toggle('active', dot.dataset.target === id));
    }
  });
}, { threshold: 0.22 });

observedSections.forEach((section) => observer.observe(section));
window.addEventListener('scroll', updateProgress, { passive: true });
window.addEventListener('resize', updateProgress);
updateProgress();
observedSections[0]?.classList.add('visible');

const printBtn = document.getElementById('printBtn');
printBtn?.addEventListener('click', () => window.print());

document.addEventListener('keydown', (event) => {
  if (!['ArrowDown', 'PageDown', 'ArrowUp', 'PageUp'].includes(event.key)) return;
  const sections = ['top', 'slide-1', 'slide-2', 'slide-3', 'closing'].map((id) => document.getElementById(id) || document.querySelector(`[data-section="${id}"]`));
  const currentIndex = sections.findIndex((section) => {
    if (!section) return false;
    const rect = section.getBoundingClientRect();
    return rect.top <= window.innerHeight * 0.34 && rect.bottom >= window.innerHeight * 0.34;
  });
  let nextIndex = currentIndex;
  if (event.key === 'ArrowDown' || event.key === 'PageDown') nextIndex = Math.min(sections.length - 1, currentIndex + 1);
  if (event.key === 'ArrowUp' || event.key === 'PageUp') nextIndex = Math.max(0, currentIndex - 1);
  const next = sections[nextIndex];
  if (next && nextIndex !== currentIndex) {
    event.preventDefault();
    next.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
});
