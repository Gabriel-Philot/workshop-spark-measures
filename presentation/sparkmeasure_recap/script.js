(() => {
  "use strict";

  const body = document.body;
  const sections = Array.from(document.querySelectorAll(".deck-slide"));
  const navDots = Array.from(document.querySelectorAll(".nav-dot"));
  const progressBar = document.getElementById("progressBar");
  const notesToggle = document.getElementById("notesToggle");
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
  let activeIndex = 0;

  body.classList.add("js-ready");

  const clampIndex = (index) => Math.max(0, Math.min(index, sections.length - 1));

  function updateNotesAccessibility() {
    const notesAreVisible = body.classList.contains("notes-visible");

    sections.forEach((section, index) => {
      const notes = section.querySelector(".speaker-notes");
      if (notes) {
        notes.setAttribute(
          "aria-hidden",
          String(!notesAreVisible || index !== activeIndex),
        );
      }
    });
  }

  function setActive(index) {
    activeIndex = clampIndex(index);

    sections.forEach((section, sectionIndex) => {
      const isActive = sectionIndex === activeIndex;
      section.classList.toggle("is-active", isActive);
      if (isActive) section.classList.add("has-been-active");
    });

    navDots.forEach((dot, dotIndex) => {
      const isActive = dotIndex === activeIndex;
      dot.classList.toggle("is-active", isActive);
      if (isActive) dot.setAttribute("aria-current", "step");
      else dot.removeAttribute("aria-current");
    });

    const activeSection = sections[activeIndex];
    body.classList.toggle("dark-active", activeSection.classList.contains("slide-dark"));
    progressBar.style.width = `${((activeIndex + 1) / sections.length) * 100}%`;
    updateNotesAccessibility();

    if (window.location.hash !== `#${activeSection.id}`) {
      history.replaceState(null, "", `#${activeSection.id}`);
    }
  }

  function goTo(index) {
    const nextIndex = clampIndex(index);
    const nextSection = sections[nextIndex];
    setActive(nextIndex);
    nextSection.scrollIntoView({
      behavior: reducedMotion.matches ? "auto" : "smooth",
      block: "start",
    });
  }

  function toggleNotes(forceState) {
    const nextState =
      typeof forceState === "boolean"
        ? forceState
        : !body.classList.contains("notes-visible");

    body.classList.toggle("notes-visible", nextState);
    notesToggle.setAttribute("aria-pressed", String(nextState));
    notesToggle.textContent = nextState ? "Ocultar notas" : "Notas";
    updateNotesAccessibility();
  }

  function keyIsEditable(target) {
    return Boolean(
      target &&
        (target.matches("input, textarea, select, button") || target.isContentEditable),
    );
  }

  document.addEventListener("keydown", (event) => {
    if (keyIsEditable(event.target)) return;

    const nextKeys = ["ArrowDown", "ArrowRight", "PageDown", " "];
    const previousKeys = ["ArrowUp", "ArrowLeft", "PageUp"];

    if (nextKeys.includes(event.key)) {
      event.preventDefault();
      goTo(activeIndex + 1);
      return;
    }

    if (previousKeys.includes(event.key)) {
      event.preventDefault();
      goTo(activeIndex - 1);
      return;
    }

    if (event.key === "Home") {
      event.preventDefault();
      goTo(0);
    } else if (event.key === "End") {
      event.preventDefault();
      goTo(sections.length - 1);
    } else if (event.key.toLowerCase() === "n") {
      event.preventDefault();
      toggleNotes();
    } else if (event.key === "Escape") {
      toggleNotes(false);
    }
  });

  navDots.forEach((dot, index) => {
    dot.addEventListener("click", (event) => {
      event.preventDefault();
      goTo(index);
    });
  });

  notesToggle.addEventListener("click", () => toggleNotes());

  const observer = new IntersectionObserver(
    (entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort((a, b) => b.intersectionRatio - a.intersectionRatio)[0];

      if (!visible) return;
      const index = sections.indexOf(visible.target);
      if (index >= 0) setActive(index);
    },
    { threshold: [0.35, 0.55, 0.72] },
  );

  sections.forEach((section) => observer.observe(section));

  document.querySelectorAll("[data-asset-src]").forEach((slot) => {
    const assetPath = slot.getAttribute("data-asset-src");
    if (!assetPath) return;

    const asset = new Image();
    asset.addEventListener("load", () => {
      slot.style.backgroundImage = `url("${assetPath}")`;
      slot.classList.add("asset-loaded");
    });
    asset.src = assetPath;
  });

  const hashIndex = sections.findIndex(
    (section) => `#${section.id}` === window.location.hash,
  );
  setActive(hashIndex >= 0 ? hashIndex : 0);
})();
