const demo = document.querySelector("[data-story-demo]");

if (demo) {
  const tabs = Array.from(demo.querySelectorAll('[role="tab"]'));
  const panels = Array.from(demo.querySelectorAll('[role="tabpanel"]'));
  const currentStep = demo.querySelector("[data-step-current]");
  const toggle = demo.querySelector("[data-autoplay-toggle]");
  const toggleLabel = demo.querySelector("[data-autoplay-label]");
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");

  let activeIndex = 0;
  let timer = 0;
  let inView = true;
  let paused = reducedMotion.matches;

  const updatePauseControl = () => {
    toggle.setAttribute("aria-pressed", String(paused));
    toggle.setAttribute(
      "aria-label",
      paused ? "Resume automatic walkthrough" : "Pause automatic walkthrough",
    );
    toggleLabel.textContent = paused ? "Play" : "Pause";
    toggle.querySelector("[aria-hidden='true']").textContent = paused ? "▶" : "Ⅱ";
  };

  const scheduleNextStep = () => {
    window.clearTimeout(timer);
    if (paused || !inView || document.hidden || reducedMotion.matches) return;

    timer = window.setTimeout(() => {
      activateStep((activeIndex + 1) % tabs.length);
    }, 7200);
  };

  const pauseForInteraction = (event) => {
    if (event.target.closest("[data-autoplay-toggle]")) return;
    if (paused) return;

    paused = true;
    updatePauseControl();
    scheduleNextStep();
  };

  const activateStep = (index, moveFocus = false) => {
    activeIndex = index;
    tabs.forEach((tab, tabIndex) => {
      const selected = tabIndex === activeIndex;
      tab.setAttribute("aria-selected", String(selected));
      tab.tabIndex = selected ? 0 : -1;
      tab.classList.toggle("is-active", selected);
    });

    panels.forEach((panel, panelIndex) => {
      const selected = panelIndex === activeIndex;
      panel.hidden = !selected;
      panel.classList.toggle("is-active", selected);
      if (selected) {
        panel.classList.remove("step-enter");
        void panel.offsetWidth;
        panel.classList.add("step-enter");
      }
    });

    currentStep.textContent = String(activeIndex + 1).padStart(2, "0");
    if (moveFocus) tabs[activeIndex].focus();
    scheduleNextStep();
  };

  tabs.forEach((tab, index) => {
    tab.addEventListener("click", () => activateStep(index));
    tab.addEventListener("keydown", (event) => {
      let nextIndex = index;
      if (event.key === "ArrowDown" || event.key === "ArrowRight") nextIndex = (index + 1) % tabs.length;
      else if (event.key === "ArrowUp" || event.key === "ArrowLeft") nextIndex = (index - 1 + tabs.length) % tabs.length;
      else if (event.key === "Home") nextIndex = 0;
      else if (event.key === "End") nextIndex = tabs.length - 1;
      else return;

      event.preventDefault();
      activateStep(nextIndex, true);
    });
  });

  demo.addEventListener("focusin", pauseForInteraction);
  demo.addEventListener("pointerdown", pauseForInteraction);

  demo.addEventListener("click", (event) => {
    const control = event.target.closest("[data-go-step]");
    if (!control || !demo.contains(control)) return;

    const requestedStep = Number(control.dataset.goStep);
    if (!Number.isInteger(requestedStep) || requestedStep < 1 || requestedStep > tabs.length) return;

    activateStep(requestedStep - 1, true);
  });

  toggle.addEventListener("click", () => {
    paused = !paused;
    updatePauseControl();
    scheduleNextStep();
  });

  document.addEventListener("visibilitychange", scheduleNextStep);
  reducedMotion.addEventListener("change", (event) => {
    if (event.matches) paused = true;
    updatePauseControl();
    scheduleNextStep();
  });

  if ("IntersectionObserver" in window) {
    const observer = new IntersectionObserver(([entry]) => {
      inView = entry.isIntersecting;
      scheduleNextStep();
    }, { threshold: 0.18 });
    observer.observe(demo);
  }

  updatePauseControl();
  activateStep(0);
}
