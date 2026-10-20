const wordmarkUrl = new URL("./softprobe-wordmark.svg", import.meta.url).href;

type SoftprobeWordmarkProps = {
  className?: string;
};

/** Prototype Softprobe wordmark — shared across signup, onboarding, and product shell. */
export function SoftprobeWordmark({ className = "signup-brand-logo" }: SoftprobeWordmarkProps) {
  return <img className={className} src={wordmarkUrl} alt="Softprobe" />;
}

type SoftprobeMarkProps = {
  className?: string;
};

/** Collapsed-sidebar mark from the Softprobe MVP UI Prototype. */
export function SoftprobeMark({ className = "brand-logo-icon" }: SoftprobeMarkProps) {
  return (
    <svg className={className} viewBox="0 0 100 100" role="img" aria-label="Softprobe">
      <path
        d="M30 13h40c9.326 0 17 7.674 17 17v40c0 9.326-7.674 17-17 17H30c-9.326 0-17-7.674-17-17V30c0-9.326 7.674-17 17-17Z"
        fill="none"
        stroke="currentColor"
        strokeWidth="9"
      />
      <path
        d="M40.258 9.084 78.895 19.437A17 17 0 0 1 90.916 40.258L80.563 78.895A17 17 0 0 1 59.742 90.916L21.105 80.563A17 17 0 0 1 9.084 59.742L19.437 21.105A17 17 0 0 1 40.258 9.084Z"
        fill="none"
        stroke="currentColor"
        strokeWidth="9"
      />
      <path
        d="m48.983 38.34 7.728 2.07a7 7 0 0 1 4.949 8.573l-2.07 7.728a7 7 0 0 1-8.573 4.949l-7.728-2.07a7 7 0 0 1-4.949-8.573l2.07-7.728a7 7 0 0 1 8.573-4.949Z"
        fill="currentColor"
      />
    </svg>
  );
}
