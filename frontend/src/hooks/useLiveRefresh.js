import { useEffect, useRef } from "react";

export function useLiveRefresh(callback, { enabled = true, interval = 20_000, immediate = false } = {}) {
  const callbackRef = useRef(callback);

  useEffect(() => {
    callbackRef.current = callback;
  }, [callback]);

  useEffect(() => {
    if (!enabled || typeof window === "undefined") return undefined;
    let stopped = false;
    let timer = 0;

    const run = () => {
      if (stopped || document.hidden || navigator.onLine === false) return;
      Promise.resolve(callbackRef.current?.()).catch(() => {});
    };

    if (immediate) run();
    timer = window.setInterval(run, interval);
    document.addEventListener("visibilitychange", run);
    window.addEventListener("online", run);

    return () => {
      stopped = true;
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", run);
      window.removeEventListener("online", run);
    };
  }, [enabled, immediate, interval]);
}
