import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App.jsx";
import "./styles.css";

function runtimeBasename() {
  const configured = String(window.SWARM_PANEL_BASENAME || "").replace(/\/+$/, "");
  if (configured) return configured;
  const path = window.location.pathname.replace(/\/+$/, "");
  if (path.startsWith("/static/react")) return "/static/react";
  if (path.startsWith("/app/static/react")) return "/app/static/react";
  return "";
}

function applyRuntimeClasses() {
  const root = document.documentElement;
  const userAgent = navigator.userAgent || "";
  const isOpera = /\bOPR\//.test(userAgent) || /\bOpera\//.test(userAgent);
  const mobileQuery = window.matchMedia("(max-width: 820px), (pointer: coarse)");
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
  root.classList.toggle("is-opera", isOpera);
  root.classList.toggle("is-mobile-runtime", mobileQuery.matches);
  root.classList.toggle("perf-lite", isOpera || mobileQuery.matches || reducedMotion.matches);
  const refresh = () => {
    root.classList.toggle("is-mobile-runtime", mobileQuery.matches);
    root.classList.toggle("perf-lite", isOpera || mobileQuery.matches || reducedMotion.matches);
  };
  mobileQuery.addEventListener?.("change", refresh);
  reducedMotion.addEventListener?.("change", refresh);
}

applyRuntimeClasses();

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <BrowserRouter basename={runtimeBasename()}>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
);
