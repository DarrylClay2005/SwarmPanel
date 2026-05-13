import { safeHex } from "./format.js";

export function payloadForAction(form) {
  if (form.action === "PLAY") {
    return { source_url: form.source_url, voice_channel_id: form.voice_channel_id, text_channel_id: form.text_channel_id || 0 };
  }
  if (form.action === "SMART_RECOMMEND") {
    return { voice_channel_id: form.voice_channel_id, text_channel_id: form.text_channel_id || 0 };
  }
  if (form.action === "SET_HOME") {
    return { voice_channel_id: form.voice_channel_id };
  }
  if (form.action === "LOOP") return { loop_mode: form.loop_mode };
  if (form.action === "FILTER") return { filter_mode: form.filter_mode };
  return {};
}

export function panelStyle(preferences) {
  const accent = safeHex(preferences.accent_color, "#89b4fa");
  const background = safeHex(preferences.background_color, "#0b0e18");
  return {
    "--accent": accent,
    "--bg": preferences.background_mode === "custom_color" ? background : "#0d1117",
    "--surface-opacity": preferences.surface_opacity ?? 0.92,
    "--surface-blur": `${preferences.surface_blur ?? 18}px`,
  };
}
