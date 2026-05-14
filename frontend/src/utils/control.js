import { safeHex } from "./format.js";

export function payloadForAction(form) {
  if (form.action === "PLAY") {
    return {
      source_url: form.source_url,
      voice_channel_id: form.voice_channel_id,
      text_channel_id: form.text_channel_id || 0,
      loop_mode: "queue",
      shuffle_before_play: true,
      save_playlist: true,
    };
  }
  if (form.action === "SMART_RECOMMEND") {
    return {
      voice_channel_id: form.voice_channel_id,
      text_channel_id: form.text_channel_id || 0,
      loop_mode: "queue",
      shuffle_before_play: true,
    };
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
  const imageUrl = String(preferences.background_image_url || "").trim();
  const useImage = preferences.background_mode === "custom_image" && /^https?:\/\//i.test(imageUrl);
  const baseBackground = preferences.background_mode === "custom_color" ? background : "#0d1117";
  return {
    "--accent": accent,
    "--bg": baseBackground,
    "--panel-bg-image": useImage ? `url("${imageUrl.replace(/["\\]/g, "\\$&")}")` : "none",
    "--surface-opacity": preferences.surface_opacity ?? 0.92,
    "--surface-blur": `${preferences.surface_blur ?? 18}px`,
  };
}
