import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const reactIndex = readFileSync(resolve("app/static/react/index.html"), "utf8");
const scriptMatch = reactIndex.match(/src="\/static\/react\/assets\/([^"]+\.js)"/);
const styleMatch = reactIndex.match(/href="\/static\/react\/assets\/([^"]+\.css)"/);

if (!scriptMatch || !styleMatch) {
  throw new Error("Unable to find built React assets.");
}

const shell = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover" />
    <meta name="theme-color" content="#0d1117" />
    <meta name="description" content="SwarmPanel React command deck for music bots, Aria, admin recovery, diagnostics, and Image Gallery operations." />
    <link rel="icon" href="/favicon.ico" />
    <title>SwarmPanel</title>
    <script>
      (function () {
        var isPages = location.hostname.endsWith("github.io");
        var base = isPages ? "/SwarmPanel" : "";
        window.SWARM_PANEL_REMOTE_MODE = isPages;
        window.SWARM_PANEL_BASENAME = base;
        window.SWARM_PANEL_CONFIG_URL = base ? base + "/live-config.json" : "live-config.json";
        var assets = base + "/app/static/react/assets";
        document.write('<link rel="stylesheet" crossorigin href="' + assets + '/${styleMatch[1]}">');
        document.write('<script type="module" crossorigin src="' + assets + '/${scriptMatch[1]}"></scr' + 'ipt>');
      }());
    </script>
  </head>
  <body>
    <div id="root"></div>
  </body>
</html>
`;

writeFileSync(resolve("index.html"), shell);
writeFileSync(resolve("404.html"), shell);
