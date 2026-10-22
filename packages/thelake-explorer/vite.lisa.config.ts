import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";

const appRoot = fileURLToPath(new URL("./lisa/", import.meta.url));
const outputDir = fileURLToPath(new URL("./lisa-dist/", import.meta.url));

export default defineConfig({
  root: appRoot,
  base: "/lisa/",
  plugins: [react()],
  server: {
    proxy: {
      "/v1": process.env.THELAKE_API_PROXY || "http://127.0.0.1:18090",
    },
  },
  build: {
    outDir: process.env.LISA_FRONTEND_OUT_DIR || outputDir,
    emptyOutDir: true,
    rollupOptions: { input: fileURLToPath(new URL("./lisa/index.html", import.meta.url)) },
  },
});
