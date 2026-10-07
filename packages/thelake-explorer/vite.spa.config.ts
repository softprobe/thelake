import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  root: ".",
  base: "/explorer/",
  plugins: [react()],
  server: {
    proxy: {
      "/v1": process.env.THELAKE_API_PROXY || "http://127.0.0.1:18090",
    },
  },
  build: {
    outDir: process.env.THELAKE_EXPLORER_OUT_DIR || "embedded",
    emptyOutDir: true,
    rollupOptions: { input: "index.html" },
  },
});
