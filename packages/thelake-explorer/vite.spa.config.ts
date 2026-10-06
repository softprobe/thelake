import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  root: ".",
  base: "/explorer/",
  plugins: [react()],
  build: {
    outDir: process.env.THELAKE_EXPLORER_OUT_DIR || "embedded",
    emptyOutDir: true,
    rollupOptions: { input: "index.html" },
  },
});
