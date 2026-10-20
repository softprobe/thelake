import { createRoot } from "react-dom/client";
import { LisaApp } from "./LisaApp";

declare global {
  interface Window {
    __LISA_API_BASE_PATH__?: string;
  }
}

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");
createRoot(root).render(<LisaApp config={{
  apiBasePath: "/v1",
  lisaApiBasePath: window.__LISA_API_BASE_PATH__,
}} />);
