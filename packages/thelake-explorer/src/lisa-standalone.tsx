import { createRoot } from "react-dom/client";
import { LisaApp } from "./LisaApp";

declare global {
  interface Window {
    __LISA_API_BASE_PATH__?: string;
  }
}

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");
const configuredExplorerUrl = document.querySelector<HTMLMetaElement>(
  'meta[name="thelake-explorer-url"]',
)?.content.trim();
const explorerUrl = configuredExplorerUrl && !configuredExplorerUrl.includes("__LISA_EXPLORER_URL__")
  ? configuredExplorerUrl
  : undefined;
createRoot(root).render(<LisaApp config={{
  apiBasePath: "/v1",
  lisaApiBasePath: window.__LISA_API_BASE_PATH__,
}} explorerUrl={explorerUrl || undefined} />);
