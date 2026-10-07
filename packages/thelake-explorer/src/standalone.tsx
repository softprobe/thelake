import { createRoot } from "react-dom/client";
import { ThelakeExplorer } from "./Explorer";
import "./style.css";

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");
createRoot(root).render(<ThelakeExplorer config={{ apiBasePath: "/v1" }} />);
