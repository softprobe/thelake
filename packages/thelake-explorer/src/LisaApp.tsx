import { useMemo } from "react";
import { ExplorerApi, type ExplorerConfig } from "./client";
import { LisaView } from "./LisaView";
import "./style.css";

export type LisaAppProps = { config: ExplorerConfig };

export function LisaApp({ config }: LisaAppProps) {
  const api = useMemo(() => new ExplorerApi(config), [config]);
  return (
    <main className="thelake-explorer lisa-app">
      <LisaView
        key={config.chatStorageKey ?? "workspace-unspecified"}
        api={api}
        onShowSessions={() => { window.location.assign("/explorer/"); }}
      />
    </main>
  );
}
