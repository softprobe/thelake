import { useMemo } from "react";
import { ExplorerApi, type ExplorerConfig } from "./client";
import { LisaView } from "./LisaView";
import "./style.css";

export type LisaAppProps = { config: ExplorerConfig; explorerUrl?: string };

export function LisaApp({ config, explorerUrl }: LisaAppProps) {
  const api = useMemo(() => new ExplorerApi(config), [config]);
  return (
    <main className="thelake-explorer lisa-app">
      <LisaView
        key={config.chatStorageKey ?? "workspace-unspecified"}
        api={api}
        onShowSessions={explorerUrl ? () => { window.location.assign(explorerUrl); } : undefined}
      />
    </main>
  );
}
