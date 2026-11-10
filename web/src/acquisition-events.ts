import { useEffect, useEffectEvent } from "react";
import { rpc } from "./rpc";

export function useAcquisitionEvents(enabled: boolean, onChange: () => void) {
  const invalidate = useEffectEvent(onChange);
  useEffect(() => {
    if (!enabled) return;
    const subscription = rpc.acquisition.changes.subscribe(undefined, { onData: () => invalidate() });
    return () => subscription.unsubscribe();
  }, [enabled]);
}
