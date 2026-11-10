import { useEffect, useEffectEvent } from "react";

export function useAcquisitionEvents(enabled: boolean, onChange: () => void) {
  const invalidate = useEffectEvent(onChange);
  useEffect(() => {
    if (!enabled) return;
    const stream = new EventSource("/api/events/acquisition");
    stream.addEventListener("change", () => invalidate());
    return () => stream.close();
  }, [enabled]);
}
