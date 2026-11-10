import { useSyncExternalStore, type AnchorHTMLAttributes } from "react";
function subscribe(listener: () => void) {
  window.addEventListener("popstate", listener);
  return () => window.removeEventListener("popstate", listener);
}
export function useLocation() {
  return useSyncExternalStore(
    subscribe,
    () => location.pathname + location.search,
  );
}
export function navigate(href: string, { replace = false } = {}) {
  if (replace) history.replaceState(null, "", href);
  else history.pushState(null, "", href);
  window.dispatchEvent(new PopStateEvent("popstate"));
  window.scrollTo(0, 0);
}
export function Link({
  href = "/",
  onClick,
  ...props
}: AnchorHTMLAttributes<HTMLAnchorElement>) {
  return (
    <a
      {...props}
      href={href}
      onClick={(event) => {
        onClick?.(event);
        if (
          event.defaultPrevented ||
          event.button !== 0 ||
          event.metaKey ||
          event.ctrlKey ||
          event.shiftKey ||
          event.altKey ||
          props.download !== undefined ||
          (props.target && props.target !== "_self") ||
          new URL(href, location.href).origin !== location.origin
        )
          return;
        event.preventDefault();
        navigate(href);
      }}
    />
  );
}
