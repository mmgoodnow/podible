import type { RecoveryResource } from "../../src/media/recovery-types";

export const recoveryResourceKey = (resource: Pick<RecoveryResource, "key">) => `${resource.key.namespace}:${resource.key.id}`;

export function recoveryView(resources: readonly RecoveryResource[]) {
  const missing: RecoveryResource[] = [], unchecked: RecoveryResource[] = [];
  let checkedCount = 0;
  for (const resource of resources) {
    if (resource.mount.state === "unavailable") {
      unchecked.push(resource);
      continue;
    }
    // Only aggregate selected-content/view evidence establishes a missing resource.
    if (resource.canonical.state === "missing" || resource.view.state === "missing" || resource.view.state === "conflict") {
      missing.push(resource);
      checkedCount++;
    } else if (resource.canonical.state === "present" && resource.view.state === "present") {
      checkedCount++;
    } else if (resource.canonical.state === "unknown" || resource.view.state === "unknown") {
      unchecked.push(resource);
    } else {
      // Initial requests with no selected version are not evidence of lost bytes.
      checkedCount++;
    }
  }
  return { missing, unchecked, checkedCount,
    actionable: missing.filter(resource => resource.action === "repair-view" || resource.action === "queue-acquisition"),
  };
}
