/** Hard identity/compatibility constraints precede any quality preference. Equal never churns. */
export function assessCompatibleUpgrade(input: {
  compatible: boolean;
  verified: boolean;
  improvement: "better" | "equal" | "worse" | "unknown";
}) {
  if (!input.compatible) return { upgrade: false, reason: "incompatible or unproven edition" };
  if (!input.verified) return { upgrade: false, reason: "candidate is not verified" };
  if (input.improvement !== "better") return { upgrade: false, reason: "keep current version" };
  return { upgrade: true, reason: "verified compatible quality improvement" };
}
