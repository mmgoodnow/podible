# UI Interaction Model

The UI is organized around the person using the library, not the services or
workers implementing it. A renamed directory of internal tools is not sufficient.

## Primary Tasks

- Find something to read or watch: artwork-led Library with local results first.
- Check a request: visible availability, season coverage and progress on its card;
  open that title for its request/detail, not a general integration dashboard.
- Resolve a problem: Activity shows the affected title, current state and relevant
  action. Automatic retries, queued work and ordinary cooldowns are not demands
  for human intervention.
- Change configuration: Settings. Integrations and raw execution history are not
  primary browsing destinations.

## Disclosure And Evidence

Useful state and consequential actions stay visible. The first row of pending
requests is visible without expanding anything. Additional requests, tracker
facts, filesystem paths and execution identifiers are secondary details.

Actions stay in context: retry beside a stopped operation, connection settings
beside credential failures, storage checks beside storage failures. Technical
detail must not be a prerequisite for the normal corrective action.

Progress includes every download in the operation. Unknown member telemetry means
unknown overall progress; recorded progress must not be represented as live.
Unknown storage is not healthy. Preview/disclosure never performs a mutation.

## Basis And Acceptance

- [NN/g: Progressive disclosure](https://www.nngroup.com/articles/progressive-disclosure/):
  prioritize common tasks and reveal specialized detail with clear, contextual controls.
- [NN/g: Visibility of system status](https://www.nngroup.com/articles/visibility-system-status/):
  state and feedback should support the person's next decision.
- [Apple: Navigation design](https://developer.apple.com/videos/play/wwdc2022/10001/):
  distinct primary destinations, predictable drill-in and a clear path back.
- [GOV.UK: Details](https://design-system.service.gov.uk/components/details/):
  secondary content can be disclosed; information most people need must not be hidden.

Acceptance tests cover actual user journeys, not just page headings: initial and
background Library reads, title/request navigation, a direct problem retry without
opening execution history, contextual storage inspection, keyboard disclosure,
role boundaries, missing artwork and mobile overflow.
