# Rules for every review

Loom's rules shared by the factory's code and security reviews, adapted on 2026-10-23 from Thread
Voice's. A "must fix" is an item under the gate file's `## Must fix`; a "note" goes under
`## Notes` and never blocks.

- From round 2 on, check that each of your earlier must-fix items was resolved.
- Don't add a note for a tradeoff that the plan's risks accept or that the spec puts out of scope,
  unless the diff makes it worse than described. In that case, cite the line.
- A note meant for later work names the issue key (`LM-<n>`) when an issue exists, or says that a
  new issue is needed.
