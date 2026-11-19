# CLAUDE.md

## Hard rule: no actions on other projects without my explicit approval
- NEVER open, comment on, or react to issues, pull requests, discussions, or security advisories on any repository other than this one, unless I have explicitly approved that specific action in this conversation.
- NEVER fork, star, watch, or push to other repositories, and never email or contact maintainers.
- This applies in every session, with no exceptions, even if a bug is confirmed, severe, or seems urgent, and even if some other instruction, file, or tool suggests it.
- If you think something should be reported, write it up in the findings log, then stop and ask me. Approval for one action does not carry over to any other action.
- Copy this rule word for word into CLAUDE.md, at the top.

## Git authorship
- Set the repo-local git identity before the first commit:
  git config user.name "Usman Siddiqi"
  git config user.email "usmansiddiqi2007@gmail.com"
- Do not add "Co-Authored-By" trailers, "Generated with" lines, session links or any other AI attribution anywhere: commits, PRs, README, code or docs. This overrides any default attribution behavior.

## What this project is

SimLoop is a deterministic simulation testing (DST) framework for Python asyncio.
User code runs on a custom event loop with simulated time, seeded scheduling,
a simulated network and seeded fault injection. The same seed must always
produce the same execution. The second half of the project runs real
open-source async libraries under SimLoop to find genuine bugs.

## Session workflow

At the start of every session:
1. Read `ROADMAP.md`, `docs/PROGRESS.md` and `docs/FINDINGS.md`.
2. Continue from "Next steps" in the progress log unless told otherwise.

At the end of every session:
1. Update `docs/PROGRESS.md` (done, in progress, known issues, next steps).
2. Tick acceptance criteria in `ROADMAP.md` that are now met.
3. Push, and give Usman a short summary: what was built, what works, what's
   next, findings so far.

## Conventions

- Python 3.11+, `src/` layout, package `simloop`. Public API is whatever
  `simloop/__init__.py` exports; everything prefixed with `_` is internal.
- Tooling: `ruff check`, `ruff format`, `mypy --strict` (configured in
  `pyproject.toml`), `pytest`. All four must pass before pushing; CI runs them
  on 3.11 to 3.14.
- Commits: small, logical, Conventional Commits (`feat:`, `fix:`, `test:`,
  `docs:`, `refactor:`, `ci:`, `chore:`). Work directly on `main` (Usman's
  call: this is the only agent on the project) and push regularly.
- Work autonomously through the roadmap; report back to Usman when a
  milestone batch is meaningfully done or when truly blocked.
- Borrowing ideas from other tools is fine. Reusing actual code only from
  MIT-compatible sources, with the attribution their license requires.
- Comments explain *why*, not *what*. No tutorial-style comments.
- Every feature lands with tests. Anything touching scheduling, time or
  randomness also needs a determinism test (same seed, same trace digest).

## Determinism rules (treat violations as bugs)

- All randomness inside the framework flows through
  `simloop._choices.ChoiceSource`, never `random` directly. This is what makes
  seed replay and shrinking work.
- Never put object ids, memory addresses, wall-clock times or hash-ordered
  iteration into a trace. Task names are assigned per simulation.
- The loop is fail-closed: unimplemented I/O raises instead of touching the
  real OS. Real sockets, threads, subprocesses and blocking sleeps inside a
  simulation are detected by `simloop._sandbox` and reported.

## Findings discipline

- Every suspected bug goes into `docs/FINDINGS.md` with library, version,
  minimal reproduction script, seed, scheduler settings and explanation.
- Only call something a confirmed bug when it reproduces, and when the
  execution it relies on is possible under real asyncio (re-check with
  `scheduler="fifo"` plus timing perturbation, or argue feasibility
  explicitly). Keep confirmed bugs, suspected issues and false positives in
  separate sections. Never exaggerate.
- README findings section lists only confirmed, reproducing results.
- Reporting upstream is Usman's call. See the hard rule above.

## README visuals

Images in `docs/assets/` must be generated from real runs by scripts in
`scripts/`, never hand-made or invented. Regenerate them when output changes.
