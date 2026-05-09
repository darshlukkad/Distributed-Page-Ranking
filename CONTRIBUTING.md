# Contributing Guidelines

Standards and policies for all 13 team members. Read this before writing a single line of code.

---

## Table of Contents

1. [Git Workflow](#git-workflow)
2. [Branching Strategy](#branching-strategy)
3. [Commit Messages](#commit-messages)
4. [Pull Requests](#pull-requests)
5. [C++ Coding Standards](#c-coding-standards)
6. [Python Coding Standards](#python-coding-standards)
7. [Testing Standards](#testing-standards)
8. [Documentation Standards](#documentation-standards)
9. [Code Review Checklist](#code-review-checklist)
10. [Communication Standards](#communication-standards)

---

## Git Workflow

### The Golden Rules

- **Never commit directly to `main`**. Ever. Not even a one-line fix.
- **Never force push** to any shared branch.
- **Every change goes through a Pull Request** with at least one reviewer.
- **`main` must always build and run.** If you break it, fixing it is your top priority.

### Branch Hierarchy

```
main          ← production, always stable, protected
  └── develop ← integration branch, all groups merge here first
        ├── group-a/sequential-pagerank
        ├── group-a/partition-loader
        ├── group-b/socket-utils
        ├── group-b/message-framing
        ├── group-b/coordinator
        ├── group-c/preprocessor
        └── group-d/build-system
```

`develop` merges into `main` only at phase milestones (end of Phase 1, 2, 3).

---

## Branching Strategy

### Creating a branch

Always branch off `develop`, not `main`:

```bash
git checkout develop
git pull origin develop
git checkout -b group-a/partition-loader
```

### Branch naming convention

```
group-{a|b|c|d}/{short-description}

Examples:
  group-a/sequential-pagerank
  group-b/tcp-framing
  group-b/coordinator-registration
  group-c/preprocessor
  group-d/cmake-setup
  group-d/experiment-scaling
```

Use hyphens, all lowercase, no spaces. Keep it under 40 characters.

### Keeping your branch up to date

Rebase onto `develop` regularly — at least once a day — to avoid painful merge conflicts:

```bash
git fetch origin
git rebase origin/develop
```

If rebase conflicts, resolve them file by file. Ask for help rather than guessing.

---

## Commit Messages

### Format

```
<type>: <short summary in present tense, under 72 chars>

<optional body — explain WHY, not what. Reference issue numbers.>
```

### Types

| Type | When to use |
|---|---|
| `feat` | New functionality |
| `fix` | Bug fix |
| `test` | Adding or fixing tests |
| `docs` | Documentation only |
| `refactor` | Code restructure, no behavior change |
| `build` | CMake, Makefile, dependencies |
| `chore` | Config, .gitignore, tooling |

### Examples

```bash
# Good
feat: implement recv_exact with partial-read loop
fix: handle dangling vertices with zero out-degree
test: validate CSR edge offsets on 5-vertex synthetic graph
docs: publish binary partition file format spec

# Bad — too vague
fix: bug
update code
stuff
```

### Rules

- Use present tense: "add feature" not "added feature"
- No period at the end of the subject line
- Reference GitHub issues: `Closes #14` or `Part of #8`
- No "WIP" commits on shared branches — squash them before opening a PR

---

## Pull Requests

### Before opening a PR

- [ ] Branch is rebased on latest `develop`
- [ ] Code builds without warnings: `make all 2>&1 | grep -i warning`
- [ ] Relevant tests pass
- [ ] No debug `printf` / `cout` left in the code
- [ ] Self-reviewed your own diff

### PR title format

Same as commit message format:

```
feat: add TCP peer mesh setup with i<j convention
fix: correct edge reversal in preprocessor
test: end-to-end correctness on 10M-edge subgraph
```

### PR description template

```markdown
## What this does
One sentence.

## Why
Link to the issue or explain the motivation.

## Testing done
- [ ] Unit test X passes
- [ ] Tested on localhost with N=4 processes
- [ ] Validated against reference output

## Notes for reviewer
Anything non-obvious the reviewer should know.

Closes #<issue-number>
```

### Review requirements

- Minimum **1 approval** before merging
- Reviewer must be from a **different group** than the author
- Author merges their own PR after approval — do not merge someone else's PR without their knowledge
- Use **Squash and Merge** to keep `develop` history clean

### Review turnaround

Respond to review requests within **4 hours** during the sprint. This is a 7-day project — slow reviews kill momentum.

---

## C++ Coding Standards

### Compiler and standard

```cmake
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
```

Build with warnings enabled:

```cmake
add_compile_options(-Wall -Wextra -Wpedantic)
```

Fix all warnings before opening a PR. Warnings become bugs.

### Naming conventions

| Element | Convention | Example |
|---|---|---|
| Files | `snake_case` | `socket_utils.cpp` |
| Classes / structs | `PascalCase` | `WorkerInfo`, `Partition` |
| Functions | `snake_case` | `tcp_connect()`, `recv_message()` |
| Variables | `snake_case` | `num_workers`, `local_delta` |
| Constants / enums | `UPPER_SNAKE_CASE` | `MAX_WORKERS`, `MSG_CONTRIBS` |
| Member variables | `snake_case` (no prefix) | `worker_id`, `edge_count` |

### Header files

Use `#pragma once` at the top of every header — not `#ifndef` guards:

```cpp
#pragma once
#include <cstdint>
```

### Integer types

Use explicit-width types for anything that crosses a wire or goes to disk:

```cpp
// Good
uint32_t worker_id;
uint64_t num_edges;
double   rank;

// Bad — size is platform-dependent
int worker_id;
long num_edges;
float rank;   // use double for PageRank — float loses precision
```

### Memory management

Prefer `std::vector` over raw arrays. No `new`/`delete` unless you have a specific reason and document it.

```cpp
// Good
std::vector<uint32_t> edges(num_local_edges);

// Avoid
uint32_t* edges = new uint32_t[num_local_edges];
```

### Error handling

On socket errors, print the error and exit — do not silently continue:

```cpp
int fd = ::connect(sock, addr, addrlen);
if (fd < 0) {
    perror("connect failed");
    exit(1);
}
```

This is a research system with no fault tolerance. Crashing loudly is correct behavior.

### Comments

Write comments only when the **why** is non-obvious. Do not narrate what the code does:

```cpp
// Good — explains a non-obvious constraint
// i<j convention: worker i initiates connection to j only if i<j.
// Prevents both sides connecting simultaneously and deadlocking.
if (my_id < peer_id) tcp_connect(...);

// Bad — narrates the obvious
// Loop over all local vertices
for (uint64_t i = 0; i < num_local_vertices; i++) {
```

### Formatting

Run `clang-format` before committing. Use the project `.clang-format` file (to be added to repo root). If in doubt, 4-space indentation, opening braces on the same line.

---

## Python Coding Standards

### Version

Python 3.8+. Do not use features from 3.10+ (match/case, etc.) — not all team machines may be up to date.

### Naming conventions

| Element | Convention |
|---|---|
| Files | `snake_case.py` |
| Functions | `snake_case` |
| Constants | `UPPER_SNAKE_CASE` |
| Classes | `PascalCase` |

### Type hints

Add type hints to all function signatures:

```python
# Good
def write_partition(worker_id: int, edges: list[tuple[int, int]], output_dir: str) -> None:

# Acceptable for short scripts
def write_partition(worker_id, edges, output_dir):
```

### No magic numbers

```python
# Bad
if len(header) != 32:

# Good
HEADER_SIZE_BYTES = 32
if len(header) != HEADER_SIZE_BYTES:
```

### Progress output

Any script that runs for more than 10 seconds must print progress:

```python
if edge_count % 100_000_000 == 0:
    print(f"  processed {edge_count:,} edges...", flush=True)
```

---

## Testing Standards

### Every deliverable needs a test

No PR merges a new function without at least one test that would catch a wrong implementation.

### Test file naming

```
tests/
  test_partition_loader.cpp
  test_message_framing.cpp
  test_preprocessor.py
  test_pagerank_formula.cpp
```

### Minimum tests required per component

| Component | Required tests |
|---|---|
| Preprocessor | Edge reversal, partition assignment, CSR correctness, dangling vertex |
| Message framing | Round-trip for every message type, partial-read resilience |
| Partition loader | Header validation, edge count matches, magic number check |
| PageRank formula | Single iteration on 3-vertex graph with known answer |
| Convergence | Known graph converges in expected iterations, rank sum ≈ 1.0 |

### Correctness is non-negotiable

Before any distributed run is considered valid, it must pass:

```bash
# Distributed result vs sequential reference
python3 scripts/validate.py \
    --sequential sequential_output.txt \
    --distributed result_worker_*.txt \
    --tolerance 1e-6
```

L1 difference must be below `1e-6`. If it is not, the result is wrong — do not present it.

---

## Documentation Standards

### What must be documented

- Every public function in a header file gets a one-line doc comment
- Every message type in `protocol.md` must have a worked byte-sequence example
- Every binary format field in `format.md` must have its offset, size, type, and description

### What must NOT be in docs

- Obvious things the code already says
- References to "the current task" or "this PR" — docs outlive the task
- Claude / AI tool attribution

### Keeping docs current

If you change an interface (add a field to a message, change a function signature), update the corresponding doc in the same PR. A PR that changes `message.h` without updating `protocol.md` will not be approved.

---

## Code Review Checklist

When reviewing someone else's PR, check:

**Correctness**
- [ ] Does the logic match the spec in `format.md` or `protocol.md`?
- [ ] Are integer types correct (`uint32_t` vs `uint64_t`)?
- [ ] Is little-endian byte order used consistently?
- [ ] Are edge cases handled (zero edges, single worker, N=1)?

**Safety**
- [ ] No buffer overflows (check array bounds)
- [ ] `recv_exact` / `send_exact` — never partial reads/writes
- [ ] `SIGPIPE` handled (`signal(SIGPIPE, SIG_IGN)` in main)
- [ ] No `new` without matching `delete` (prefer `vector`)

**Clarity**
- [ ] Function names describe what they do
- [ ] No magic numbers — named constants instead
- [ ] Comments explain why, not what

**Tests**
- [ ] At least one test for the new functionality
- [ ] Test would catch a wrong implementation (not just a happy path)

---

## Communication Standards

### Daily standup (async, every morning by 9am)

Post in the group chat:

```
Done: [what you finished yesterday]
Today: [what you are working on today]
Blocked: [anything stopping you — tag the person who can unblock]
```

If you are blocked, say so immediately — do not sit on it for a day.

### Decisions that affect other groups

Any change to `format.md` or `protocol.md` requires **group lead approval from every affected group** before merging. These are contracts. Changing them unilaterally breaks other people's code.

### Definition of Done

A task is **done** when:
1. Code is written
2. Tests pass
3. PR is merged to `develop`
4. GitHub issue is closed

"It works on my machine" is not done.

---

## Quick Reference

```
Branch off:        develop
Branch name:       group-{a|b|c|d}/description
Commit format:     feat|fix|test|docs|refactor: summary
PR requires:       1 approval, from different group
Merge strategy:    Squash and Merge
Integer types:     uint32_t / uint64_t / double — never int/long/float
Endianness:        little-endian everywhere
SIGPIPE:           signal(SIGPIPE, SIG_IGN) in every main()
Tests:             required for every new function
Done means:        merged to develop + issue closed
```
