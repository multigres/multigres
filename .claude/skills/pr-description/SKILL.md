---
name: "pr-description"
description: "Generate a PR title and description from the current branch diff against main"
---

# PR Description Generator

Generates a PR title and description based on the diff between the current branch and `upstream/main`, and writes it to `pr-description.md` in the repo root.

## Usage

```text
/pr-description
```

## Instructions

When this skill is invoked, follow these steps:

### 1. Gather context

Run these commands to understand the changes:

```bash
# Get the branch name
git branch --show-current

# Get the commit log for this branch (commits not on main)
git log --oneline upstream/main..HEAD

# Get the full diff against main
git diff upstream/main...HEAD
```

If the diff is large, also run `git diff --stat upstream/main...HEAD` first for an overview, then read specific files as needed.

### 2. Write the PR description

Create a file called `pr-description.md` in the repo root with this structure:

```markdown
**Title:** <type>(<scope>): <short description>

**Body:**

## Summary

<2-4 bullet points describing what changed and why>
```

Then choose the appropriate detail sections based on the nature of the PR:

**If the PR fixes a bug or addresses a non-obvious issue**, add:

```markdown
## Problem

<Brief description of the issue being solved>

## Solution

<Brief description of the approach taken>
```

**Otherwise** (pure feature, refactor, or when summary alone isn't enough), add:

```markdown
## Description

<More detailed explanation of the changes, covering the what and why>
```

If the summary already says everything, you can omit the extra section entirely.

### 3. Guidelines

- **Title** must follow [Conventional Commits](https://www.conventionalcommits.org/): `feat`, `fix`, `docs`, `test`, `refactor`, `chore`, `build`, `ci`, `perf`. Keep it under 72 characters.
- **Summary** should be concise — focus on *what* and *why*, not implementation minutiae.
- Do not pad the description with filler. Shorter is better.
