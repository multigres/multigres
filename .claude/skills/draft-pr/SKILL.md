---
name: "draft-pr"
description: "Generate a PR title and description from the current branch diff against main"
disable-model-invocation: true
---

# PR Description Generator

Generates a PR title and description based on the diff between the current branch and `upstream/main`, and creates a draft PR.

## Usage

```text
/draft-pr
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

### 2. Generate the PR title and body

Compose the title and body following this structure:

**Title:** `<type>(<scope>): <short description>`

**Body:**

```markdown
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

### 3. Create a draft PR

Push the branch to the remote if not already pushed, then create a draft PR:

```bash
# Push the branch (set upstream if needed)
git push -u origin HEAD

# Create draft PR using the generated title and body
gh pr create --draft --title "<title>" --body "<body>"
```

Use a HEREDOC for the body to preserve formatting:

```bash
gh pr create --draft --title "<title>" --body "$(cat <<'EOF'
<body content here>
EOF
)"
```

Print the PR URL when done so the user can review it.

### 4. Guidelines

- **Title** must follow [Conventional Commits](https://www.conventionalcommits.org/): `feat`, `fix`, `docs`, `test`, `refactor`, `chore`, `build`, `ci`, `perf`. Keep it under 72 characters.
- **Summary** should be concise — focus on _what_ and _why_, not implementation minutiae.
- Do not pad the description with filler. Shorter is better.
- **Do not hard-wrap paragraphs or bullet bodies in the PR body.** GitHub's PR/issue Markdown treats single newlines inside a paragraph as soft line breaks (`<br>`), so wrapping at ~70–80 chars produces visible broken-up sentences in the rendered PR.
- Keep each paragraph or bullet on one long line; only insert a newline to start a new paragraph, bullet, heading, or code block. Code fences and lists are fine — the rule applies to prose.
