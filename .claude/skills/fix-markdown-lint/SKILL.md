---
name: "fix-markdown-lint"
description: "Fix markdown linting and prettier formatting issues in markdown files"
disable-model-invocation: true
---

# Fix Markdown Lint

Fix markdown lint errors and prettier formatting issues so CI passes.

## Usage

```text
/fix-markdown [file ...]
```

If no files are specified, auto-detect changed `.md` files:

```bash
git diff --name-only HEAD upstream/main -- '*.md'
```

## Instructions

### 1. Identify errors

Run markdownlint with the repo config to see all errors:

```bash
markdownlint-cli2 --config .github/linters/.markdownlint.json <files>
```

Read each file that has errors before fixing.

### 2. Fix errors

Apply fixes for each error type:

**MD013 — Line length** (max 120 chars, code blocks and tables exempt):

- Break long lines at sentence boundaries
- Keep related phrases together when possible
- Do not break URLs or inline code spans

**MD040 — Fenced code block language**:

- Add a language identifier after the opening triple backticks
- Use `text` for plain text, CLI output, or pseudo-code diagrams
- Use the actual language (`bash`, `go`, `json`, `yaml`, `sql`, `markdown`) when the content is code

**MD060 — Table column style**:

- Add spaces around pipes in separator rows: `| --- | --- |` not `|---|---|`
- Prettier will handle full table alignment afterward

**Other rules**:

- Read the markdownlint rule documentation if you encounter an unfamiliar rule
- Fix according to the rule's requirements

### 3. Run prettier

After fixing lint errors, run prettier to normalize formatting:

```bash
npx prettier --write <files>
```

Prettier will align tables, normalize spacing, and fix other formatting.

### 4. Verify

Re-run markdownlint to confirm all errors are resolved:

```bash
markdownlint-cli2 --config .github/linters/.markdownlint.json <files>
```

If errors remain, fix them and repeat until clean.

## Config reference

- Markdownlint config: `.github/linters/.markdownlint.json`
  - Line length: 120 (code blocks and tables exempt)
- Prettier: default config (no `.prettierrc`)
- CI runs both checks via super-linter
