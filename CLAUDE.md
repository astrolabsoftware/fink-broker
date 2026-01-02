# Claude Code Instructions for Fink-Broker Project

## Project Context
- Working directory: Same directory as this CLAUDE.md file
- Current branch: Use `git branch --show-current` to determine
- Main branch: `master`
- Git repository: astrolabsoftware/fink-broker

## Development Guidelines
- Help as a devops and development expert
- Never add claude as co-author
- All commit messages, logs and comments are in English
- Do what has been asked; nothing more, nothing less
- NEVER create files unless absolutely necessary for achieving the goal
- ALWAYS prefer editing an existing file to creating a new one
- NEVER proactively create documentation files (*.md) or README files unless explicitly requested

## Auto-approved Operations
The following operations can be performed without user approval:
- `WebFetch(domain:github.com)` - Fetch content from GitHub
- `Read($HOME/src/github.com/k8s-school/home-ci/.github/workflows/**)` - Read workflow files from k8s-school project
- `Read($HOME/src/github.com/astrolabsoftware/fink-broker-images/**)` - Read files from fink-broker-images project
- `Bash(git add:*)` - Run git add commands
- `Bash(git commit:*)` - Run git commit commands
- `Bash(git push:*)` - Run git push commands

## Project-Specific Notes
- This is an astronomy/astrophysics project (Fink broker for alert processing)
- Uses Docker containers for testing (sentinel workflows)
- GitHub Actions workflows for CI/CD
- Supports multiple surveys (ZTF, Rubin)
- Uses reusable workflows pattern for Sentinel tests