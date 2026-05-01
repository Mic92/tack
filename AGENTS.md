# Tack

- Use `go doc` to find documentation for Go packages. For example:
  `go doc github.com/mitchellh/go-libghostty`. Full syntax is
  `go doc [<pkg>.][<sym>.]<methodOrField>` for full help output.

## Style Guide

- Comment heavily, but not redundantly. Explain the "why" behind decisions
  clearly, don't repeat the "what."
- Try to limit line length below 100 characters, but don't be afraid to break
  this rule if it improves readability.
  - Do not make lines too short to follow this rule either. Try
    to use as much of the max characters as possible without sacrificing
    readability.

## Go Guide

- Add compile-time interface checks whenever a type implements an interface
  we care about. For example: `var _ io.Reader = (*MyType)(nil)`.

## Providers

- Buildkite API: <https://buildkite.com/docs/apis/rest-api.md>
