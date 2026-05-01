# Known Issues

A list of known issues we should address one day.

* We don't disconnect from knots when they remove us as a spindle.
  This originates in an upstream issue (<https://tangled.org/tangled.org/core/pulls/1401>).
  The workaround right now is to periodically restart tack.

* We never prune the events table. Realistically our load here is very
  small and is fully controlled by repos that add us as a spindle,
  but we should still consider it one day.
