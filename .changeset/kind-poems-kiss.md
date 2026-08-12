---
'multitars': patch
---

Fix tar input's header being concurrently readable with a cancellation padding skip. This meant that partially reading a file may have caused issues and misalignment on the next file access
