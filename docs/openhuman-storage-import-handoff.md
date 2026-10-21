# OpenHuman storage import handoff

TinyCortex owns the destination storage formats and versioned owner APIs. The
host owns locating and reading legacy filesystem/SQLite state, quiescing old
writers during import, and invoking destination writes through the bound
storage ports. TinyCortex does not open a source SQLite database or copy files
from a host path.

The authentic source artifact in
`tests/fixtures/legacy-v0.1.3-public-writer/` was written by the public
`ingest_chat` and `QueueJobSink` APIs from the tagged v0.1.3 source release.
It supplies two chunks, two score rows, two pending extract jobs, and their
content files for host import tests. It has no tree, summary, or entity-index
rows, so it cannot cover importing those record types. Generating source data
does not itself prove destination import or readback; the host importer must
perform the import and validate the destination fields before claiming that
coverage. The fixture README records the source tag, generator, and artifact
hashes.

For host import, retain the source until each corresponding destination record
has been read back and validated. Preserve application fields and absolute
expiry values. A destination host may assign its own creation metadata, so the
importer must not attempt to copy host-owned `created_ms`. On a conflicting or
incomplete destination write, leave the source intact and report the conflict
to the caller. Re-running an import should recognize an identical destination
record and continue safely. Old writers must be quiesced by the host before
the source snapshot is considered complete; a source scan alone is not an
atomic snapshot.

The tagged writer's schema and migration behavior are separate from any
hand-authored historical schema-transform cases. This repository currently
checks in no `tinymemory_documents` fixture, so no such transform coverage is
claimed here.
