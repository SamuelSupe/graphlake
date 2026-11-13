The source and license files here come from boltr 0.3.0. GraphLake uses the
local copy because this version's server backend requires all records to be
materialized before the first PULL. The server patch adds a lazy record
producer, bounded connection buffers and disconnect detection during reads.
The Bolt value types, PackStream encoding and handshake remain upstream.
