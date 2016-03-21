// Log records each change to the record in the db, in order to recover data
// iff there's something wrong with the table file or data lose of mem.
// The format of db is described as following:
// 1. each log file consists of several blocks, with each block 32KB. The only
//    exception is that the tail of the file may contains a partial block.
// 2. within each block, there's several records, with each record 4KB size
//    block := record * trailer ?
//    record :=
//       checksum: uint32  crc32c of type and data[]; little-endian
//       length  : uint16  little endian
//       type    : uint8   one of full, first, middle, last
//       data    : uint8[length]
// A record never starts within the last six bytes of a block (since it won't fint).
// Any leftover bytes here form the trailer, which must consists entirely of zero bytes
// and must be skipped by log readers.
// Aside: if exactly seven bytes are left in the current block, and a new non-zero length
// record is added, the writer must emit a FIRST record to fill up the trailing seven bytes
// and then emit all of the user data in subsequent blocks.
// FULL == 1
// FIRST == 2
// MIDDLE == 3
// LAST == 4
// 
// The FULL record contains the contents of an entire user record.
// 
// FIRST, MIDDLE, LAST are types used for user records that have been
// split into multiple fragments (typically because of block boundaries).
// FIRST is the type of the first fragment of a user record, LAST is the
// type of the last fragment of a user record, and MIDDLE is the type of
// all interior fragments of a user record.
package log