# checkBackup

Scans a large Dgraph JSON backup file and reports which `Event.value` entries are missing in the range `0..max-1`. The parser streams the top-level array to keep memory use low.

## Usage

```bash
go run . -file ../../container-data/alpha-2/backup/dgraph.r1868290.u0211.1440/g01.json/g01.json -max 100005
```

## Flags

- `-file` (required): Path to the JSON file to scan.
- `-max` (required): Maximum event value (exclusive) to check for missing values.
- `-print-missing`: Print each missing value on its own line.

## Output

The final line prints a summary:

```
50014
78941
checked=0..100004 seen=100002 missing=2
```
