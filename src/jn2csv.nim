# This is just an example to get you started. A typical binary package
# uses this file as the main entry point of the application.

import json
import tables
import sets
import sequtils
import strutils

let
  debugging = false
  lineErrors = false
  skipLines = 0

proc readAllJsonLines*(f: File): seq[JsonNode] =
  # no real good way around this for stream inputs
  result = newSeq[JsonNode]()

  var line: string
  while stdin.readLine(line):
    try:
      result.add(line.parseJson())
    except Exception as err:
      # echo "Error reading json lines input"
      # raise err
      if debugging:
        stderr.writeLine "Error parsing json lines: ", repr line
      if lineErrors:
        raise err
      else:
        continue

type
  Jitter* = iterator(): JsonNode {.closure.}

proc scanForKeys(lines: seq[JsonNode]): HashSet[string] =
  var keys = initHashSet[string]() 

  # stderr.writeLine "json:len: ", $lines.len()
  # for jn in nodes(lines):
  for jn in lines:
    if jn.kind == JObject:
      for k in jn.keys:
        var s: string = k
        keys.incl(s)
    else:
      raise newException(ValueError, "json lines must be simple json objects")
  
  result = keys


proc toCols(cols: HashSet[string], jdata: seq[JsonNode]): Table[string, seq[string]] =
  result = initTable[string, seq[string]](jdata.len())
  for col in cols:
    result[col] = @[]

  for jn in jdata:
    for colName in cols:
      let
        val = jn[colName]
        # valStr: string = if val == nil: "" else: val.getStr()
        valStr: string = $val
      result[colName].add(valStr)

proc print(headers: seq[string], columns: seq[seq[string]]) =
  # Headers
  echo headers.join(",")

  if headers.len() == 0:
    return

  let
    colLens = columns.mapIt(it.len())
    rowCount = colLens.max()

  assert rowCount == colLens.min()

  # Columns
  for row in 0..<rowCount:
    stdout.writeLine(columns.mapIt(it[row]).join(","))

proc execIoStream*(fl: File) =

  let
    jdata = fl.readAllJsonLines()
    jkeys = jdata.scanForKeys()

  # stderr.writeLine "jn:jkeys: ", $jkeys

  let
    columnMaps = toCols(jkeys, jdata)
    headers: seq[string] = jkeys.mapIt(it)
    columns = headers.mapIt(columnMaps[it])

  # stderr.writeLine "jn:columns: ", $columns.len()
  # for c, cd in columnMaps.pairs():
    # stderr.writeLine "jn:columns: ", c, ": ", $cd.len()

  print(headers, columns)


when isMainModule:
  execIoStream(stdin)

