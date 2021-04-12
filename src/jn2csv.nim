# This is just an example to get you started. A typical binary package
# uses this file as the main entry point of the application.

import json
import tables
import sets
import sequtils
import strutils

var
  debugging = false
  lineErrors = false

proc readAllJsonLines*(f: File): seq[TaintedString] =
  # no real good way around this for stream inputs
  result = newSeq[TaintedString]()

  var line: string
  while stdin.readLine(line):
    try:
      result.add(line)
    except Exception as err:
      echo "Error reading json lines input"
      raise err

type
  Jitter* = iterator(): JsonNode {.closure.}

iterator nodes*(lines: seq[TaintedString]): JsonNode {.closure.} =
  for line in lines:
    try:
      var js: JsonNode = line.parseJson()
      yield js
    except Exception as err:
      if debugging:
        stderr.writeLine "Error parsing json lines: ", repr line
      if lineErrors:
        raise err
      else:
        continue

proc scanForKeys(lines: seq[TaintedString]): HashSet[string] =
  var keys = initHashSet[string]() 

  # stderr.writeLine "json:len: ", $lines.len()
  for jn in nodes(lines):
    if jn.kind == JObject:
      for k in jn.keys:
        var s: string = k
        keys.incl(s)
    else:
      raise newException(ValueError, "json lines must be simple json objects")
  
  result = keys


proc toCols(cols: HashSet[string], jdata: seq[string]): Table[string, seq[string]] =
  result = initTable[string, seq[string]](jdata.len())
  for col in cols:
    result[col] = @[]

  for jn in nodes(jdata):
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

