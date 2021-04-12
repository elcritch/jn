# This is just an example to get you started. A typical binary package
# uses this file as the main entry point of the application.

import json
import tables
import sets

var debugging = false

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

# iterator jsonLines*(lines: seq[TaintedString]): JsonNode {.closure.} =
proc jsonLines*(lines: seq[TaintedString]): Jitter =
  iterator jsonLines(): JsonNode {.closure.} =
    for line in lines:
      try:
        var js: JsonNode = line.parseJson()
        yield js
      except Exception as err:
        if debugging == true:
          stderr.writeLine "Error parsing json lines: ", repr line
        # raise err
  return jsonLines

proc scanForKeys(jlines: Jitter): HashSet[string] =
  var keys = initHashSet[string]() 

  stderr.writeLine "keys:len: ", $keys
  for jl in jlines():
    if jl.kind == JObject:
      for k in jl.keys:
        var s: string = k
        keys.incl(s)
    else:
      raise newException(ValueError, "json lines must be simple json objects")
  
  result = keys


proc execIoStream(fl: File) =

  let
    jdata = fl.readAllJsonLines()
    jiter: Jitter = jdata.jsonLines
    jkeys = jiter.scanForKeys()

  echo "jkeys: ", $jkeys


when isMainModule:
  echo("jn2csv")
  execIoStream(stdin)

