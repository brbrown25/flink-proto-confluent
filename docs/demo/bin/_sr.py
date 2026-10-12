#!/usr/bin/env python3
"""Tiny Schema Registry client for the demo casts: _sr.py register|check|compat SUBJECT [LEVEL]. Schema comes from $SCHEMA.
check takes --all-versions before SUBJECT to test against every registered version (the registry only checks the latest)."""
import json, os, re, subprocess, sys

def call(method, path, body):
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "schema-registry", "curl", "-s", "-X", method,
         "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "-d", "@-",
         "http://localhost:8081" + path],
        input=json.dumps(body), capture_output=True, text=True).stdout
    return json.loads(out)

all_versions = "--all-versions" in sys.argv
args = [a for a in sys.argv[1:] if a != "--all-versions"]
cmd, subject = args[0], args[1]
if cmd == "compat":
    call("PUT", "/config/" + subject, {"compatibility": args[2]})
    print("compatibility %s = %s" % (subject, args[2]))
else:
    body = {"schemaType": "PROTOBUF", "schema": os.environ["SCHEMA"]}
    if cmd == "register":
        r = call("POST", "/subjects/%s/versions" % subject, body)
        if "id" in r:
            print("registered: schema id %d" % r["id"])
        else:
            m = re.search(r'errorType:"(\w+)", description:"([^"]*)"', r["message"])
            print("REJECTED (HTTP %s): %s" % (r["error_code"], r["message"].split(",")[0]))
            if m:
                print("  %s: %s" % m.groups())
    elif all_versions:
        versions = json.loads(subprocess.run(
            ["docker", "compose", "exec", "-T", "schema-registry", "curl", "-s",
             "http://localhost:8081/subjects/%s/versions" % subject],
            capture_output=True, text=True).stdout)
        ok = True
        for v in versions:
            r = call("POST", "/compatibility/subjects/%s/versions/%d" % (subject, v), body)
            ok = ok and r.get("is_compatible") is True
            print("  vs v%d: is_compatible: %s" % (v, str(r.get("is_compatible", r)).lower()))
        print("is_compatible (all versions): %s" % str(ok).lower())
    else:
        r = call("POST", "/compatibility/subjects/%s/versions/latest?verbose=true" % subject, body)
        print("is_compatible: %s" % str(r.get("is_compatible", r)).lower())
