#!/usr/bin/env python3
"""Tiny Schema Registry client for the demo casts: _sr.py register|check|compat SUBJECT [LEVEL]. Schema comes from $SCHEMA."""
import json, os, re, subprocess, sys

def call(method, path, body):
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "schema-registry", "curl", "-s", "-X", method,
         "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "-d", "@-",
         "http://localhost:8081" + path],
        input=json.dumps(body), capture_output=True, text=True).stdout
    return json.loads(out)

cmd, subject = sys.argv[1], sys.argv[2]
if cmd == "compat":
    call("PUT", "/config/" + subject, {"compatibility": sys.argv[3]})
    print("compatibility %s = %s" % (subject, sys.argv[3]))
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
    else:
        r = call("POST", "/compatibility/subjects/%s/versions/latest?verbose=true" % subject, body)
        print("is_compatible: %s" % str(r.get("is_compatible", r)).lower())
