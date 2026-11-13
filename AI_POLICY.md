# AI Usage Policy

This project is built with AI assistance, and says so: much of the commit history carries a `Co-Authored-By` trailer naming the model that helped write it. AI tools are welcome here. What is not welcome is work that nobody has understood.

The policy exists because of an asymmetry. Generating a plausible-looking change now takes minutes; reviewing it still takes a person's full attention. A format library sits in the data path, where a subtle mistake means silently wrong or dropped records in someone's pipeline. Every issue and pull request here is read by a human, and a contribution that hands the work of understanding it to the reviewer costs more than it gives.

## The rules

1. **Disclose AI use.** If an AI tool produced or substantially shaped your contribution, say so in the pull request or issue: which tool, and roughly how much of the work it did ("Claude Code wrote the first draft of the converter, I rewrote the null handling and wrote the tests" is the right level of detail). For commits, a `Co-Authored-By` or `Assisted-by` trailer naming the tool is enough. Autocomplete and spell-checking need no disclosure.

2. **Understand what you submit.** You must be able to explain what your change does, why it is correct, and how it interacts with the rest of the format, without asking the tool again. If you cannot, the change is not ready. You are the author and you are accountable for it, whatever wrote the first draft.

3. **Verify it yourself.** Run it. For code, that means `make check` passes locally and the behaviour you claim is covered by a test you have read. A bug report must describe something you actually observed, with the versions and the reproduction you actually used. Do not submit a model's reading of the code as a finding: analyses that were never reproduced are the most common form of noise.

4. **Edit what the tool wrote.** Issues, pull request descriptions and review replies may be drafted with AI, but a person must review and trim them before posting. Say what matters, in your own judgment of what matters. Long, generic text that restates the diff or pads a report will be asked to be rewritten.

5. **A person submits and a person responds.** Pull requests and issues opened by autonomous agents with nobody behind them are closed without review, whether or not the change is correct. So are unsolicited pull requests from accounts submitting at scale across many unrelated projects. Review conversations must be answered by you; pasting review comments into a tool and posting its reply unread is the same problem one step later.

6. **Mind the provenance.** Do not submit generated code you have reason to believe reproduces someone else's code under an incompatible license. Contributions are accepted under the Apache License 2.0, and you are vouching that you can offer them under it.

7. **No AI-generated media.** Text and code only. Diagrams, screenshots and terminal recordings in the docs must show the real thing; the demo casts are recorded against a real Kafka, Schema Registry and Flink stack for that reason.

## How this is applied

The pull request template asks you to confirm that the change comes from a person who understands it and can discuss it in review, and to disclose AI use. Ticking that box untruthfully is a conduct problem, not a code-quality one.

A contribution that does not meet these rules is closed with a pointer to this page. That is a statement about the contribution, not about you, and a corrected resubmission is welcome. Accounts that keep submitting unreviewed generated work are blocked from the repository.

If your pull request or issue was closed under this policy and that was a misread, say so in a comment and it will be looked at again.

## Maintainers

These rules apply to maintainers as well. Maintainer commits disclose AI assistance through commit trailers and go through the same pull request checks as everyone else's.

## Credit

This policy borrows ideas from the [Ghostty AI usage policy](https://github.com/ghostty-org/ghostty/blob/main/AI_POLICY.md) and the [Hardwood contribution guide](https://github.com/hardwood-hq/hardwood/blob/main/CONTRIBUTING.md#a-note-on-ai-assisted-contributions).
