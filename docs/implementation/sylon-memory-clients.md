# Private Sylon memory through the SDK and CLI

The workspace, Python SDK and CLI use the same private memory API. An administrator must enable memory on an approved Sylon binding. Memory is explicitly saved from a successful owned answer; it is scoped to your exact identity, workspace and assistant profile. Its original data permissions remain required. See [the backend contract](sylon-memory-contract.md) for bounds, retention and authorization.

Saving, correcting, deleting and recalling memory do not invoke a model or execute a notebook. Recall returns candidates for review; including an exact memory revision in a later Sylon message is a separate transfer to the approved model endpoint.

## Save and recover an explicit intent

Prepare a JSON file containing `idempotencyKey`, `source: {jobId, revision}`, `title`, `content` and `expiresAt`. Use the exact successful job revision, a reviewed title of at most 200 UTF-8 bytes, a summary of at most 4,096 UTF-8 bytes, and an explicit RFC3339 expiry within the binding's permitted lifetime. Generate and persist the key once before submission. Protect this file: it contains private memory text.

```sh
dataslate sylon memory save --workspace team --file reviewed-memory.json
dataslate sylon memory list --workspace team
dataslate sylon memory get --workspace team --id MEMORY_ID
```

All mutation responses contain an immutable `receipt`, a `replayed` flag and a current management summary. They do not contain the memory body. If the connection fails or a receipt is invalid, retain the original file and replay the identical request. The CLI does not retry mutations automatically, regenerate keys, substitute newer revisions or reread a source to change your intent. A replay may return a minimal restricted summary if access has since changed; the receipt is not authority to read old content.

## Review recall before sending

```sh
dataslate sylon memory recall --workspace team --binding assistant --query "reporting timezone" --limit 4
```

Recall is bounded lexical matching over currently authorized private memories. It has no embedding/model charge and makes no claim of semantic-memory retrieval. General notebook/catalog hybrid search remains a separate capability. Review the returned text, expiry and source before including up to four exact selections in a message file:

```json
{
  "idempotencyKey": "persist-once-before-send",
  "prompt": "Explain the reporting convention using these reviewed memories.",
  "memories": [{"id": "MEMORY_ID", "revision": 1}]
}
```

```sh
dataslate sylon send --workspace team --conversation CONVERSATION_ID --file message.json
```

Replace the illustrative IDs with actual resource IDs. A correction or expiry between selection and dispatch must fail; clients never silently choose a replacement revision. Context and derived history remain subject to the original sources' current permissions even when an answer does not cite them.

## Correct or forget

A correction file contains exactly `idempotencyKey`, expected `revision`, `title`, `content` and `expiresAt`. A deletion file contains exactly `idempotencyKey` and expected `revision`.

```sh
dataslate sylon memory correct --workspace team --id MEMORY_ID --file correction.json
dataslate sylon memory delete --workspace team --id MEMORY_ID --file deletion.json
```

Correction, deletion and expiry block history derived from the old revision. Start a new conversation with newly reviewed context. The API tracks local purge completion separately; it cannot erase information already transferred to a model provider or downloaded elsewhere. Deleting a source conversation also invalidates its saved memories. A restricted memory can still be deleted by its owner with current workspace membership; its previous title, body and source are hidden.

The equivalent Python methods are `save_sylon_memory`, `sylon_memories`, `sylon_memory`, `recall_sylon_memories`, `correct_sylon_memory`, and `delete_sylon_memory`. `send_sylon_message(..., memories=[{"id": memory_id, "revision": revision}])` sends only exact selections; the server resolves and authorizes their contents.
