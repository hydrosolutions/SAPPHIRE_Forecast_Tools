## `sapphire-dg-client` writes to a server-supplied filename without validating it (PREPG-018)

**Status**: Draft (2026-08-20)
**Module**: the **`sapphire-dg-client`** dependency (separate hydrosolutions repo), consumed by
`apps/preprocessing_gateway`
**Priority**: **Low** — see § How much does this actually matter, which argues *against* inflating
it. Filed for completeness and because the fix is three lines, not because an incident is expected.
**Labels**: `preprocessing_gateway`, `dependency`, `security`, `upstream`
**Found**: 2026-08-20, by the out-of-loop review of **PREPG-014**.
**Related**: **PREPG-014** — same file, same method, same relock. Almost certainly the same change.

---

## The defect

`_call_api_and_save_file` takes both the download URL **and the destination filename** from the
server's JSON, and `_save_file` concatenates that filename straight into a path:

```python
for file_resp in resp.json():
    resp = requests.get(file_resp.get("link"))                  # client_base.py:43
    local_file_path = self._save_file(resp, directory, file_resp["filename"])

@staticmethod
def _save_file(response, directory: str, filename: str):
    file = f"{directory}/{filename}"                            # client_base.py:27
    with open(file, "wb") as f:
        f.write(response.content)
```

A `filename` containing `../` escapes `directory`; an absolute path replaces it entirely. Nothing
normalises, rejects, or confines the result.

## How much does this actually matter — read before assigning a severity

**Do not file this as a critical remote-write vulnerability. It is not one, on the evidence
available.** The counter-arguments are real:

- The Data Gateway is **first-party hydrosolutions infrastructure**, authenticated with an API key.
  It is not an untrusted internet endpoint.
- **Observed filenames are plain basenames.** Checked against the live gateway 2026-08-20:
  `ECMWFIFS_<date>_ENS<n>_HRU<code>_tp.csv` and `..._2t.csv` — `basename(f) == f`, no separators,
  not absolute. So nothing malformed is being served today; this is a guard against a shape the
  server does not currently produce.
- Reaching it requires the gateway itself to be compromised or defective — at which point it is
  already serving the forecast data we then run models on, which is the larger problem.

**What is nonetheless true, and why it is worth three lines of code:** this is unvalidated remote
input used directly to construct a filesystem write path, and the process runs with whatever rights
the operator has. The trust assumption is undocumented and unenforced, so it survives only as long
as nobody points the client at a different host — and `PUBLIC_BULLETIN_BASE_URL`-style host
configurability already exists elsewhere in this codebase.

## The fix

In `_save_file`, confine the write to `directory`:

- take `os.path.basename(filename)` and reject empty results;
- resolve the joined path and assert it is inside the resolved `directory`;
- fail with a clear error rather than silently sanitising, so a malformed response is visible.

**Do it in the same change as PREPG-014.** Same file, same method, same upstream repo, same
relock — splitting them means two review cycles and two relocks for ~15 lines total.

## Acceptance criteria

- A response whose `filename` is `../escape.csv`, `/absolute/path.csv`, or empty does **not**
  create a file outside the target directory, and raises rather than silently writing elsewhere.
- An ordinary filename still writes exactly where it does today — pin one real DG-shaped name so
  the guard cannot pass by rejecting everything.
- Tested upstream in the client's own repo, and by the isolated downstream contract test
  PREPG-014 introduces.

## Contract not to break

- The **returned path** is used by callers (`files_downloaded` feeds the ensemble merge), so
  `_save_file` must keep returning the path it wrote.
- Do not change behaviour for well-formed filenames — this is a guard, not a renaming scheme.
