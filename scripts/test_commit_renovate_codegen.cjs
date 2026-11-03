const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const { readGenerated, commitGenerated } = require("./commit_renovate_codegen.cjs");

function fixture(t) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "renovate-codegen-"));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  for (const name of ["settingsproto.go", "settingsschema.go", "go.work.sum"]) {
    fs.writeFileSync(path.join(directory, name), "generated\n");
  }
  const env = {
    GENERATED_DIRECTORY: directory,
    REPOSITORY: "StackVista/sts-opentelemetry-collector",
    DEFAULT_BRANCH: "main",
    PR_NUMBER: "252",
    PR_BRANCH: "renovate/go-example",
    HEAD_SHA: "a".repeat(40),
  };
  const pr = {
    state: "open",
    user: { login: "renovate-update-app[bot]" },
    base: { ref: "main" },
    head: { repo: { full_name: env.REPOSITORY }, ref: env.PR_BRANCH, sha: env.HEAD_SHA },
  };
  return { directory, env, pr };
}

test("artifact accepts only the three regular files within the size limit", (t) => {
  const { directory } = fixture(t);
  assert.equal(readGenerated(directory).length, 3);
  const extra = path.join(directory, "workflow.yml");
  fs.writeFileSync(extra, "untrusted");
  assert.throws(() => readGenerated(directory), /Unexpected artifact/);
  fs.unlinkSync(extra);
  const filename = path.join(directory, "settingsproto.go");
  fs.unlinkSync(filename);
  fs.symlinkSync("go.work.sum", filename);
  assert.throws(() => readGenerated(directory), /regular file/);
  fs.unlinkSync(filename);
  fs.mkdirSync(filename);
  assert.throws(() => readGenerated(directory), /regular file/);
  fs.rmdirSync(filename);
  fs.writeFileSync(filename, Buffer.alloc(1024 * 1024 + 1));
  assert.throws(() => readGenerated(directory), /regular file/);
  fs.writeFileSync(filename, "");
  assert.throws(() => readGenerated(directory), /regular file/);
  fs.unlinkSync(filename);
  assert.throws(() => readGenerated(directory), /Unexpected artifact/);
});

test("closed, moved, fork, human, wrong-base and wrong-branch PRs cannot receive commits", async (t) => {
  const { env, pr } = fixture(t);
  const candidates = [
    { ...pr, state: "closed" },
    { ...pr, head: { ...pr.head, sha: "b".repeat(40) } },
    { ...pr, head: { ...pr.head, repo: { full_name: "other/repo" } } },
    { ...pr, user: { login: "human" } },
    { ...pr, base: { ref: "other" } },
    { ...pr, head: { ...pr.head, ref: "other-branch" } },
  ];
  for (const candidate of candidates) {
    let requests = 0;
    await commitGenerated(env, async () => { requests++; return candidate; });
    assert.equal(requests, 1);
  }
});

test("commit includes only changed allowlisted files and guards the expected head", async (t) => {
  const { env, pr } = fixture(t);
  let mutation;
  const request = async (endpoint, body) => {
    if (endpoint.includes("/pulls/")) return pr;
    if (endpoint.includes("/contents/")) {
      assert(endpoint.endsWith(`?ref=${env.HEAD_SHA}`));
      return { type: "file", encoding: "base64", content: Buffer.from(
        endpoint.includes("go.work.sum") ? "old\n" : "generated\n"
      ).toString("base64") };
    }
    assert.equal(endpoint, "/graphql");
    mutation = body;
    return { data: { createCommitOnBranch: { commit: { oid: "c".repeat(40) } } } };
  };
  await commitGenerated(env, request);
  const input = mutation.variables.input;
  assert.deepEqual(input.branch, { repositoryNameWithOwner: env.REPOSITORY, branchName: env.PR_BRANCH });
  assert.equal(input.expectedHeadOid, env.HEAD_SHA);
  assert.deepEqual(input.fileChanges.additions, [{ path: "go.work.sum", contents: Buffer.from("generated\n").toString("base64") }]);
});

test("unchanged output is a no-op and API or head-race failures fail closed", async (t) => {
  const { env, pr } = fixture(t);
  const source = { type: "file", encoding: "base64", content: Buffer.from("generated\n").toString("base64") };
  await commitGenerated(env, async (endpoint) => {
    assert.notEqual(endpoint, "/graphql");
    return endpoint.includes("/pulls/") ? pr : source;
  });
  await assert.rejects(commitGenerated(env, async () => { throw new Error("API failure"); }), /API failure/);
  await assert.rejects(commitGenerated(env, async (endpoint) => {
    if (endpoint.includes("/pulls/")) return pr;
    if (endpoint.includes("/contents/")) return { ...source, content: "b2xk" };
    return { errors: [{ message: "head moved" }] };
  }), /GitHub rejected/);
});
