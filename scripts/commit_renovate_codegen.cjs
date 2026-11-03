const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const files = {
  "settingsproto.go": "extension/settingsproviderextension/generated/settingsproto/model.gen.go",
  "settingsschema.go": "extension/settingsproviderextension/generated/settingsschema/model.gen.go",
  "go.work.sum": "go.work.sum",
};

function readGenerated(directory) {
  assert(fs.lstatSync(directory).isDirectory(), "Expected an artifact directory");
  assert.deepEqual(fs.readdirSync(directory).sort(), Object.keys(files).sort(), "Unexpected artifact files");
  return Object.entries(files).map(([name, destination]) => {
    const filename = path.join(directory, name);
    const stat = fs.lstatSync(filename);
    assert(stat.isFile() && stat.size > 0 && stat.size <= 1024 * 1024, "Expected a nonempty regular file up to 1 MiB");
    return { path: destination, contents: fs.readFileSync(filename).toString("base64") };
  });
}

function matchesPullRequest(pr, env) {
  return pr.state === "open" &&
    pr.user.login === "renovate-update-app[bot]" &&
    pr.head.repo?.full_name === env.REPOSITORY &&
    pr.base.ref === env.DEFAULT_BRANCH &&
    pr.head.ref.startsWith("renovate/") &&
    pr.head.ref === env.PR_BRANCH &&
    pr.head.sha === env.HEAD_SHA;
}

async function commitGenerated(env, request) {
  assert(/^[\w.-]+\/[\w.-]+$/.test(env.REPOSITORY), "Invalid repository");
  assert(/^[1-9]\d*$/.test(env.PR_NUMBER), "Invalid PR number");
  assert(/^[a-f0-9]{40}$/.test(env.HEAD_SHA), "Invalid head SHA");
  const additions = readGenerated(env.GENERATED_DIRECTORY);
  const pr = await request(`/repos/${env.REPOSITORY}/pulls/${env.PR_NUMBER}`);
  if (!matchesPullRequest(pr, env)) {
    console.log("PR moved or closed; leave it for the next Renovate run");
    return;
  }
  const changed = [];
  for (const file of additions) {
    const previous = await request(`/repos/${env.REPOSITORY}/contents/${file.path}?ref=${env.HEAD_SHA}`);
    assert(previous.type === "file" && previous.encoding === "base64", "Expected an existing regular source file");
    if (Buffer.from(previous.content, "base64").toString("base64") !== file.contents) changed.push(file);
  }
  if (!changed.length) {
    console.log("Generated files are already current");
    return;
  }
  const result = await request("/graphql", {
    query: `mutation($input: CreateCommitOnBranchInput!) {
      createCommitOnBranch(input: $input) { commit { oid } }
    }`,
    variables: {
      input: {
        branch: { repositoryNameWithOwner: env.REPOSITORY, branchName: env.PR_BRANCH },
        expectedHeadOid: env.HEAD_SHA,
        message: { headline: "Regenerate OpenAPI models" },
        fileChanges: { additions: changed },
      },
    },
  });
  assert(!result.errors, "GitHub rejected the commit; the PR may have moved");
  console.log(`Committed generated files: ${result.data.createCommitOnBranch.commit.oid}`);
}

if (require.main === module) {
  const env = process.env;
  const request = async (endpoint, body) => {
    const response = await fetch(`https://api.github.com${endpoint}`, {
      method: body ? "POST" : "GET",
      headers: {
        Authorization: `Bearer ${env.GH_TOKEN}`,
        Accept: "application/vnd.github+json",
        "Content-Type": "application/json",
      },
      ...(body && { body: JSON.stringify(body) }),
    });
    assert(response.ok, `GitHub request failed: ${response.status}`);
    return response.json();
  };
  const task = process.argv.includes("--validate")
    ? Promise.resolve().then(() => readGenerated(env.GENERATED_DIRECTORY))
    : commitGenerated(env, request);
  task.catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}

module.exports = { readGenerated, matchesPullRequest, commitGenerated };
