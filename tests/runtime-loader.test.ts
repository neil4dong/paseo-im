import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { loadPackage } from "../server/channels/runtime-loader.ts";

test("Paseo 0.16：从目录插件配置解析运行时依赖", async () => {
  const root = await mkdtemp(join(tmpdir(), "paseo-im-runtime-loader-"));
  const home = join(root, "home");
  const plugin = join(root, "plugin");
  const fixture = join(plugin, "node_modules", "runtime-loader-fixture");
  const previousHome = process.env.PASEO_HOME;
  try {
    await mkdir(home, { recursive: true });
    await mkdir(fixture, { recursive: true });
    await writeFile(join(home, "config.json"), JSON.stringify({ plugins: { "paseo-im": { path: plugin } } }));
    await writeFile(join(plugin, "package.json"), JSON.stringify({ name: "paseo-im", type: "module" }));
    await writeFile(
      join(fixture, "package.json"),
      JSON.stringify({ name: "runtime-loader-fixture", type: "module", exports: "./index.js" }),
    );
    await writeFile(join(fixture, "index.js"), "export const marker = 'configured-plugin-root';\n");
    process.env.PASEO_HOME = home;

    const loaded = await loadPackage<{ marker: string }>("runtime-loader-fixture");
    assert.equal(loaded.marker, "configured-plugin-root");
  } finally {
    if (previousHome === undefined) delete process.env.PASEO_HOME;
    else process.env.PASEO_HOME = previousHome;
    await rm(root, { recursive: true, force: true });
  }
});
