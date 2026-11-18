import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { dirname, isAbsolute, join } from "node:path";
import { pathToFileURL } from "node:url";

/**
 * Load a node_modules package at runtime instead of letting the daemon's
 * server bundler inline it. Needed for SDKs whose code references bare
 * `__dirname` or `import.meta`: the daemon executes the CJS bundle inside a
 * plain function scope, where neither is defined.
 *
 * The specifier is only present as a runtime string, so the bundler keeps
 * this call opaque. Paseo 0.16 starts directory plugins from the host app
 * entry, so prefer the configured plugin directory and retain the legacy
 * process-based lookup for older hosts.
 */
function pluginRequire(): NodeJS.Require {
  const home = process.env.PASEO_HOME;
  if (home) {
    try {
      const config = JSON.parse(readFileSync(join(home, "config.json"), "utf8")) as {
        plugins?: Record<string, { path?: unknown }>;
      };
      const configured = config.plugins?.["paseo-im"]?.path;
      if (typeof configured === "string" && isAbsolute(configured)) {
        return createRequire(join(configured, "package.json"));
      }
    } catch {
      // Fall through for older hosts and incomplete configurations.
    }
  }
  const entry = process.argv[1];
  const base = entry && isAbsolute(entry) ? dirname(entry) : process.cwd();
  return createRequire(join(base, "package.json"));
}

export async function loadPackage<T = Record<string, unknown>>(name: string): Promise<T> {
  const entry = pluginRequire().resolve(name);
  return (await import(pathToFileURL(entry).href)) as T;
}
