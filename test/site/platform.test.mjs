import { test } from "node:test";
import assert from "node:assert/strict";
import { BUILDS, assetName, assetURL, detectPlatform } from "../../site/js/platform.js";

test("asset names match .goreleaser.yaml", () => {
  assert.equal(assetName("darwin", "arm64"), "raftra-cli_darwin_arm64.tar.gz");
  assert.equal(assetName("windows", "amd64"), "raftra-cli_windows_amd64.zip");
  assert.equal(assetURL("shantanu-1607/Raftra", "linux", "amd64"),
    "https://github.com/shantanu-1607/Raftra/releases/latest/download/raftra-cli_linux_amd64.tar.gz");
  assert.equal(BUILDS.length, 6);
});

test("detects desktop platforms", () => {
  assert.deepEqual(
    detectPlatform({ userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", platform: "Win32" }),
    { os: "windows", arch: "amd64", guessed: false });
  assert.deepEqual(
    detectPlatform({ userAgent: "Mozilla/5.0 (X11; Linux x86_64)", platform: "Linux x86_64" }),
    { os: "linux", arch: "amd64", guessed: false });
  assert.deepEqual(
    detectPlatform({ userAgent: "Mozilla/5.0 (X11; Linux aarch64)", platform: "Linux aarch64" }),
    { os: "linux", arch: "arm64", guessed: false });
});

test("macOS: trusts client hints, otherwise guesses Apple silicon", () => {
  const ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)";
  assert.deepEqual(detectPlatform({ userAgent: ua, platform: "MacIntel", uaPlatform: "macOS", uaArch: "x86" }),
    { os: "darwin", arch: "amd64", guessed: false });
  assert.deepEqual(detectPlatform({ userAgent: ua, platform: "MacIntel", uaPlatform: "macOS", uaArch: "arm" }),
    { os: "darwin", arch: "arm64", guessed: false });
  assert.deepEqual(detectPlatform({ userAgent: ua, platform: "MacIntel" }),
    { os: "darwin", arch: "arm64", guessed: true });
});

test("phones are not desktop builds", () => {
  assert.equal(detectPlatform({ userAgent: "Mozilla/5.0 (Linux; Android 14; Pixel 8)", platform: "Linux armv8l" }).os, "mobile");
  assert.equal(detectPlatform({ userAgent: "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X)", platform: "iPhone" }).os, "mobile");
});
