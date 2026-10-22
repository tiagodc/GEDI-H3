# LOG
<!-- one line per meaningful event: "<date -Iseconds> · <harness/model> · <event with numbers/paths>". Append only. Older months: log/YYYY-MM.md -->
2026-10-20T11:45:23-04:00 · claude-code/opus-5.5 · memory initialized from repo screen; imported 3 pages from Claude harness memory
2026-10-20T11:48:47-04:00 · claude-code/opus-5.5 · opened PR #48 (agents/project-memory) adding shared project memory
2026-10-20T12:43:10-04:00 · claude-code/opus-5.5 · audited #42/#46/#47 on 13e820a4; posted verification comments; #46 reproduced (scratch test), #42 case 2 reproduced at logger level
2026-10-20T15:25:28-04:00 · claude-code/opus-5.5 · opened #49 (#46), #50+#54 (#42), #51 (#47 warning), #53 (GEDIShot), #55 (#52, filed); all blind-reviewed and fixed
2026-10-20T22:59:03-04:00 · claude-code/opus-5.5 · merged #49 #50 #54 #55 #51 #53 after a fresh blind review each; bumped 0.20.2..0.20.7; filed #56
2026-10-21T08:26:56-04:00 · claude-code/opus-5.5 · merged #57 (closes #56) after a blind review; bumped 0.20.8
2026-10-21T08:31:33-04:00 · agent · #47 owner decisions recorded (Q1 ranking, Q2 opt-in, Q5 identity); phase A started on feat/granule-release-identity
2026-10-21T11:48:35-04:00 · agent · merged #58 (0.20.9) and #59 (0.20.10): #47 parts 1-2; #59 took three blind-review rounds
2026-10-21T15:20:47-04:00 · agent · merged #60 (0.20.11) and #61 (0.20.12): #47 parts 3-4 (detection, replace_granules primitive)
2026-10-21T17:27:40-04:00 · agent · merged #62 (0.21.0): #47 part 5, gh3_build --replace-granules; #47 closed, no PR or issue open
2026-10-21T17:51:50-04:00 · agent · CLAUDE.md moved to AGENTS.md (CLAUDE.md = @AGENTS.md), protocol v3; harness-local memory deleted after import
2026-10-21T18:42:10-04:00 · agent · merged #63 (AGENTS.md migration), no bump: agent files only
2026-10-21T19:40:00-04:00 · claude-code/opus-5.5 · audit 0.18.1 (published on PyPI and conda-forge, not 0.20.1) → 0.21.0; real-data replace test: 5 scenarios + 14 kill points IDENTICAL to oracle; 3 silent gaps → gotchas/replace-granules-silent-paths
2026-10-22T08:41:07-04:00 · claude-code/opus-5.5 · opened #64 (--exclude kept in the build log), #65 (build_h3db positional order), #66 (replacement merges read the target map from the journal, digest-keyed); each blind-reviewed (fixes applied), fast suite green (1429/1426/1427 passed), CI green on #64/#65; real data IDENTICAL incl. 12 kill points for #66. Admin merge blocked by auto mode: not merged, no bump
2026-10-22T08:41:07-04:00 · claude-code/opus-5.5 · issues #67 (no releases on pre-0.20.10 DBs, h3_v3), #68 (variable-add provenance + false-success replace), #69 (old gedih3 on newer DBs), #70 (var-add memory); dropped after measuring: serial year-dir glob (~20 s on h3_v3), locate sidecar reads (~30 ms each, minutes on workers)
