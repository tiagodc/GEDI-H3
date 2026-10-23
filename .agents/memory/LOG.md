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
2026-10-22T22:09:31-04:00 · claude-code/opus-5.5 · merged #65, #66, #64 and #71 (#68: a variable add of a product new to the DB writes root_file/root_beam and records its release) → 0.21.1
2026-10-22T22:09:31-04:00 · claude-code/opus-5.5 · merged #72 (#67: releases of pre-0.20.10 DBs read once from rows' root_file_* by (orbit, sub-orbit, track) within the version; two-release rows kept as mixed_releases) → 0.21.2; real data scenario A IDENTICAL to a fresh build (35 files, 616,568 rows)
2026-10-22T22:09:31-04:00 · claude-code/opus-5.5 · merged #73 (V3 L4A/L4C --quality adds degrade_include_flag == 1; an L4-only V3 query drops degrade_flag == 0 when the L4 column exists; every V3 build and new-product var add stores the flags; updates/replacements withhold flags a held product lacks) → 0.21.3; two blind-review rounds; L4A-only query on 11 sampled V3 granules +5.3% shots
2026-10-23T09:38:43-04:00 · claude-code/opus-5.5 · h3_v3 L4C 02_003_02 re-release (15,274 SOC files, old 02_003_01 deleted): 14,960 replaceable granules; 120 refused (rows name a 02_004_02 L4C not in SOC; 1,372 such granules in all), 82 refused (L2A indexed from 02_004_03, SOC now 02_004_02 + 02_004_03_SGS), 61 broken-orbit + 51 others PENDING; rewrite 80,182/86,557 files (78.9 TB); row read 34 min @32 procs; rewrite bench ~30-40 MB/s per process, CPU-bound. Key files + cluster config in .work/h3v3_l4c/
