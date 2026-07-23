# Fork Notes

This repository is a fork of [JoeanAmier/XHS-Downloader](https://github.com/JoeanAmier/XHS-Downloader).

## Delta from upstream

- Adds `POST /xhs/creator/batch` for cursor-based creator-note pagination.
- Resolves each creator note through the existing XHS detail extraction pipeline.
- Returns normalized image, video, and livePhoto media summaries.
- Skips failed notes and reports the error count without aborting a batch.
- Adds the `xhshow` request-signing dependency used by the creator-feed API.

The upstream GNU GPL v3 license is preserved unchanged. See [SOURCES.md](SOURCES.md) for the pinned upstream revision.
