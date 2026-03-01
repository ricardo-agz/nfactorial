from __future__ import annotations

try:
    import vercel.workers  # type: ignore  # noqa: F401
except Exception:
    try:
        import vercel.workers.client  # type: ignore  # noqa: F401
    except Exception as exc:
        raise ImportError(
            "factorial.runtimes.vercel requires `vercel-workers`. "
            "Install with `pip install \"nfactorial[vercel]\"`."
        ) from exc
