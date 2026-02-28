import asyncio
import json
import sys

from examples.vercel_runtime.agent import assistant_agent

from factorial import Orchestrator

orchestrator = Orchestrator()

orchestrator.register_runner(agent=assistant_agent)


def main() -> None:
    result = asyncio.run(orchestrator.run_maintenance_cron_tick())
    print(json.dumps(result), file=sys.stderr)


if __name__ == "__main__":
    main()
