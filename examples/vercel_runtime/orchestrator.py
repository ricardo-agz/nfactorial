from examples.vercel_runtime.agent import assistant_agent

from factorial import Orchestrator

orchestrator = Orchestrator()

orchestrator.register_runner(agent=assistant_agent)


def main() -> None:
    orchestrator.run()


if __name__ == "__main__":
    main()
