# Agents

Agents are a lightweight abstraction over the Factorial queue and workers to execute an LLM completion + tool calling loop until some end condition is met. Each agent is _stateless_, meaning it takes in a _context_ object to execute a single turn, updates the context, then continues, freeing the context to be picked up by the same or a different instance of the agent.

#### The mental model is:
**Agent**:
* Defines the tools, instructions, stop conditions, logic, etc. 
* Takes in an _AgentContext_ object (messages, turn number, etc.) and _ExecutionContext_ object (retry attempts, owner id, task id, event publisher, etc.) and executes a single agent turn.


**Orchestrator**: 
* Registers each agent through `runners` which spawn concurrent agent workers to process queued tasks.


While a big part of Factorial is the ease of orchestrating durable agents through the orchestrator and its underlying queuing system, agents can also be executed "in line" without needing an orchestrator or Redis (for simpler use cases).


## Quick Start

The simplest way to create an agent:

```python
from factorial import Agent, AgentContext

def get_weather(location: str) -> str:
    """Get the current weather for a location"""
    return f"The weather in {location} is sunny and 72°F"

agent = Agent(
    instructions="You are a helpful weather assistant. Always be friendly and informative.",
    tools=[get_weather],
)
```

## Agent Parameters

**`instructions`** (str): The system prompt sent as the first message to all LLM completions. Used to guide the agent's behavior.

**`tools`** (list): List of Python functions that the agent can call. See [Tools documentation](tools.md) for details.

```python
def search_web(query: str) -> str:
    """Search the web for information"""
    return f"Search results for: {query}"

agent = Agent(
    instructions="You help users research topics by searching the web.",
    tools=[search_web],
)
```

**`model`** (Model | Callable): The LLM model to use. Can be a static model or a function that returns a model based on context.

**`model_settings`** (ModelSettings): Configuration for model parameters like temperature, tool choice, etc.

**`max_turns`** (int): Maximum number of conversation turns before the agent must complete. Useful for preventing infinite loops.

**`output_type`** (BaseModel): Pydantic model for structured output. When set, the agent must use the `final_output` tool to complete.

**`context_class`** (type): Custom context class to use instead of the default `AgentContext`.

**`context_window_limit`** (int): Maximum number of tokens in the context window.

**`request_timeout`** (float): HTTP timeout for LLM requests in seconds (default: 120.0).

**`parse_tool_args`** (bool): Whether LLM tool call arguments should be parsed as opposed to remaining a raw string. Defaults to true.

## Setting the Model

```python
from factorial import gpt_5, gpt_41, gpt_41_mini, claude_4_sonnet, fallback_models

# Static model
agent = Agent(
    instructions="You are a helpful assistant",
    model=gpt_41,
)

# Dynamic model based on context
def choose_model(agent_ctx: AgentContext) -> Model:
    return gpt_41 if agent_ctx.turn == 0 else gpt_41_mini

agent = Agent(
    instructions="You are a helpful assistant",
    model=choose_model,
)

# Fallback models (will be attempted in order, e.g. if claude_4_sonnet errors, gpt_5 will be tried next)
agent = Agent(
    instructions="You are a helpful assistant",
    model=fallback_models(claude_4_sonnet, gpt_5, gpt_41),
)
```

## Model Settings

Use `ModelSettings` to configure LLM behavior:

```python
from factorial import ModelSettings, AgentContext

agent = Agent(
    description="Creative Writer",
    instructions="You write creative stories",
    model_settings=ModelSettings[AgentContext](
        temperature=0.8, 
        max_completion_tokens=1000,
        tool_choice="auto",
        parallel_tool_calls=True,
    ),
)
```

### Dynamic Model Settings

Model settings can be functions that receive the agent context:

```python
agent = Agent(
    description="Adaptive Assistant",
    instructions="You adapt your behavior based on conversation progress",
    model_settings=ModelSettings(
        temperature=lambda ctx: 0 if ctx.turn == 0 else 1,
        tool_choice=lambda ctx: (
            {
                "type": "function",
                "function": {"name": "plan"},
            }
            if ctx.turn == 0
            else "required"
        ),
    ),
)
```

## Structured Output

Define structured output using Pydantic models:

```python
from factorial.utils import BaseModel

class Joke(BaseModel):
    setup: str
    punchline: str

agent = Agent(
    description="Joke Agent",
    instructions="Funny agent",
    tools=[search_web],
    output_type=Joke,
)
```

When `output_type` is set, the agent automatically creates a `final_output` tool that the agent must call to complete.

## Creating and Running Tasks

Agents don't run directly - they process tasks through an orchestrator:

```python
from factorial import AgentContext

# Create and enqueue the task (see the orchestrator section on how to set up the orchestrator)
task = await orchestrator.enqueue_task(
    agent=agent,
    owner_id="user123", 
    payload=AgentContext(
        query="What's the weather like in San Francisco?"
    )
)
```

## Custom Agent Classes

For advanced use cases, extend `BaseAgent` or `Agent`:

```python
from factorial import BaseAgent, AgentContext, ExecutionContext

class CustomAgent(BaseAgent[AgentContext]):
    async def completion(
        self, 
        agent_ctx: AgentContext, 
        messages: list[dict[str, Any]]
    ) -> ChatCompletion:
        """Override to customize LLM completion"""
        # Add custom logic before completion
        print(f"Making completion request for turn {agent_ctx.turn}")
        
        # Call parent implementation
        response = await super().completion(agent_ctx, messages)
        
        # Add custom logic after completion
        print(f"Received response with {len(response.choices)} choices")
        
        return response
    
    async def tool_action(
        self, 
        tool_call: ChatCompletionMessageToolCall, 
        agent_ctx: AgentContext
    ) -> FunctionToolActionResult:
        """Override to customize tool execution"""
        print(f"Executing tool: {tool_call.function.name}")
        
        return await super().tool_action(tool_call, agent_ctx)
    
    async def run_turn(
        self, 
        agent_ctx: AgentContext
    ) -> TurnCompletion[AgentContext]:
        """Override to customize turn logic"""
        execution_ctx = self.get_execution_context()
        
        # Custom logic before turn
        if agent_ctx.turn > 5:
            # Force completion after 5 turns
            return TurnCompletion(
                is_done=True,
                context=agent_ctx,
                output="Conversation limit reached"
            )
        
        return await super().run_turn(agent_ctx)
```

## Custom Context Classes

Create custom context classes for specialized agents:

```python
from factorial import AgentContext, BaseAgent

class ResearchContext(AgentContext):
    research_topic: str = ""
    sources_found: list[str] = []
    confidence_level: float = 0.0

class ResearchAgent(BaseAgent[ResearchContext]):
    def __init__(self):
        super().__init__(
            description="Research specialist",
            instructions="You conduct thorough research",
            tools=[search_web, analyze_source],
            context_class=ResearchContext,
        )
    
    async def run_turn(self, agent_ctx: ResearchContext) -> TurnCompletion[ResearchContext]:
        # Access custom context fields
        print(f"Researching: {agent_ctx.research_topic}")
        print(f"Sources found: {len(agent_ctx.sources_found)}")
        
        return await super().run_turn(agent_ctx)

# Usage
research_context = ResearchContext(
    query="Research the impact of AI on education",
    research_topic="AI in Education",
    messages=[],
    turn=0,
)

# set up orchestrator and register runners...
orchestrator = ...

task = await orchestrator.enqueue_task(
    agent=research_agent,
    owner_id="researcher123",
    payload=research_context
)
```

## Error Handling and Retries

Agents include built-in retry logic with exponential backoff:

```python
from factorial import retry, publish_progress

class RobustAgent(BaseAgent[AgentContext]):
    @retry(max_attempts=5, delay=2.0, exponential_base=1.5)
    async def custom_method(self, data: str) -> str:
        """This method will retry up to 5 times on failure"""
        # Your logic here
        return "processed"
    
    @publish_progress(func_name="data_processing")
    async def process_data(self, agent_ctx: AgentContext) -> str:
        """This method publishes progress events automatically"""
        # Events published:
        # - progress_update_data_processing_started
        # - progress_update_data_processing_completed (or _failed)
        return await self.custom_method("some data")
```

## Agent Lifecycle

Understanding the agent execution lifecycle:

1. **Task Creation**: Create a task with initial context
2. **Task Enqueueing**: Add task to the processing queue
3. **Turn Execution**: Agent processes one turn at a time
   - Prepare messages (system prompt + conversation history)
   - Make LLM completion request
   - Execute any tool calls
   - Update context with results
4. **Completion Check**: Determine if agent should finish
5. **Context Update**: Save updated context
6. **Next Turn**: Repeat until completion or max turns reached
