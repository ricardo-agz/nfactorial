# Examples

Practical examples demonstrating common patterns and use cases with the Factorial framework.

## Simple Chat Agent

A basic conversational agent:

```python
from factorial import BaseAgent, AgentContext

# Define tools
def get_time(query: str, agent_ctx: AgentContext) -> str:
    """Get the current time."""
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def calculate(expression: str, agent_ctx: AgentContext) -> str:
    """Safely evaluate mathematical expressions."""
    try:
        # Simple calculator - be careful with eval in production
        result = eval(expression.replace("^", "**"))
        return str(result)
    except Exception as e:
        return f"Error: {e}"

# Create agent
chat_agent = BaseAgent(
    description="Helpful Chat Assistant",
    instructions="You are a helpful assistant that can tell time and do math.",
    tools=[get_time, calculate],
    tool_actions={
        "get_time": get_time,
        "calculate": calculate,
    }
)

# Use the agent
context = AgentContext(query="What time is it?")
task = chat_agent.create_task(owner_id="user123", payload=context)
result = chat_agent.run_task(task.id)
print(result.response)
```

## Research Agent

An agent that can search and analyze information:

```python
from factorial import BaseAgent, AgentContext
from typing import List, Dict, Any
import requests

class ResearchContext(AgentContext):
    sources: List[Dict[str, str]] = []
    findings: Dict[str, Any] = {}
    
    def add_source(self, url: str, title: str):
        self.sources.append({"url": url, "title": title})

def web_search(query: str, agent_ctx: ResearchContext) -> str:
    """Search the web for information."""
    # Mock implementation - replace with real search API
    results = [
        {"title": f"Result 1 for {query}", "url": "https://example1.com"},
        {"title": f"Result 2 for {query}", "url": "https://example2.com"},
    ]
    
    for result in results:
        agent_ctx.add_source(result["url"], result["title"])
    
    return f"Found {len(results)} results for '{query}'"

def analyze_content(url: str, agent_ctx: ResearchContext) -> str:
    """Analyze content from a URL."""
    # Mock analysis
    analysis = f"Analysis of {url}: This content discusses key concepts related to the research topic."
    
    # Store findings
    agent_ctx.findings[url] = {
        "summary": analysis,
        "key_points": ["Point 1", "Point 2", "Point 3"]
    }
    
    return analysis

research_agent = BaseAgent[ResearchContext](
    description="Research Assistant",
    instructions="You help users research topics by searching and analyzing information.",
    tools=[web_search, analyze_content],
    tool_actions={
        "web_search": web_search,
        "analyze_content": analyze_content,
    },
    context_class=ResearchContext,
)

# Use the research agent
context = ResearchContext(query="Research artificial intelligence trends")
task = research_agent.create_task(owner_id="researcher", payload=context)
result = research_agent.run_task(task.id)
```

## File Processing Agent

An agent that processes files and documents:

```python
from factorial import BaseAgent, AgentContext
import os
import json
from pathlib import Path

class FileContext(AgentContext):
    processed_files: List[str] = []
    file_contents: Dict[str, str] = {}

def read_file(filepath: str, agent_ctx: FileContext) -> str:
    """Read content from a file."""
    try:
        path = Path(filepath)
        if not path.exists():
            return f"File {filepath} does not exist"
        
        content = path.read_text(encoding='utf-8')
        agent_ctx.file_contents[filepath] = content
        agent_ctx.processed_files.append(filepath)
        
        return f"Read {len(content)} characters from {filepath}"
    except Exception as e:
        return f"Error reading {filepath}: {e}"

def write_file(filepath: str, content: str, agent_ctx: FileContext) -> str:
    """Write content to a file."""
    try:
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding='utf-8')
        
        return f"Successfully wrote {len(content)} characters to {filepath}"
    except Exception as e:
        return f"Error writing to {filepath}: {e}"

def list_files(directory: str, agent_ctx: FileContext) -> str:
    """List files in a directory."""
    try:
        path = Path(directory)
        if not path.exists():
            return f"Directory {directory} does not exist"
        
        files = [f.name for f in path.iterdir() if f.is_file()]
        return f"Files in {directory}: {', '.join(files)}"
    except Exception as e:
        return f"Error listing files in {directory}: {e}"

file_agent = BaseAgent[FileContext](
    description="File Processing Assistant",
    instructions="You help users read, write, and manage files.",
    tools=[read_file, write_file, list_files],
    tool_actions={
        "read_file": read_file,
        "write_file": write_file,
        "list_files": list_files,
    },
    context_class=FileContext,
)
```

## API Integration Agent

An agent that integrates with external APIs:

```python
from factorial import BaseAgent, AgentContext
import requests
from typing import Optional

class APIContext(AgentContext):
    api_calls: List[Dict[str, Any]] = []
    api_responses: Dict[str, Any] = {}

def make_api_request(
    url: str, 
    method: str = "GET", 
    headers: Optional[str] = None,
    data: Optional[str] = None,
    agent_ctx: APIContext = None
) -> str:
    """Make an HTTP API request."""
    try:
        # Parse headers if provided
        headers_dict = {}
        if headers:
            for line in headers.split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    headers_dict[key.strip()] = value.strip()
        
        # Make request
        if method.upper() == "GET":
            response = requests.get(url, headers=headers_dict)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers_dict, data=data)
        else:
            return f"Unsupported method: {method}"
        
        # Store request info
        request_info = {
            "url": url,
            "method": method,
            "status_code": response.status_code,
            "timestamp": datetime.now().isoformat()
        }
        agent_ctx.api_calls.append(request_info)
        agent_ctx.api_responses[url] = response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text
        
        return f"API request to {url} returned status {response.status_code}"
        
    except Exception as e:
        return f"Error making API request: {e}"

def get_weather(city: str, agent_ctx: APIContext) -> str:
    """Get weather information for a city."""
    # Mock weather API call
    weather_data = {
        "city": city,
        "temperature": "22°C",
        "condition": "Sunny",
        "humidity": "65%"
    }
    
    agent_ctx.api_responses[f"weather_{city}"] = weather_data
    
    return f"Weather in {city}: {weather_data['temperature']}, {weather_data['condition']}"

api_agent = BaseAgent[APIContext](
    description="API Integration Assistant",
    instructions="You help users interact with APIs and external services.",
    tools=[make_api_request, get_weather],
    tool_actions={
        "make_api_request": make_api_request,
        "get_weather": get_weather,
    },
    context_class=APIContext,
)
```

## Multi-Step Workflow Agent

An agent that handles complex multi-step processes:

```python
from factorial import BaseAgent, AgentContext
from enum import Enum

class WorkflowStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

class WorkflowContext(AgentContext):
    workflow_steps: List[str] = []
    completed_steps: List[str] = []
    current_step: str = ""
    status: WorkflowStatus = WorkflowStatus.PENDING
    step_results: Dict[str, Any] = {}

def start_workflow(steps: str, agent_ctx: WorkflowContext) -> str:
    """Initialize a workflow with specified steps."""
    step_list = [step.strip() for step in steps.split(',')]
    agent_ctx.workflow_steps = step_list
    agent_ctx.status = WorkflowStatus.IN_PROGRESS
    agent_ctx.current_step = step_list[0] if step_list else ""
    
    return f"Started workflow with {len(step_list)} steps: {', '.join(step_list)}"

def complete_step(step_name: str, result: str, agent_ctx: WorkflowContext) -> str:
    """Mark a workflow step as completed."""
    if step_name not in agent_ctx.workflow_steps:
        return f"Step '{step_name}' is not part of the current workflow"
    
    if step_name in agent_ctx.completed_steps:
        return f"Step '{step_name}' is already completed"
    
    agent_ctx.completed_steps.append(step_name)
    agent_ctx.step_results[step_name] = result
    
    # Move to next step
    remaining_steps = [s for s in agent_ctx.workflow_steps if s not in agent_ctx.completed_steps]
    if remaining_steps:
        agent_ctx.current_step = remaining_steps[0]
        return f"Completed step '{step_name}'. Next step: '{agent_ctx.current_step}'"
    else:
        agent_ctx.status = WorkflowStatus.COMPLETED
        agent_ctx.current_step = ""
        return f"Workflow completed! All {len(agent_ctx.workflow_steps)} steps finished."

def get_workflow_status(agent_ctx: WorkflowContext) -> str:
    """Get the current workflow status."""
    total_steps = len(agent_ctx.workflow_steps)
    completed_count = len(agent_ctx.completed_steps)
    
    if total_steps == 0:
        return "No workflow is currently active"
    
    progress = (completed_count / total_steps) * 100
    
    return f"""
Workflow Status: {agent_ctx.status.value}
Progress: {completed_count}/{total_steps} steps ({progress:.1f}%)
Current Step: {agent_ctx.current_step or 'None'}
Completed Steps: {', '.join(agent_ctx.completed_steps)}
"""

workflow_agent = BaseAgent[WorkflowContext](
    description="Workflow Management Assistant",
    instructions="You help users manage multi-step workflows and processes.",
    tools=[start_workflow, complete_step, get_workflow_status],
    tool_actions={
        "start_workflow": start_workflow,
        "complete_step": complete_step,
        "get_workflow_status": get_workflow_status,
    },
    context_class=WorkflowContext,
)
```

## Agent Orchestration Example

Using multiple agents together:

```python
from factorial import Orchestrator

# Create multiple specialized agents
research_agent = BaseAgent(
    description="Research Specialist",
    instructions="You specialize in finding and analyzing information.",
    tools=[web_search, analyze_content],
    tool_actions={"web_search": web_search, "analyze_content": analyze_content}
)

writing_agent = BaseAgent(
    description="Writing Specialist", 
    instructions="You specialize in creating well-structured documents.",
    tools=[write_file, format_text],
    tool_actions={"write_file": write_file, "format_text": format_text}
)

# Create orchestrator
orchestrator = Orchestrator(
    agents=[research_agent, writing_agent],
    description="Research and Writing Team",
    instructions="Coordinate research and writing tasks."
)

# Use orchestrator for complex task
context = AgentContext(
    query="Research AI trends and write a comprehensive report"
)
task = orchestrator.create_task(owner_id="user123", payload=context)
result = orchestrator.run_task(task.id)
```

## Error Handling Patterns

Robust error handling in agents:

```python
from factorial import BaseAgent, AgentContext
import logging

def safe_operation(input_data: str, agent_ctx: AgentContext) -> str:
    """Demonstrate safe operation with error handling."""
    try:
        # Validate input
        if not input_data or not input_data.strip():
            return "Error: Input data cannot be empty"
        
        # Process data
        result = process_data(input_data)
        
        # Log success
        logging.info(f"Successfully processed: {input_data}")
        
        return f"Processed: {result}"
        
    except ValueError as e:
        error_msg = f"Invalid input: {e}"
        logging.warning(error_msg)
        return error_msg
        
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logging.error(error_msg)
        return error_msg

def process_data(data: str) -> str:
    """Mock data processing function."""
    if "error" in data.lower():
        raise ValueError("Data contains error keyword")
    return data.upper()

# Agent with error handling
robust_agent = BaseAgent(
    description="Robust Processing Agent",
    instructions="You handle data processing with comprehensive error handling.",
    tools=[safe_operation],
    tool_actions={"safe_operation": safe_operation}
)
```

## Testing Agents

Example of testing agent functionality:

```python
import pytest
from factorial import BaseAgent, AgentContext

def test_chat_agent():
    """Test basic chat agent functionality."""
    agent = BaseAgent(
        description="Test Agent",
        instructions="You are a test assistant.",
        tools=[get_time],
        tool_actions={"get_time": get_time}
    )
    
    context = AgentContext(query="What time is it?")
    task = agent.create_task(owner_id="test_user", payload=context)
    
    assert task is not None
    assert task.owner_id == "test_user"
    
    # Run task
    result = agent.run_task(task.id)
    assert result is not None
    assert "time" in result.response.lower()

def test_custom_context():
    """Test agent with custom context."""
    context = ResearchContext(
        query="Test query",
        research_depth=2
    )
    
    assert context.research_depth == 2
    assert context.sources == []
    
    # Test context methods
    context.add_source("https://example.com", "Test Source")
    assert len(context.sources) == 1
    assert context.sources[0]["url"] == "https://example.com"

if __name__ == "__main__":
    pytest.main([__file__])
```

## Best Practices

1. **Start Simple**: Begin with basic agents and add complexity gradually
2. **Error Handling**: Always include proper error handling in tools
3. **Context Design**: Design context classes that match your use case
4. **Tool Composition**: Create focused tools that do one thing well
5. **Testing**: Write tests for your agents and tools
6. **Logging**: Add logging for debugging and monitoring
7. **Documentation**: Document your tools and their expected inputs/outputs
8. **Security**: Validate inputs and sanitize outputs, especially for file operations
9. **Performance**: Consider async operations for I/O-bound tasks
10. **Modularity**: Keep agents focused on specific domains or capabilities 