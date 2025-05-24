import React, { useState, useRef, useEffect } from 'react';
import { Send, ChevronDown, ChevronUp, Search, Calculator, MessageSquare, ThumbsUp } from 'lucide-react';

// Constants
const API_ENDPOINT = 'http://localhost:3001/api/deliberate';

const AI_MODELS = [
  { id: 'openai', name: 'OpenAI', color: 'text-green-600', bgColor: 'bg-green-100' },
  { id: 'claude', name: 'Claude', color: 'text-blue-600', bgColor: 'bg-blue-100' },
  { id: 'gemini', name: 'Gemini', color: 'text-purple-600', bgColor: 'bg-purple-100' }
];

const DISCUSSION_ICONS = {
  comment: '💭',
  question: '❓',
  defense: '🛡️',
  proposal: '💡'
};

// Utility functions
const getModelById = (modelId) => AI_MODELS.find(m => m.id === modelId);

// Component: Tool Call Display
const ToolCall = ({ toolName, toolInput }) => (
  <div className="flex items-start gap-2 my-1">
    <div className="flex items-center gap-1 text-xs bg-gray-200 rounded px-2 py-1">
      {toolName === 'calculation' ? <Calculator className="w-3 h-3" /> : <Search className="w-3 h-3" />}
      <span className="font-medium">{toolName}</span>
    </div>
    <code className="text-xs bg-gray-100 rounded px-2 py-1 font-mono">{toolInput}</code>
  </div>
);

// Component: Tool Response Display
const ToolResponse = ({ response }) => (
  <div className="ml-6 text-sm text-gray-600 bg-red-50 rounded p-2 my-1">
    → {response}
  </div>
);

// Component: Linear Thinking Display for each model
const ModelThinkingSection = ({ modelId, thinkingSteps }) => {
  const model = getModelById(modelId);
  
  return (
    <div className="mb-4">
      <div className={`text-xs font-medium ${model.color} mb-2`}>{model.name}</div>
      <div className="pl-4 space-y-1">
        {thinkingSteps.map((step, idx) => (
          <div key={idx}>
            {step.type === 'thought' && (
              <div className="text-sm text-gray-700">{step.content}</div>
            )}
            {step.type === 'tool_call' && (
              <ToolCall toolName={step.toolName} toolInput={step.toolInput} />
            )}
            {step.type === 'tool_response' && (
              <ToolResponse response={step.toolResponse} />
            )}
          </div>
        ))}
      </div>
    </div>
  );
};

// Component: Proposals Section
const ProposalsSection = ({ proposalTurn }) => {
  if (!proposalTurn) return null;
  
  return (
    <div>
      <h4 className="text-xs font-medium text-gray-500 mb-2">Initial Proposals</h4>
      <div className="grid grid-cols-3 gap-2">
        {Object.entries(proposalTurn.responses).map(([modelId, response]) => {
          const model = getModelById(modelId);
          return (
            <div key={modelId} className="bg-gray-50 rounded p-2 text-center">
              <div className={`text-xs font-medium ${model.color}`}>{model.name}</div>
              <div className="text-sm font-mono mt-1">{response.content}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

// Component: Ratings Section
const RatingsSection = ({ ratingTurn }) => {
  if (!ratingTurn) return null;
  
  return (
    <div>
      <h4 className="text-xs font-medium text-gray-500 mb-2">Peer Review</h4>
      <div className="space-y-2">
        {Object.entries(ratingTurn.responses).map(([reviewerId, response]) => {
          const reviewer = getModelById(reviewerId);
          return (
            <div key={reviewerId} className="bg-gray-50 rounded p-3">
              <div className={`text-xs font-medium ${reviewer.color} mb-2`}>
                {reviewer.name}'s ratings:
              </div>
              <div className="grid grid-cols-3 gap-2">
                {Object.entries(response.metadata.ratings).map(([modelId, rating]) => {
                  const model = getModelById(modelId);
                  return (
                    <div key={modelId} className="text-xs">
                      <div className="flex items-center gap-1">
                        <span className={model.color}>
                          {model.name}: {'⭐'.repeat(rating.score)}
                        </span>
                      </div>
                      <p className="text-gray-600 mt-1">{rating.comment}</p>
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

// Component: Discussion Item
const DiscussionItem = ({ modelId, response }) => {
  const model = getModelById(modelId);
  const icon = DISCUSSION_ICONS[response.metadata?.discussionType] || '💬';
  
  return (
    <div className="flex gap-3 p-3 bg-white rounded border border-gray-200">
      <div className={`${model.bgColor} ${model.color} rounded-full w-8 h-8 flex items-center justify-center text-xs font-medium`}>
        {model.name[0]}
      </div>
      <div className="flex-1">
        <div className="flex items-center gap-2 mb-1">
          <span className={`text-xs font-medium ${model.color}`}>{model.name}</span>
          <span className="text-xs text-gray-400">{icon}</span>
          {response.metadata?.target && (
            <span className="text-xs text-gray-400">→ {response.metadata.target}</span>
          )}
        </div>
        <p className="text-sm text-gray-700">{response.content}</p>
      </div>
    </div>
  );
};

// Component: Discussion Section
const DiscussionSection = ({ discussionTurns }) => {
  if (!discussionTurns || discussionTurns.length === 0) return null;
  
  return (
    <div>
      <h4 className="text-xs font-medium text-gray-500 mb-2">Discussion</h4>
      <div className="space-y-2">
        {discussionTurns.map(turn => 
          Object.entries(turn.responses).map(([modelId, response]) => (
            <DiscussionItem
              key={`${turn.turnId}-${modelId}`}
              modelId={modelId}
              response={response}
            />
          ))
        )}
      </div>
    </div>
  );
};

// Component: Voting Results
const VotingResults = ({ votes, winner }) => {
  if (!votes || Object.keys(votes).length === 0) return null;
  
  return (
    <div>
      <h4 className="text-xs font-medium text-gray-500 mb-2">Final Vote</h4>
      <div className="bg-gray-50 rounded p-3">
        <div className="grid grid-cols-3 gap-4">
          {AI_MODELS.map(model => {
            const voteCount = votes[model.id] || 0;
            const isWinner = model.id === winner;
            return (
              <div key={model.id} className="text-center">
                <div className={`text-xs font-medium ${model.color}`}>{model.name}</div>
                <div className={`text-2xl font-bold mt-1 ${isWinner ? 'text-green-600' : 'text-gray-400'}`}>
                  {voteCount}
                </div>
                <div className="text-xs text-gray-500">vote{voteCount !== 1 ? 's' : ''}</div>
                {isWinner && <div className="text-xs text-green-600 mt-1">✓ Selected</div>}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};

// Component: Collapsible Section
const CollapsibleSection = ({ title, expanded, onToggle, children }) => (
  <div className="bg-white rounded-lg border border-gray-500 overflow-hidden">
    <button
      onClick={onToggle}
      className="flex items-center justify-between w-full p-3 hover:bg-gray-50 transition-colors"
    >
      <span className="text-sm font-medium text-gray-700">{title}</span>
      {expanded ? 
        <ChevronUp className="w-4 h-4 text-gray-200" /> : 
        <ChevronDown className="w-4 h-4 text-gray-400" />
      }
    </button>
    
    {expanded && (
      <div className="p-4 border-t border-gray-200">
        {children}
      </div>
    )}
  </div>
);

// Component: Loading State
const LoadingState = () => {
  const steps = [
    'Thinking independently...',
    'Running calculations...',
    'Reviewing proposals...',
    'Reaching consensus...'
  ];
  
  return (
    <div className="space-y-3">
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <div className="flex items-center gap-2 text-sm text-gray-600 mb-3">
          <div className="w-2 h-2 bg-gray-400 rounded-full animate-pulse" />
          <span>Models are working on your question...</span>
        </div>
        <div className="space-y-1">
          {steps.map((step, idx) => (
            <div key={idx} className="text-xs text-gray-500 flex items-center gap-2">
              <div className={`w-1.5 h-1.5 rounded-full ${idx === 0 ? 'bg-blue-500 animate-pulse' : 'bg-gray-300'}`} />
              {step}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

// Component: Message
const Message = ({ message, expandedSection, onToggleSection }) => {
  if (message.role === 'user') {
    return (
      <div className="flex justify-end">
        <div className="bg-gray-800 text-white rounded-2xl px-4 py-2 max-w-[80%]">
          <p className="text-sm">{message.content}</p>
        </div>
      </div>
    );
  }

  // Process data for display
  const processedData = message.processedData;

  return (
    <div className="space-y-3">
      {/* Process Sections */}
      {message.turns && processedData && (
        <>
          {/* Thinking Section */}
          <CollapsibleSection
            title="Thinking"
            expanded={expandedSection[`${message.id}-thinking`]}
            onToggle={() => onToggleSection(`${message.id}-thinking`)}
          >
            {processedData.thinkingByModel && Object.entries(processedData.thinkingByModel).map(([modelId, steps]) => (
              <ModelThinkingSection key={modelId} modelId={modelId} thinkingSteps={steps} />
            ))}
          </CollapsibleSection>

          {/* Deliberation Section */}
          {processedData.deliberation.length > 0 && (
            <CollapsibleSection
              title="Deliberation"
              expanded={expandedSection[`${message.id}-deliberation`]}
              onToggle={() => onToggleSection(`${message.id}-deliberation`)}
            >
              <div className="space-y-4">
                <ProposalsSection 
                  proposalTurn={processedData.proposalTurn} 
                />
                <RatingsSection 
                  ratingTurn={processedData.ratingTurn} 
                />
                <DiscussionSection 
                  discussionTurns={processedData.discussionTurns} 
                />
                <VotingResults votes={processedData.votes} winner={processedData.winner} />
              </div>
            </CollapsibleSection>
          )}
        </>
      )}

      {/* Final Answer */}
      <div className="flex gap-3">
        <div className="flex-1">
          <p className="text-gray-800 text-sm leading-relaxed">{message.content}</p>
        </div>
      </div>
    </div>
  );
};

// Main App Component
function App() {
  const [messages, setMessages] = useState([
    {
      id: 1,
      role: 'assistant',
      content: 'Hey! I\'m Deliberations. Ask me anything and I\'ll consult with multiple AI models to get you the best answer.',
      timestamp: new Date(),
      turns: null,
      processedData: null
    }
  ]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [expandedSection, setExpandedSection] = useState({});
  const messagesEndRef = useRef(null);
  const inputRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  // Process turns into display format - restructured to maintain linearity
  const processTurns = (turns) => {
    const thinkingTurns = turns.filter(t => t.phase === 'thinking');
    const deliberationTurns = turns.filter(t => t.phase === 'deliberation');
    const votingTurns = turns.filter(t => t.phase === 'voting');
    
    // Group thinking steps by model to maintain linear flow
    const thinkingByModel = {};
    AI_MODELS.forEach(model => {
      thinkingByModel[model.id] = [];
    });
    
    thinkingTurns.forEach(turn => {
      Object.entries(turn.responses).forEach(([modelId, response]) => {
        thinkingByModel[modelId].push(response);
      });
    });
    
    // Extract specific deliberation turns
    const proposalTurn = deliberationTurns.find(t => 
      Object.values(t.responses).some(r => r.type === 'proposal')
    );
    const ratingTurn = deliberationTurns.find(t => 
      Object.values(t.responses).some(r => r.type === 'rating')
    );
    const discussionTurns = deliberationTurns.filter(t => 
      Object.values(t.responses).some(r => r.type === 'discussion')
    );
    
    // Count votes
    const votes = {};
    votingTurns.forEach(turn => {
      Object.entries(turn.responses).forEach(([model, response]) => {
        if (response.type === 'vote' && response.metadata?.vote) {
          votes[response.metadata.vote] = (votes[response.metadata.vote] || 0) + 1;
        }
      });
    });
    
    const winner = Object.entries(votes).sort((a, b) => b[1] - a[1])[0]?.[0];
    
    return {
      thinkingByModel,
      deliberation: deliberationTurns,
      proposalTurn,
      ratingTurn,
      discussionTurns,
      voting: votingTurns,
      votes,
      winner
    };
  };

  // Mock data generation
  const generateMockTurns = () => {
    return [
      // Thinking phase
      {
        turnId: 1,
        phase: 'thinking',
        responses: {
          openai: { type: 'thought', content: 'I need to calculate this expression following order of operations.' },
          claude: { type: 'thought', content: 'Let me parse this mathematical expression and solve step by step.' },
          gemini: { type: 'thought', content: 'Following PEMDAS, I\'ll handle multiplication first.' }
        }
      },
      {
        turnId: 2,
        phase: 'thinking',
        responses: {
          openai: { type: 'tool_call', toolName: 'calculation', toolInput: '136726 * 4' },
          claude: { type: 'tool_call', toolName: 'calculation', toolInput: '136726 * 4' },
          gemini: { type: 'tool_call', toolName: 'search', toolInput: 'order of operations mathematics' }
        }
      },
      {
        turnId: 3,
        phase: 'thinking',
        responses: {
          openai: { type: 'tool_response', toolResponse: '546904' },
          claude: { type: 'tool_response', toolResponse: '546904' },
          gemini: { type: 'tool_response', toolResponse: 'PEMDAS: Parentheses, Exponents, Multiplication/Division, Addition/Subtraction' }
        }
      },
      {
        turnId: 4,
        phase: 'thinking',
        responses: {
          openai: { type: 'thought', content: 'Now I\'ll add the first two numbers.' },
          claude: { type: 'thought', content: 'I\'ll compute the sum of the first two terms.' },
          gemini: { type: 'tool_call', toolName: 'calculation', toolInput: '136726 * 4' }
        }
      },
      {
        turnId: 5,
        phase: 'thinking',
        responses: {
          openai: { type: 'tool_call', toolName: 'calculation', toolInput: '37463767 + 348736734' },
          claude: { type: 'tool_call', toolName: 'calculation', toolInput: '37463767 + 348736734' },
          gemini: { type: 'tool_response', toolResponse: '546904' }
        }
      },
      // Deliberation phase
      {
        turnId: 10,
        phase: 'deliberation',
        responses: {
          openai: { type: 'proposal', content: '385653597' },
          claude: { type: 'proposal', content: '385653597' },
          gemini: { type: 'proposal', content: '385653597' }
        }
      },
      {
        turnId: 11,
        phase: 'deliberation',
        responses: {
          openai: { 
            type: 'rating',
            metadata: {
              ratings: {
                openai: { score: 5, comment: 'My calculation follows the correct order of operations.' },
                claude: { score: 5, comment: 'Claude\'s approach is methodical and correct.' },
                gemini: { score: 5, comment: 'Gemini\'s verification of rules is commendable.' }
              }
            }
          },
          claude: { 
            type: 'rating',
            metadata: {
              ratings: {
                openai: { score: 5, comment: 'Clear and accurate calculation.' },
                claude: { score: 5, comment: 'My systematic approach ensures accuracy.' },
                gemini: { score: 5, comment: 'Good practice to verify fundamentals.' }
              }
            }
          },
          gemini: { 
            type: 'rating',
            metadata: {
              ratings: {
                openai: { score: 5, comment: 'Straightforward and correct.' },
                claude: { score: 5, comment: 'Matches my result perfectly.' },
                gemini: { score: 5, comment: 'I verified rules before applying.' }
              }
            }
          }
        }
      },
      // Discussion turns
      {
        turnId: 12,
        phase: 'deliberation',
        responses: {
          gemini: { 
            type: 'discussion', 
            content: 'I notice we all arrived at the same answer. This gives me high confidence in our result.',
            metadata: { discussionType: 'comment' }
          }
        }
      },
      {
        turnId: 13,
        phase: 'deliberation',
        responses: {
          openai: { 
            type: 'discussion', 
            content: 'Was the search for PEMDAS necessary given the straightforward nature of the problem?',
            metadata: { discussionType: 'question', target: 'gemini' }
          }
        }
      },
      {
        turnId: 14,
        phase: 'deliberation',
        responses: {
          gemini: { 
            type: 'discussion', 
            content: 'I believe verifying fundamental rules prevents errors, especially in more complex scenarios.',
            metadata: { discussionType: 'defense' }
          }
        }
      },
      {
        turnId: 15,
        phase: 'deliberation',
        responses: {
          claude: { 
            type: 'discussion', 
            content: 'I agree with both approaches. For this problem, we all handled it correctly.',
            metadata: { discussionType: 'comment' }
          }
        }
      },
      {
        turnId: 16,
        phase: 'deliberation',
        responses: {
          openai: { 
            type: 'discussion', 
            content: 'Since we have consensus, I propose we finalize on 385,653,597 as our answer.',
            metadata: { discussionType: 'proposal' }
          }
        }
      },
      // Voting phase
      {
        turnId: 20,
        phase: 'voting',
        responses: {
          openai: { type: 'vote', metadata: { vote: 'openai' } },
          claude: { type: 'vote', metadata: { vote: 'openai' } },
          gemini: { type: 'vote', metadata: { vote: 'openai' } }
        }
      }
    ];
  };

  const handleSubmit = async () => {
    if (!inputValue.trim() || isLoading) return;

    const userMessage = {
      id: Date.now(),
      role: 'user',
      content: inputValue.trim(),
      timestamp: new Date()
    };

    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);

    const messageId = Date.now() + 1;

    try {
      // Simulate API delay
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // In production: replace with real API call
      const turns = generateMockTurns();
      const processedData = processTurns(turns);
      
      const assistantMessage = {
        id: messageId,
        role: 'assistant',
        content: '385,653,597',
        timestamp: new Date(),
        turns: turns,
        processedData: processedData
      };

      setMessages(prev => [...prev, assistantMessage]);
      
      // Auto-expand sections
      setExpandedSection(prev => ({
        ...prev,
        [`${messageId}-thinking`]: true,
        [`${messageId}-deliberation`]: true
      }));
    } catch (err) {
      console.error('Error:', err);
      
      const errorMessage = {
        id: messageId,
        role: 'assistant',
        content: 'Sorry, I encountered an error. Please try again.',
        timestamp: new Date(),
        turns: null,
        processedData: null
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
      inputRef.current?.focus();
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  const toggleSection = (sectionId) => {
    setExpandedSection(prev => ({
      ...prev,
      [sectionId]: !prev[sectionId]
    }));
  };

  return (
    <div className="flex flex-col h-screen bg-gray-50">
      {/* Messages Container */}
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-4 py-8">
          {messages.map((message) => (
            <div key={message.id} className="mb-6">
              <Message 
                message={message} 
                expandedSection={expandedSection}
                onToggleSection={toggleSection}
              />
            </div>
          ))}

          {isLoading && <LoadingState />}

          <div ref={messagesEndRef} />
        </div>
      </div>

      {/* Input Area */}
      <div className="border-t border-gray-200 bg-white">
        <div className="max-w-4xl mx-auto px-4 py-4">
          <div className="relative flex items-center bg-gray-50 rounded-full shadow-sm border border-gray-200">
            <input
              ref={inputRef}
              type="text"
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              onKeyPress={handleKeyPress}
              placeholder="Ask anything - multiple AIs will deliberate on your question"
              className="flex-1 bg-transparent px-6 py-3 text-sm focus:outline-none placeholder-gray-400"
              disabled={isLoading}
            />

            <button 
              onClick={handleSubmit}
              disabled={!inputValue.trim() || isLoading}
              className="mr-2 p-2 bg-gray-800 text-white rounded-full hover:bg-gray-700 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
            >
              <Send className="w-4 h-4" />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;