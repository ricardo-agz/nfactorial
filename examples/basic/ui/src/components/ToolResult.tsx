import React from 'react';
import { 
  Brain, 
  Search, 
  Sparkles, 
  ExternalLink, 
  Globe, 
  MessageCircle, 
  Users, 
  TrendingUp, 
  User, 
  ArrowUp, 
  ArrowDown,
  Clock,
  Hash
} from 'lucide-react';

interface ToolIconProps {
  name: string;
}

export const ToolIcon: React.FC<ToolIconProps> = ({ name }) => {
  switch (name) {
    case 'plan':
      return <Brain className="w-3 h-3 text-blue-600" />;
    case 'search':
      return <Search className="w-3 h-3 text-blue-600" />;
    case 'reflect':
      return <Sparkles className="w-3 h-3 text-blue-600" />;
    case 'scrape':
      return <Globe className="w-3 h-3 text-green-600" />;
    default:
      return <div className="w-3 h-3 rounded-full bg-gray-300" />;
  }
};

interface ToolArgumentsProps {
  name: string;
  args: any;
}

export const ToolArguments: React.FC<ToolArgumentsProps> = ({ name, args }) => {
  const parsed = typeof args === 'string' ? JSON.parse(args) : args;
  
  const getArgumentText = () => {
    switch (name) {
      case 'search': return `Searching: "${parsed.query}"`;
      case 'plan': return 'Creating plan...';
      case 'reflect': return 'Reflecting...';
      case 'scrape': return `Reading: ${new URL(parsed.url).hostname}`;
      default: return JSON.stringify(parsed);
    }
  };

  return <span>{getArgumentText()}</span>;
};

interface ToolResultDisplayProps {
  name: string;
  result: any;
}

const formatTimeAgo = (timestamp: number): string => {
  const now = Date.now() / 1000;
  const diff = now - timestamp;
  
  if (diff < 60) return 'just now';
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
};

const formatNumber = (num: number): string => {
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}k`;
  return num.toString();
};

export const ToolResultDisplay: React.FC<ToolResultDisplayProps> = ({ name, result }) => {
  // Scrape tool result
  if (name === 'scrape') {
    const content = typeof result === 'string' ? result : result?.text || result?.content || String(result);
    const truncatedContent = content.slice(0, 500);
    
    return (
      <div className="mt-2 p-3 bg-green-50 border border-green-200 rounded-lg">
        <div className="flex items-center gap-2 mb-2">
          <Globe className="w-3 h-3 text-green-600" />
          <span className="text-xs font-medium text-green-800">Scraped Content</span>
        </div>
        <div className="text-xs text-green-900 leading-relaxed">
          {truncatedContent}
          {content.length > 500 && (
            <span className="text-green-600 font-medium">... (truncated)</span>
          )}
        </div>
      </div>
    );
  }

  // Search results
  if (name === 'search' && Array.isArray(result)) {
    return (
      <div className="mt-2 space-y-1">
        <div className="text-xs text-gray-500 mb-2">
          {result.length} results
        </div>
        {result.slice(0, 6).map((item, idx) => (
          <a
            key={idx}
            href={item.url}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 p-2 rounded hover:bg-gray-50 transition-colors group text-xs"
          >
            <img
              src={`https://www.google.com/s2/favicons?domain=${new URL(item.url).hostname}&sz=16`}
              alt=""
              className="w-3 h-3 flex-shrink-0"
              onError={e => { (e.currentTarget as HTMLImageElement).style.display = 'none'; }}
            />
            <div className="min-w-0 flex-1">
              <div className="font-medium truncate text-gray-900 group-hover:text-blue-600">
                {item.title}
              </div>
              <div className="text-gray-500 truncate">
                {new URL(item.url).hostname}
              </div>
            </div>
            <ExternalLink className="w-3 h-3 text-gray-400 opacity-0 group-hover:opacity-100 transition-opacity" />
          </a>
        ))}
      </div>
    );
  }

  // Plan results
  if (name === 'plan' && result?.overview && Array.isArray(result.steps)) {
    return (
      <div className="mt-2 space-y-2">
        <div className="text-xs text-gray-700">
          {result.overview}
        </div>
        <div className="space-y-1">
          {result.steps.map((step: string, idx: number) => (
            <div key={idx} className="flex gap-2 text-xs">
              <div className="w-4 h-4 mt-0.5 rounded-full bg-blue-100 flex items-center justify-center flex-shrink-0">
                <span className="text-xs font-medium text-blue-600">{idx + 1}</span>
              </div>
              <span className="text-gray-600">{step}</span>
            </div>
          ))}
        </div>
      </div>
    );
  }

  // Reflect results
  if (name === 'reflect') {
    return (
      <div className="mt-2 p-2 bg-amber-50 border border-amber-200 rounded text-xs">
        <div className="text-amber-900">{String(result)}</div>
      </div>
    );
  }

  // Fallback for unknown results
  return (
    <div className="mt-2 text-xs text-gray-600">
      {String(result).slice(0, 150)}...
    </div>
  );
}; 