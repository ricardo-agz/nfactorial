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
    case 'get_top_subreddit_posts':
    case 'get_posts_by_sort':
    case 'search_posts':
    case 'get_user_posts':
    case 'get_random_posts':
    case 'get_multireddit_posts':
      return <MessageCircle className="w-3 h-3 text-orange-600" />;
    case 'get_subreddit_info':
    case 'get_trending_subreddits':
    case 'search_subreddits':
      return <Users className="w-3 h-3 text-orange-600" />;
    case 'get_post_comments':
      return <MessageCircle className="w-3 h-3 text-orange-600" />;
    case 'get_user_info':
      return <User className="w-3 h-3 text-orange-600" />;
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
      case 'scrape': return `Scraping: ${new URL(parsed.url).hostname}`;
      case 'get_top_subreddit_posts': return `Top posts from r/${parsed.subreddit_name}`;
      case 'get_posts_by_sort': return `${parsed.sort_type} posts from r/${parsed.subreddit_name}`;
      case 'search_posts': return `Searching posts: "${parsed.query}"`;
      case 'get_user_posts': return `Posts by u/${parsed.username}`;
      case 'get_random_posts': return `Random posts from r/${parsed.subreddit_name}`;
      case 'get_multireddit_posts': return `Posts from ${parsed.subreddit_names?.join(', ')}`;
      case 'get_subreddit_info': return `Info for r/${parsed.subreddit_name}`;
      case 'get_trending_subreddits': return 'Finding trending subreddits...';
      case 'search_subreddits': return `Searching subreddits: "${parsed.query}"`;
      case 'get_post_comments': return 'Getting comments...';
      case 'get_user_info': return `Info for u/${parsed.username}`;
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

  // Search results (existing)
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

  // Reddit Posts (multiple tools return this)
  if ((name.includes('posts') || name === 'search_posts') && Array.isArray(result)) {
    return (
      <div className="mt-2 space-y-2">
        <div className="text-xs text-gray-500 mb-2">
          {result.length} posts
        </div>
        {result.slice(0, 5).map((post, idx) => (
          <div key={idx} className="p-3 bg-orange-50 border border-orange-200 rounded-lg">
            <div className="flex items-start gap-2 mb-2">
              <div className="flex items-center gap-1 text-xs text-orange-600">
                <ArrowUp className="w-3 h-3" />
                <span className="font-medium">{formatNumber(post.score)}</span>
              </div>
              <div className="flex-1 min-w-0">
                <a 
                  href={post.url} 
                  target="_blank" 
                  rel="noopener noreferrer"
                  className="font-medium text-xs text-gray-900 hover:text-orange-600 transition-colors line-clamp-2"
                >
                  {post.title}
                </a>
                <div className="flex items-center gap-2 mt-1 text-xs text-gray-500">
                  <span>u/{post.author}</span>
                  <span>•</span>
                  <span>{formatTimeAgo(post.created_utc)}</span>
                  <span>•</span>
                  <span className="flex items-center gap-1">
                    <MessageCircle className="w-3 h-3" />
                    {post.num_comments}
                  </span>
                </div>
              </div>
            </div>
            {post.content && (
              <div className="text-xs text-gray-700 mt-2 line-clamp-3">
                {post.content.slice(0, 200)}
                {post.content.length > 200 && '...'}
              </div>
            )}
          </div>
        ))}
      </div>
    );
  }

  // Subreddit Info
  if (name === 'get_subreddit_info' && result?.name) {
    return (
      <div className="mt-2 p-3 bg-orange-50 border border-orange-200 rounded-lg">
        <div className="flex items-center gap-2 mb-2">
          <Users className="w-4 h-4 text-orange-600" />
          <a 
            href={result.url} 
            target="_blank" 
            rel="noopener noreferrer"
            className="font-bold text-sm text-orange-800 hover:text-orange-600"
          >
            {result.display_name}
          </a>
          {result.over18 && (
            <span className="px-1 py-0.5 bg-red-100 text-red-600 text-xs rounded">18+</span>
          )}
        </div>
        <div className="text-xs text-gray-700 font-medium mb-1">{result.title}</div>
        <div className="text-xs text-gray-600 mb-2">{result.public_description}</div>
        <div className="flex items-center gap-3 text-xs text-gray-500">
          <span className="flex items-center gap-1">
            <Users className="w-3 h-3" />
            {formatNumber(result.subscribers)} members
          </span>
          <span className="flex items-center gap-1">
            <Clock className="w-3 h-3" />
            Created {formatTimeAgo(result.created_utc)}
          </span>
        </div>
      </div>
    );
  }

  // Trending/Search Subreddits
  if ((name === 'get_trending_subreddits' || name === 'search_subreddits') && Array.isArray(result)) {
    return (
      <div className="mt-2 space-y-2">
        <div className="text-xs text-gray-500 mb-2">
          {result.length} subreddits
        </div>
        {result.slice(0, 6).map((sub, idx) => (
          <div key={idx} className="p-2 bg-orange-50 border border-orange-200 rounded">
            <div className="flex items-center justify-between">
              <a 
                href={sub.url} 
                target="_blank" 
                rel="noopener noreferrer"
                className="font-medium text-xs text-orange-800 hover:text-orange-600"
              >
                {sub.display_name}
              </a>
              <div className="flex items-center gap-1 text-xs text-gray-500">
                <Users className="w-3 h-3" />
                {formatNumber(sub.subscribers)}
              </div>
            </div>
            <div className="text-xs text-gray-600 mt-1 line-clamp-2">
              {sub.public_description || sub.title}
            </div>
          </div>
        ))}
      </div>
    );
  }

  // Comments
  if (name === 'get_post_comments' && Array.isArray(result)) {
    return (
      <div className="mt-2 space-y-2">
        <div className="text-xs text-gray-500 mb-2">
          {result.length} comments
        </div>
        {result.slice(0, 4).map((comment, idx) => (
          <div key={idx} className="p-2 bg-orange-50 border border-orange-200 rounded">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-xs font-medium text-orange-800">u/{comment.author}</span>
              <div className="flex items-center gap-1 text-xs text-gray-500">
                <ArrowUp className="w-3 h-3" />
                {comment.score}
              </div>
              <span className="text-xs text-gray-500">{formatTimeAgo(comment.created_utc)}</span>
              {comment.is_submitter && (
                <span className="px-1 bg-blue-100 text-blue-600 text-xs rounded">OP</span>
              )}
            </div>
            <div className="text-xs text-gray-700 line-clamp-3">
              {comment.body.slice(0, 300)}
              {comment.body.length > 300 && '...'}
            </div>
          </div>
        ))}
      </div>
    );
  }

  // User Info
  if (name === 'get_user_info' && result?.name) {
    return (
      <div className="mt-2 p-3 bg-orange-50 border border-orange-200 rounded-lg">
        <div className="flex items-center gap-2 mb-2">
          <User className="w-4 h-4 text-orange-600" />
          <span className="font-bold text-sm text-orange-800">u/{result.name}</span>
          {result.is_verified && (
            <span className="px-1 py-0.5 bg-green-100 text-green-600 text-xs rounded">✓</span>
          )}
        </div>
        <div className="grid grid-cols-2 gap-3 text-xs">
          <div className="flex items-center gap-1 text-gray-600">
            <MessageCircle className="w-3 h-3" />
            <span>{formatNumber(result.comment_karma)} comment karma</span>
          </div>
          <div className="flex items-center gap-1 text-gray-600">
            <TrendingUp className="w-3 h-3" />
            <span>{formatNumber(result.link_karma)} post karma</span>
          </div>
          <div className="flex items-center gap-1 text-gray-600">
            <Clock className="w-3 h-3" />
            <span>Joined {formatTimeAgo(result.created_utc)}</span>
          </div>
          {result.has_verified_email && (
            <div className="flex items-center gap-1 text-green-600">
              <span>✓ Email verified</span>
            </div>
          )}
        </div>
      </div>
    );
  }

  // Plan results (existing)
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

  // Reflect results (existing)
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