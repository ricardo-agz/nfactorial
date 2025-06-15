// ============================================================================
// CONSTANTS AND CONFIGURATION
// ============================================================================

const CONFIG = {
  DEFAULT_REFRESH_INTERVAL: 10000, // 10 seconds
  CHART_BARS: 60,
  MIN_BAR_HEIGHT_RATIO: 0.15,
  LAST_REFRESH_UPDATE_INTERVAL: 30000 // 30 seconds
};

const COLORS = {
  task: {
    queued: '#3B82F6',
    processing: '#10B981', 
    backoff: '#F97316',
    completed: '#059669',
    failed: '#EF4444',
    cancelled: '#6B7280',
    pending: '#F59E0B',
  },
  activity: {
    completed: '#10B981',
    failed: '#EF4444', 
    cancelled: '#8B5CF6'
  }
};

const STATUS_CONFIG = {
  active: { label: 'Active', color: 'text-green-600', dot: 'bg-green-500' },
  busy: { label: 'Busy', color: 'text-blue-600', dot: 'bg-blue-500' },
  pending: { label: 'Pending', color: 'text-gray-600', dot: 'bg-gray-400' },
  ready: { label: 'Ready', color: 'text-green-600', dot: 'bg-green-400' },
  warning: { label: 'Warning', color: 'text-yellow-600', dot: 'bg-yellow-500' },
  offline: { label: 'Offline', color: 'text-red-600', dot: 'bg-red-500' },
  default: { label: 'Unknown', color: 'text-gray-600', dot: 'bg-gray-500' }
};

// ============================================================================
// STATE MANAGEMENT
// ============================================================================

const state = {
  refreshTimer: null,
  lastData: null,
  autoRefresh: true,
  charts: {},
  refreshInterval: CONFIG.DEFAULT_REFRESH_INTERVAL,
  lastRefreshTime: null,
  showIndividualAgents: {
    completed: false,
    failed: false,
    cancelled: false
  }
};

// ============================================================================
// DOM ELEMENTS
// ============================================================================

const elements = {
  statusBadge: document.getElementById('status-badge'),
  manualRefreshBtn: document.getElementById('manual-refresh-btn'),
  refreshRateBtn: document.getElementById('refresh-rate-btn'),
  refreshDropdown: document.getElementById('refresh-dropdown'),
  autoToggle: document.getElementById('auto-toggle'),
  mainContent: document.getElementById('main-content'),
  footer: document.querySelector('footer'),
  lastRefreshText: document.getElementById('last-refresh-text'),
  refreshRateDisplay: document.getElementById('refresh-rate-display')
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Safely creates Lucide icons without throwing errors
 * @returns {void}
 */
function safeCreateIcons() {
  try {
    if (typeof lucide !== 'undefined' && lucide.createIcons) {
      lucide.createIcons();
    }
  } catch (e) {
    console.warn('Failed to create icons:', e);
  }
}

/**
 * Formats a timestamp into a human-readable relative time string
 * @param {string|number|Date} timestamp - The timestamp to format
 * @returns {string} Formatted time string (e.g., "2m ago", "1h ago", "3d ago")
 */
function formatTime(timestamp) {
  if (!timestamp) return 'Never';
  const now = new Date();
  const time = new Date(timestamp);
  const diffMs = now - time;
  const diffMins = Math.floor(diffMs / 60000);
  
  if (diffMins < 1) return 'Just now';
  if (diffMins < 60) return `${diffMins}m ago`;
  
  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `${diffHours}h ago`;
  
  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays}d ago`;
}

/**
 * Formats a time interval in milliseconds to a readable string
 * @param {number} ms - Time interval in milliseconds
 * @returns {string} Formatted interval string (e.g., "30 s", "2 min")
 */
function formatInterval(ms) {
  return ms < 60000 ? `${ms / 1000} s` : `${ms / 60000} min`;
}

/**
 * Formats the last refresh time for display
 * @returns {string} Formatted last refresh string
 */
function formatLastRefresh() {
  if (!state.lastRefreshTime) return 'Last refresh: --';
  
  const diffMs = Date.now() - state.lastRefreshTime;
  const diffMins = Math.floor(diffMs / 60000);
  
  if (diffMs < 60000) return 'Last refresh: < 1 min';
  if (diffMins < 60) return `Last refresh: ${diffMins} min ago`;
  
  const diffHours = Math.floor(diffMins / 60);
  return `Last refresh: ${diffHours}h ago`;
}

/**
 * Formats a duration in seconds to a human-readable string
 * @param {number} seconds - Duration in seconds
 * @returns {string} Formatted duration string (e.g., "30s", "2.5m", "1.2h")
 */
function formatDuration(seconds) {
  if (seconds < 60) return `${seconds}s`;
  
  const minutes = seconds / 60;
  if (seconds < 3600) {
    return minutes % 1 === 0 ? `${minutes}m` : `${minutes.toFixed(1)}m`;
  }
  
  const hours = seconds / 3600;
  return hours % 1 === 0 ? `${hours}h` : `${hours.toFixed(1)}h`;
}

// ============================================================================
// AGENT STATUS FUNCTIONS
// ============================================================================

/**
 * Determines the status of an agent based on its current activity
 * Status priority: warning > active > busy > idle > ready > offline
 * @param {Object} agent - Agent data object
 * @param {number} agent.stale_tasks_count - Number of stale tasks
 * @param {number} agent.processing_count - Number of tasks currently processing
 * @param {number} agent.main_queue_size - Number of tasks in main queue
 * @param {number} agent.completed_count - Number of completed tasks
 * @param {number} agent.failed_count - Number of failed tasks
 * @returns {string} Agent status ('warning'|'active'|'busy'|'idle'|'ready'|'offline')
 */
function getAgentStatus(agent) {
  if (agent.stale_tasks_count > 0) return 'warning';
  if (agent.processing_count > 0) return 'active';
  if (agent.main_queue_size > 0) return 'busy';
  if (agent.completed_count > 0 || agent.failed_count > 0) return 'idle';
  return 'ready';
}

/**
 * Gets display information for a given agent status
 * @param {string} status - Agent status string 
 * @returns {Object} Status display configuration
 * @returns {string} returns.label - Human-readable status label
 * @returns {string} returns.color - CSS class for text color
 * @returns {string} returns.dot - CSS class for status indicator dot
 */
function getStatusInfo(status) {
  return STATUS_CONFIG[status] || STATUS_CONFIG.default;
}

// ============================================================================
// CHART MANAGEMENT
// ============================================================================

/**
 * Safely destroys a Chart.js instance and removes it from state
 * @param {string} chartId - Unique identifier for the chart
 * @returns {void}
 */
function destroyChart(chartId) {
  if (state.charts[chartId]) {
    state.charts[chartId].destroy();
    delete state.charts[chartId];
  }
}

/**
 * Creates or updates the task distribution doughnut chart
 * @param {Object} distribution - Task distribution data
 * @param {number} distribution.queued - Number of queued tasks
 * @param {number} distribution.processing - Number of processing tasks
 * @param {number} distribution.backoff - Number of tasks in backoff
 * @param {number} distribution.completed - Number of completed tasks
 * @param {number} distribution.failed - Number of failed tasks
 * @param {number} distribution.cancelled - Number of cancelled tasks
 * @param {number} distribution.pending_tool_results - Number of tasks pending tool results
 * @returns {void}
 */
function createTaskDistributionChart(distribution) {
  const ctx = document.getElementById('taskDistributionChart');
  if (!ctx) return;

  const totalTasks = Object.values(distribution).reduce((sum, count) => sum + count, 0);
  const chartContainer = ctx.parentElement;
  const legendContainer = document.getElementById('chart-legend');

  // Update legend
  updateTaskDistributionLegend(distribution, legendContainer);

  // Update or create chart
  if (state.charts.taskDistribution && !state.charts.taskDistribution.destroyed) {
    updateExistingTaskChart(distribution, totalTasks, chartContainer);
  } else {
    createNewTaskChart(ctx, distribution, totalTasks, chartContainer);
  }
}

/**
 * Updates the task distribution chart legend
 * @param {Object} distribution - Task distribution data
 * @param {HTMLElement} legendContainer - Legend container element
 * @returns {void}
 */
function updateTaskDistributionLegend(distribution, legendContainer) {
  if (!legendContainer) return;

  const legendData = [
    { label: 'Queued', color: COLORS.task.queued, value: distribution.queued },
    { label: 'Processing', color: COLORS.task.processing, value: distribution.processing },
    { label: 'Backoff', color: COLORS.task.backoff, value: distribution.backoff },
    { label: 'Completed', color: COLORS.task.completed, value: distribution.completed },
    { label: 'Failed', color: COLORS.task.failed, value: distribution.failed },
    { label: 'Cancelled', color: COLORS.task.cancelled, value: distribution.cancelled },
    { label: 'Pending', color: COLORS.task.pending, value: distribution.pending }
  ];

  legendContainer.innerHTML = `
    <div class="space-y-3">
      ${legendData.map(item => `
        <div class="flex items-center gap-3">
          <div class="w-3 h-3 rounded-full" style="background-color: ${item.color}"></div>
          <span class="text-sm text-gray-700 flex-1">${item.label}</span>
          <span class="text-sm font-medium text-gray-900">${item.value}</span>
        </div>
      `).join('')}
    </div>
  `;
}

/**
 * Updates an existing task distribution chart with new data
 * @param {Object} distribution - Task distribution data
 * @param {number} totalTasks - Total number of tasks
 * @param {HTMLElement} chartContainer - Chart container element
 * @returns {void}
 */
function updateExistingTaskChart(distribution, totalTasks, chartContainer) {
  const chart = state.charts.taskDistribution;
  
  if (totalTasks === 0) {
    // Show placeholder
    chart.data.datasets[0].data = [1, 0, 0, 0, 0, 0, 0];
    chart.data.datasets[0].backgroundColor = ['#E5E7EB', ...Object.values(COLORS.task).slice(1)];
    chart.options.plugins.legend.display = false;
    chart.options.plugins.tooltip.enabled = false;
    chart.update('none');
    
    toggleChartOverlay(chartContainer, true, 'No Tasks', 'System is idle');
  } else {
    // Show real data
    chart.data.datasets[0].data = [
      distribution.queued, distribution.processing, distribution.backoff,
      distribution.completed, distribution.failed, distribution.cancelled,
      distribution.pending
    ];
    chart.data.datasets[0].backgroundColor = Object.values(COLORS.task);
    chart.options.plugins.legend.display = false;
    chart.options.plugins.tooltip.enabled = true;
    chart.update('none');
    
    toggleChartOverlay(chartContainer, false);
  }
}

/**
 * Creates a new task distribution chart
 * @param {HTMLCanvasElement} ctx - Canvas context for the chart
 * @param {Object} distribution - Task distribution data
 * @param {number} totalTasks - Total number of tasks
 * @param {HTMLElement} chartContainer - Chart container element
 * @returns {void}
 */
function createNewTaskChart(ctx, distribution, totalTasks, chartContainer) {
  destroyChart('taskDistribution');

  const chartData = totalTasks === 0 
    ? [1, 0, 0, 0, 0, 0, 0]
    : [distribution.queued, distribution.processing, distribution.backoff,
       distribution.completed, distribution.failed, distribution.cancelled,
       distribution.pending];

  const backgroundColor = totalTasks === 0
    ? ['#E5E7EB', ...Object.values(COLORS.task).slice(1)]
    : Object.values(COLORS.task);

  state.charts.taskDistribution = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: ['Queued', 'Processing', 'Backoff', 'Completed', 'Failed', 'Cancelled', 'Pending'],
      datasets: [{
        data: chartData,
        backgroundColor,
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        legend: { display: false },
        tooltip: { enabled: totalTasks > 0 }
      },
      elements: {
        arc: { borderWidth: 2 }
      }
    }
  });

  if (totalTasks === 0) {
    toggleChartOverlay(chartContainer, true, 'No Tasks', 'System is idle');
  }
}

/**
 * Toggles chart overlay visibility for empty state display
 * @param {HTMLElement} container - Chart container element
 * @param {boolean} show - Whether to show the overlay
 * @param {string} [title=''] - Overlay title text
 * @param {string} [subtitle=''] - Overlay subtitle text
 * @returns {void}
 */
function toggleChartOverlay(container, show, title = '', subtitle = '') {
  let overlay = container.querySelector('.chart-overlay');
  
  if (show) {
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.className = 'chart-overlay absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 pointer-events-none';
      container.appendChild(overlay);
    }
    
    overlay.innerHTML = `
      <div class="text-center">
        <div class="text-sm font-normal text-gray-500">${title}</div>
        <div class="text-xs text-gray-400">${subtitle}</div>
      </div>
    `;
  } else if (overlay) {
    overlay.remove();
  }
}

/**
 * Creates or updates an activity chart (bar chart) for completed, failed, or cancelled tasks
 * @param {string} canvasId - ID of the canvas element
 * @param {Object} data - Activity chart data
 * @param {Array} data.buckets - Array of time buckets with task counts
 * @param {number} data.buckets[].timestamp - Bucket timestamp
 * @param {number} data.buckets[].count - Task count for this bucket
 * @param {string} color - Chart color (hex code)
 * @param {string} emptyMessage - Message to display when no data
 * @returns {Chart|undefined} Chart.js instance or undefined if creation failed
 */
function createActivityChart(canvasId, data, color, emptyMessage) {
  const ctx = document.getElementById(canvasId);
  if (!ctx || typeof Chart === 'undefined') return;

  // Only destroy and recreate if necessary
  const existingChart = state.charts[canvasId];
  
  // Get the bucket data and ensure we show the most recent activity
  let taskCounts = [];
  
  if (data?.buckets?.length > 0) {
    // Sort buckets by timestamp to ensure proper order
    const sortedBuckets = [...data.buckets].sort((a, b) => a.timestamp - b.timestamp);
    
    // Take the most recent buckets up to CHART_BARS count
    const recentBuckets = sortedBuckets.slice(-CONFIG.CHART_BARS);
    taskCounts = recentBuckets.map(bucket => bucket.count);
    
    // Pad with zeros if we have fewer buckets than chart bars
    while (taskCounts.length < CONFIG.CHART_BARS) {
      taskCounts.unshift(0); // Add zeros at the beginning for older time periods
    }
  } else {
    taskCounts = Array(CONFIG.CHART_BARS).fill(0);
  }

  const paddedTaskCounts = taskCounts.slice(-CONFIG.CHART_BARS); // Ensure exactly CHART_BARS elements

  // If chart exists and data structure is the same, just update data
  if (existingChart && !existingChart.destroyed && existingChart.data.datasets[0].data.length === paddedTaskCounts.length) {
    const maxTaskCount = Math.max(...paddedTaskCounts);
    const effectiveMaxTaskCount = Math.max(maxTaskCount, 10);
    const minHeight = effectiveMaxTaskCount * CONFIG.MIN_BAR_HEIGHT_RATIO;
    const maxHeight = effectiveMaxTaskCount;
    
    const barData = paddedTaskCounts.map(count => 
      count === 0 ? minHeight : minHeight + (count / maxHeight) * maxHeight
    );
    const barColors = paddedTaskCounts.map(count => count > 0 ? color : '#F3F4F6');
    
    // Update existing chart data
    existingChart.data.datasets[0].data = barData;
    existingChart.data.datasets[0].backgroundColor = barColors;
    existingChart.options.scales.y.max = maxHeight + minHeight;
    existingChart.update('none'); // Use 'none' mode for fastest update
    return existingChart;
  }

  // Create new chart only if necessary
  destroyChart(canvasId);

  const maxTaskCount = Math.max(...paddedTaskCounts);
  const effectiveMaxTaskCount = Math.max(maxTaskCount, 10);
  const minHeight = effectiveMaxTaskCount * CONFIG.MIN_BAR_HEIGHT_RATIO;
  const maxHeight = effectiveMaxTaskCount;
  const totalScale = maxHeight + minHeight;

  const barData = paddedTaskCounts.map(count => 
    count === 0 ? minHeight : minHeight + (count / maxHeight) * maxHeight
  );
  
  const barColors = paddedTaskCounts.map(count => count > 0 ? color : '#F3F4F6');

  try {
    state.charts[canvasId] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: Array(CONFIG.CHART_BARS).fill(''),
        datasets: [{
          data: barData,
          backgroundColor: barColors,
          borderWidth: 0
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false, // Disable animations for better performance
        plugins: {
          legend: { display: false },
          tooltip: { 
            enabled: true,
            mode: 'index',
            intersect: false,
            callbacks: {
              title: () => '',
              label: (context) => {
                const count = paddedTaskCounts[context.dataIndex];
                return count > 0 ? (count === 1 ? '1 task' : `${count} tasks`) : null;
              }
            },
            filter: (tooltipItem) => paddedTaskCounts[tooltipItem.dataIndex] > 0
          }
        },
        scales: {
          x: { display: false, grid: { display: false } },
          y: { 
            display: false, 
            beginAtZero: true, 
            min: 0, 
            max: totalScale, 
            grid: { display: false } 
          }
        },
        layout: { padding: 0 },
        elements: {
          bar: {
            categoryPercentage: 1.0,
            barPercentage: 0.8
          }
        }
      }
    });
  } catch (error) {
    console.error(`Error creating chart for ${canvasId}:`, error);
  }

  return state.charts[canvasId];
}

/**
 * Updates all activity charts (completed, failed, cancelled)
 * @param {Object} activityCharts - Activity charts data
 * @param {Object} activityCharts.completed - Completed tasks chart data
 * @param {Object} activityCharts.failed - Failed tasks chart data
 * @param {Object} activityCharts.cancelled - Cancelled tasks chart data
 * @returns {void}
 */
function updateActivityCharts(activityCharts) {
  if (!activityCharts) return;

  // Create/update charts
  createActivityChart('completedChart', activityCharts.completed, COLORS.activity.completed, 'No completed tasks');
  createActivityChart('failedChart', activityCharts.failed, COLORS.activity.failed, 'No failed tasks');
  createActivityChart('cancelledChart', activityCharts.cancelled, COLORS.activity.cancelled, 'No cancelled tasks');

  // Update summaries
  updateActivitySummary('completed', activityCharts.completed);
  updateActivitySummary('failed', activityCharts.failed);
  updateActivitySummary('cancelled', activityCharts.cancelled);
}

/**
 * Updates the activity summary text for a specific activity type
 * @param {string} type - Activity type ('completed'|'failed'|'cancelled')
 * @param {Object} data - Activity data
 * @param {number} data.total_tasks - Total number of tasks
 * @param {number} data.ttl_seconds - Time-to-live in seconds
 * @returns {void}
 */
function updateActivitySummary(type, data) {
  const summaryElement = document.getElementById(`${type}-summary`);
  if (summaryElement) {
    summaryElement.textContent = `${data.total_tasks} tasks over the last ${formatDuration(data.ttl_seconds)}`;
  }
}

/**
 * Creates individual agent activity charts for a specific activity type
 * @param {Object} agentActivityCharts - Agent-specific activity chart data
 * @param {string} activityType - Type of activity ('completed'|'failed'|'cancelled')
 * @returns {void}
 */
function createIndividualAgentCharts(agentActivityCharts, activityType) {
  if (!agentActivityCharts || !state.showIndividualAgents[activityType]) return;

  const containerSelector = `individual-${activityType}-charts`;
  const agentChartsContainer = document.getElementById(containerSelector);
  if (!agentChartsContainer) return;

  // Clean up existing charts
  cleanupExistingAgentCharts(agentChartsContainer);

  // Add "All" chart first
  addAggregateChart(agentChartsContainer, activityType);

  // Create charts for each individual agent
  Object.entries(agentActivityCharts).forEach(([agentName, agentCharts]) => {
    addIndividualAgentChart(agentChartsContainer, agentName, agentCharts[activityType], activityType);
  });
  
  // Refresh icons for the new content
  safeCreateIcons();
}

/**
 * Cleans up existing agent charts and their DOM elements
 * @param {HTMLElement} container - Container element holding the charts
 * @returns {void}
 */
function cleanupExistingAgentCharts(container) {
  const existingCanvases = container.querySelectorAll('canvas');
  existingCanvases.forEach(canvas => {
    destroyChart(canvas.id);
  });
  container.innerHTML = '';
}

/**
 * Adds an aggregate "All" chart to the individual charts container
 * @param {HTMLElement} container - Container element
 * @param {string} activityType - Type of activity
 * @returns {void}
 */
function addAggregateChart(container, activityType) {
  const aggregateData = state.lastData?.activity_charts?.[activityType];
  const totalTasks = aggregateData?.total_tasks || 0;
  
  const allSection = document.createElement('div');
  allSection.className = 'space-y-3';
  allSection.innerHTML = createAgentChartHTML('All', totalTasks, `all-${activityType}`);
  container.appendChild(allSection);
  
  createActivityChart(`all-${activityType}`, aggregateData, COLORS.activity[activityType], `No ${activityType} tasks`);
}

/**
 * Adds an individual agent chart to the container
 * @param {HTMLElement} container - Container element
 * @param {string} agentName - Name of the agent
 * @param {Object} activityData - Activity data for this agent
 * @param {string} activityType - Type of activity
 * @returns {void}
 */
function addIndividualAgentChart(container, agentName, activityData, activityType) {
  const agentSection = document.createElement('div');
  agentSection.className = 'space-y-3';
  agentSection.innerHTML = createAgentChartHTML(agentName, activityData.total_tasks, `agent-${agentName}-${activityType}`);
  container.appendChild(agentSection);
  
  createActivityChart(`agent-${agentName}-${activityType}`, activityData, COLORS.activity[activityType], `No ${activityType} tasks`);
}

/**
 * Creates HTML for an individual agent chart section
 * @param {string} name - Agent name or "All"
 * @param {number} totalTasks - Total number of tasks
 * @param {string} canvasId - Canvas element ID
 * @returns {string} HTML string for the chart section
 */
function createAgentChartHTML(name, totalTasks, canvasId) {
  return `
    <div class="flex items-center justify-between">
      <div class="flex items-center gap-2">
        <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">${name}</span>
      </div>
      <div class="text-xs text-gray-600">${totalTasks} tasks</div>
    </div>
    <div class="h-8 chart-container">
      <canvas id="${canvasId}" class="w-full h-full"></canvas>
    </div>
  `;
}

/**
 * Toggles the visibility of individual agent charts for a specific activity type
 * @param {string} activityType - Type of activity ('completed'|'failed'|'cancelled')
 * @returns {void}
 */
function toggleIndividualAgents(activityType) {
  state.showIndividualAgents[activityType] = !state.showIndividualAgents[activityType];
  
  const toggleBtn = document.getElementById(`toggle-${activityType}-agents`);
  const toggleIcon = toggleBtn?.querySelector('[data-lucide]');
  const individualChartsContainer = document.getElementById(`individual-${activityType}-charts`);
  const mainChartContainer = document.querySelector(`#${activityType}Chart`)?.parentElement;
  
  if (!toggleIcon || !individualChartsContainer || !mainChartContainer) return;
  
  // Update icon
  toggleIcon.setAttribute('data-lucide', state.showIndividualAgents[activityType] ? 'chevron-up' : 'chevron-down');
  
  if (state.showIndividualAgents[activityType]) {
    // Show individual charts
    if (state.lastData?.agent_activity_charts) {
      createIndividualAgentCharts(state.lastData.agent_activity_charts, activityType);
    }
    
    mainChartContainer.style.display = 'none';
    individualChartsContainer.style.display = 'block';
    individualChartsContainer.style.opacity = '1';
    individualChartsContainer.style.transform = 'translateY(0)';
  } else {
    // Hide individual charts
    individualChartsContainer.style.opacity = '0';
    individualChartsContainer.style.transform = 'translateY(-10px)';
    mainChartContainer.style.display = 'block';
    
    setTimeout(() => {
      if (!state.showIndividualAgents[activityType]) {
        individualChartsContainer.style.display = 'none';
      }
    }, 200);
  }
  
  // Update only the specific icon
  if (typeof lucide !== 'undefined' && lucide.createIcons) {
    lucide.createIcons({ nameAttr: 'data-lucide', attrs: { 'data-lucide': toggleIcon.getAttribute('data-lucide') } });
  }
}

// ============================================================================
// UI UPDATE FUNCTIONS
// ============================================================================

/**
 * Updates the last refresh time display in the UI
 * @returns {void}
 */
function updateLastRefreshDisplay() {
  if (elements.lastRefreshText) {
    elements.lastRefreshText.textContent = formatLastRefresh();
  }
}

/**
 * Updates the refresh rate display in the UI
 * @returns {void}
 */
function updateRefreshRateDisplay() {
  if (elements.refreshRateDisplay) {
    elements.refreshRateDisplay.textContent = formatInterval(state.refreshInterval);
  }
}

/**
 * Updates the system status badge based on current system health
 * @param {Object} data - Dashboard data
 * @param {Object} data.system - System information
 * @param {boolean} data.system.redis_connected - Redis connection status
 * @param {Array} data.queues - Array of agent queue information
 * @returns {void}
 */
function updateStatusBadge(data) {
  const { system, queues } = data;
  const hasIssues = queues.some(q => getAgentStatus(q) === 'warning' || getAgentStatus(q) === 'offline');
  const isHealthy = system.redis_connected && !hasIssues;
  
  const statusClass = isHealthy ? 'text-green-600' : hasIssues ? 'text-yellow-600' : 'text-red-600';
  const dotClass = isHealthy ? 'bg-green-500' : hasIssues ? 'bg-yellow-500' : 'bg-red-500';
  const statusText = isHealthy ? 'All systems operational' : hasIssues ? 'Some agents need attention' : 'System issues detected';
  
  elements.statusBadge.className = `flex items-center gap-2 text-sm font-medium ${statusClass}`;
  elements.statusBadge.querySelector('.w-2').className = `w-2 h-2 rounded-full ${dotClass}`;
  elements.statusBadge.querySelector('span').textContent = statusText;
}

/**
 * Updates the overview cards with system metrics
 * @param {Object} system - System metrics data
 * @param {number} system.total_agents - Total number of agents
 * @param {number} system.total_workers - Total number of workers
 * @param {number} system.total_tasks_queued - Total queued tasks
 * @param {number} system.total_tasks_processing - Total processing tasks
 * @param {number} system.system_throughput_per_minute - System throughput per minute
 * @param {number} system.overall_success_rate_percent - Overall success rate percentage
 * @param {string} system.activity_window_display - Activity window display string
 * @returns {void}
 */
function updateOverviewCards(system) {
  const overviewCards = document.getElementById('overview-cards');
  if (!overviewCards) return;

  overviewCards.innerHTML = `
    <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
      <div>
        <div class="text-xl font-semibold text-gray-900">${system.total_agents}</div>
        <div class="text-xs text-gray-600">Active Agents</div>
      </div>
      <i data-lucide="cpu" class="w-4 h-4 text-blue-500 absolute top-4 right-4"></i>
      <div class="mt-2 text-xs text-gray-500">${system.total_workers} workers running</div>
    </div>

    <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
      <div>
        <div class="text-xl font-semibold text-gray-900">${system.total_tasks_queued}</div>
        <div class="text-xs text-gray-600">Tasks Queued</div>
      </div>
      <i data-lucide="clock" class="w-4 h-4 text-yellow-500 absolute top-4 right-4"></i>
      <div class="mt-2 text-xs text-gray-500">${system.total_tasks_processing} currently processing</div>
    </div>

    <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
      <div>
        <div class="text-xl font-semibold text-gray-900">${system.system_throughput_per_minute.toFixed(1)}</div>
        <div class="text-xs text-gray-600">Tasks/Min</div>
      </div>
      <i data-lucide="zap" class="w-4 h-4 text-green-500 absolute top-4 right-4"></i>
      <div class="mt-2 text-xs text-gray-500">recent average throughput</div>
    </div>

    <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
      <div>
        <div class="text-xl font-semibold text-gray-900">${system.overall_success_rate_percent.toFixed(1)}%</div>
        <div class="text-xs text-gray-600">Success Rate</div>
      </div>
      <i data-lucide="check-circle" class="w-4 h-4 text-green-500 absolute top-4 right-4"></i>
      <div class="mt-2 text-xs text-gray-500">${system.activity_window_display}</div>
    </div>
  `;
  
  // Refresh icons for the new content
  safeCreateIcons();
}

/**
 * Updates the agent performance table with current data
 * @param {Array} queues - Array of agent queue data
 * @param {Array} performance - Array of agent performance data
 * @returns {void}
 */
function updateAgentTable(queues, performance) {
  const agentTable = document.getElementById('agent-table');
  if (!agentTable) return;

  // Check if we can update existing rows instead of full re-render
  const existingRows = agentTable.querySelectorAll('tbody tr:not(.animate-pulse)');
  const canUpdate = existingRows.length === queues.length && existingRows.length > 0;

  if (canUpdate) {
    // Update existing rows in place for better performance
    existingRows.forEach((row, index) => {
      const agent = queues[index];
      const perf = performance[index] || {};
      const status = getAgentStatus(agent);
      const statusInfo = getStatusInfo(status);
      
      const cells = row.querySelectorAll('td');
      if (cells.length >= 9) {
        // Update only the dynamic content
        cells[0].innerHTML = `<div class="text-sm font-medium text-gray-900">${agent.agent_name}</div>`;
        cells[1].innerHTML = `
          <div class="flex items-center gap-2">
            <div class="w-2 h-2 rounded-full ${statusInfo.dot}"></div>
            <span class="text-sm ${statusInfo.color}">${statusInfo.label}</span>
            ${agent.stale_tasks_count > 0 ? `<span class="text-xs text-yellow-600">(${agent.stale_tasks_count} stale)</span>` : ''}
          </div>
        `;
        cells[2].textContent = agent.main_queue_size;
        cells[3].textContent = agent.processing_count;
        cells[4].textContent = agent.backoff_count || 0;
        cells[5].textContent = `${(perf.current_throughput || 0).toFixed(1)}/min`;
        cells[6].textContent = `${(perf.success_rate_percent || 0).toFixed(1)}%`;
        cells[7].textContent = `${(perf.avg_processing_time_seconds || 0).toFixed(1)}s`;
        cells[8].textContent = formatTime(perf.last_activity);
      }
    });
    return;
  }

  // Full re-render only when necessary (different number of agents or transitioning from skeleton)
  const fragment = document.createDocumentFragment();
  
  // Create header
  const headerDiv = document.createElement('div');
  headerDiv.className = 'px-6 py-4 border-b border-gray-200';
  headerDiv.innerHTML = '<h3 class="text-lg font-medium text-gray-900">Agents</h3>';
  fragment.appendChild(headerDiv);
  
  // Create table container
  const tableContainer = document.createElement('div');
  tableContainer.className = 'overflow-x-auto';
  
  const table = document.createElement('table');
  table.className = 'w-full';
  
  // Create table header
  const thead = document.createElement('thead');
  thead.className = 'bg-gray-50';
  thead.innerHTML = `
    <tr>
      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Agent</th>
      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
      <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Queue</th>
      <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Processing</th>
      <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Backoff</th>
      <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Throughput</th>
      <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Success Rate</th>
      <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Avg Time</th>
      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Last Active</th>
    </tr>
  `;
  
  // Create table body
  const tbody = document.createElement('tbody');
  tbody.className = 'bg-white divide-y divide-gray-200';
  
  queues.forEach((agent, index) => {
    const status = getAgentStatus(agent);
    const statusInfo = getStatusInfo(status);
    const perf = performance[index] || {};
    
    const row = document.createElement('tr');
    row.className = 'hover:bg-gray-50';
    row.innerHTML = `
      <td class="px-6 py-4 whitespace-nowrap">
        <div class="text-sm font-medium text-gray-900">${agent.agent_name}</div>
      </td>
      <td class="px-6 py-4 whitespace-nowrap">
        <div class="flex items-center gap-2">
          <div class="w-2 h-2 rounded-full ${statusInfo.dot}"></div>
          <span class="text-sm ${statusInfo.color}">${statusInfo.label}</span>
          ${agent.stale_tasks_count > 0 ? `<span class="text-xs text-yellow-600">(${agent.stale_tasks_count} stale)</span>` : ''}
        </div>
      </td>
      <td class="px-6 py-4 whitespace-nowrap text-center text-sm text-gray-900">${agent.main_queue_size}</td>
      <td class="px-6 py-4 whitespace-nowrap text-center text-sm text-gray-900">${agent.processing_count}</td>
      <td class="px-6 py-4 whitespace-nowrap text-center text-sm text-gray-900">${agent.backoff_count || 0}</td>
      <td class="px-6 py-4 whitespace-nowrap text-center text-sm text-gray-900">${(perf.current_throughput || 0).toFixed(1)}/min</td>
      <td class="px-6 py-4 whitespace-nowrap text-center text-sm text-gray-900">${(perf.success_rate_percent || 0).toFixed(1)}%</td>
      <td class="px-6 py-4 whitespace-nowrap text-center text-sm text-gray-900">${(perf.avg_processing_time_seconds || 0).toFixed(1)}s</td>
      <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${formatTime(perf.last_activity)}</td>
    `;
    tbody.appendChild(row);
  });
  
  table.appendChild(thead);
  table.appendChild(tbody);
  tableContainer.appendChild(table);
  fragment.appendChild(tableContainer);
  
  // Replace content efficiently
  agentTable.innerHTML = '';
  agentTable.appendChild(fragment);
}

/**
 * Updates the state of toggle buttons for individual agent charts
 * @returns {void}
 */
function updateToggleButtonStates() {
  ['completed', 'failed', 'cancelled'].forEach(activityType => {
    const toggleBtn = document.getElementById(`toggle-${activityType}-agents`);
    const toggleIcon = toggleBtn?.querySelector('[data-lucide]');
    const mainChartContainer = document.querySelector(`#${activityType}Chart`)?.parentElement;
    const individualChartsContainer = document.getElementById(`individual-${activityType}-charts`);
    
    if (toggleIcon) {
      toggleIcon.setAttribute('data-lucide', state.showIndividualAgents[activityType] ? 'chevron-up' : 'chevron-down');
    }
    
    if (mainChartContainer && individualChartsContainer) {
      if (state.showIndividualAgents[activityType]) {
        mainChartContainer.style.display = 'none';
        individualChartsContainer.style.display = 'block';
        individualChartsContainer.style.opacity = '1';
        individualChartsContainer.style.transform = 'translateY(0)';
      } else {
        mainChartContainer.style.display = 'block';
        individualChartsContainer.style.display = 'none';
      }
    }
  });
  
  // Refresh icons after updating attributes
  safeCreateIcons();
}

// ============================================================================
// DASHBOARD LAYOUT TEMPLATES
// ============================================================================

/**
 * Creates the main dashboard layout HTML structure
 * @returns {string} HTML string for the complete dashboard layout
 */
function createDashboardLayout() {
  return `
    <div class="w-full space-y-8">
      <!-- Top Row: System Overview Cards (2x2 grid) + Task Distribution Chart -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <!-- System Overview Cards (2x2 grid) -->
        <div id="overview-cards" class="grid grid-cols-2 gap-4">
          <!-- Cards will be updated separately -->
        </div>

        <!-- Task Distribution Chart -->
        <div class="bg-white border border-gray-200 rounded-lg p-6">
          <h3 class="text-lg font-medium text-gray-900 mb-4">Task Distribution</h3>
          <div class="flex items-center gap-6">
            <div class="relative w-64 h-64 flex-shrink-0">
              <canvas id="taskDistributionChart" class="w-full h-full"></canvas>
            </div>
            <div id="chart-legend" class="flex-1 ml-3">
              <!-- Legend will be populated by JavaScript -->
            </div>
          </div>
        </div>
      </div>

      <!-- Activity Charts -->
      <div class="bg-white border border-gray-200 rounded-lg overflow-hidden">
        <div class="p-6 space-y-6">
          ${createActivityChartSection('completed', 'check-circle', 'text-green-500', 'Completed Tasks')}
          ${createActivityChartSection('failed', 'x-circle', 'text-red-500', 'Failed Tasks')}
          ${createActivityChartSection('cancelled', 'ban', 'text-purple-500', 'Cancelled Tasks')}
        </div>
      </div>

      <!-- Agent Performance Table -->
      <div id="agent-table" class="bg-white border border-gray-200 rounded-lg overflow-hidden">
        <!-- Table will be updated separately -->
      </div>
    </div>
  `;
}

/**
 * Creates HTML for an activity chart section
 * @param {string} type - Activity type ('completed'|'failed'|'cancelled')
 * @param {string} icon - Lucide icon name
 * @param {string} iconColor - CSS class for icon color
 * @param {string} title - Section title
 * @returns {string} HTML string for the activity chart section
 */
function createActivityChartSection(type, icon, iconColor, title) {
  return `
    <div class="space-y-3">
      <div class="flex items-center justify-between">
        <div class="flex items-center gap-2">
          <i data-lucide="${icon}" class="w-4 h-4 ${iconColor}"></i>
          <h4 class="text-sm font-medium text-gray-900">${title}</h4>
          <button id="toggle-${type}-agents" class="flex items-center gap-1 px-2 py-1 text-xs text-gray-500 hover:text-gray-700 hover:bg-gray-50 rounded transition-colors">
            <i data-lucide="chevron-down" class="w-3 h-3"></i>
          </button>
        </div>
        <div id="${type}-summary" class="text-xs text-gray-600">
          <!-- Summary will be populated by JavaScript -->
        </div>
      </div>
      <div class="h-8 chart-container">
        <canvas id="${type}Chart" class="w-full h-full"></canvas>
      </div>
      <!-- Individual Agent Charts (hidden by default) -->
      <div id="individual-${type}-charts" class="space-y-3" style="display: none;">
        <!-- Individual agent charts will be populated by JavaScript -->
      </div>
    </div>
  `;
}

// ============================================================================
// MAIN DASHBOARD RENDERING
// ============================================================================

/**
 * Main function to render the entire dashboard with provided data
 * @param {Object} data - Complete dashboard data
 * @param {Object} data.system - System metrics and information
 * @param {Array} data.queues - Agent queue information
 * @param {Array} data.performance - Agent performance metrics
 * @param {Object} data.task_distribution - Task distribution data
 * @param {Object} data.activity_charts - Activity chart data
 * @param {Object} data.agent_activity_charts - Individual agent activity data
 * @returns {void}
 */
function renderDashboard(data) {
  const { system, queues, performance, task_distribution, activity_charts, agent_activity_charts } = data;
  
  // Update status badge immediately (lightweight)
  updateStatusBadge(data);

  // Check if this is the first render
  const isFirstRender = !elements.mainContent.querySelector('.w-full');
  
  if (isFirstRender) {
    elements.mainContent.innerHTML = createDashboardLayout();
    setupActivityToggleListeners();
  }

  // Batch the heavy operations using requestAnimationFrame
  const renderTasks = [
    () => updateOverviewCards(system),
    () => createTaskDistributionChart(task_distribution),
    () => updateActivityCharts(activity_charts),
    () => {
      // Update individual agent charts for any that are visible
      if (agent_activity_charts) {
        ['completed', 'failed', 'cancelled'].forEach(activityType => {
          if (state.showIndividualAgents[activityType]) {
            createIndividualAgentCharts(agent_activity_charts, activityType);
          }
        });
      }
    },
    () => updateToggleButtonStates(),
    () => updateAgentTable(queues, performance),
    () => {
      // Only refresh icons on first render when new DOM elements are created
      if (isFirstRender) {
        safeCreateIcons();
      }
      state.lastData = data;
    }
  ];

  // Execute render tasks in batches to prevent blocking
  executeBatchedRender(renderTasks);
}

/**
 * Executes render tasks in batches to prevent UI blocking
 * @param {Array<Function>} tasks - Array of render task functions
 * @returns {void}
 */
function executeBatchedRender(tasks) {
  if (tasks.length === 0) return;
  
  const batchSize = 2;
  const currentBatch = tasks.splice(0, batchSize);
  
  currentBatch.forEach(task => {
    try {
      task();
    } catch (error) {
      console.warn('Render task failed:', error);
    }
  });
  
  if (tasks.length > 0) {
    requestAnimationFrame(() => executeBatchedRender(tasks));
  }
}

/**
 * Sets up event listeners for activity chart toggle buttons
 * @returns {void}
 */
function setupActivityToggleListeners() {
  ['completed', 'failed', 'cancelled'].forEach(activityType => {
    const toggleBtn = document.getElementById(`toggle-${activityType}-agents`);
    if (toggleBtn) {
      toggleBtn.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        toggleIndividualAgents(activityType);
      });
    }
  });
}

// ============================================================================
// ERROR HANDLING
// ============================================================================

/**
 * Analyzes an error and returns appropriate error information for display
 * @param {Error} error - The error object to analyze
 * @returns {Object} Error information object
 * @returns {string} returns.title - Error title for display
 * @returns {string} returns.message - Error message for display
 * @returns {string} returns.details - Additional error details
 */
function getErrorInfo(error) {
  const errorTypes = {
    'HTTP 500': {
      title: 'Server Error',
      message: 'The dashboard service encountered an internal error',
      details: 'This might be due to Redis connectivity issues or service problems.'
    },
    'HTTP 404': {
      title: 'Service Not Found',
      message: 'The dashboard API endpoint is not available',
      details: 'Please check if the observability service is properly configured.'
    },
    'HTTP 503': {
      title: 'Service Unavailable',
      message: 'The dashboard service is temporarily unavailable',
      details: 'The service may be starting up or experiencing high load.'
    }
  };

  // Check for specific HTTP errors
  for (const [errorCode, info] of Object.entries(errorTypes)) {
    if (error.message.includes(errorCode)) {
      return info;
    }
  }

  // Check for network errors
  if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
    return {
      title: 'Network Error',
      message: 'Unable to reach the dashboard service',
      details: 'Please check your network connection and ensure the service is running.'
    };
  }
  
  // Default error
  return {
    title: 'Connection Error',
    message: 'Unable to connect to the dashboard service',
    details: ''
  };
}

/**
 * Creates HTML for error display UI
 * @param {Object} errorInfo - Error information object from getErrorInfo
 * @param {string} errorInfo.title - Error title
 * @param {string} errorInfo.message - Error message
 * @param {string} errorInfo.details - Error details
 * @returns {string} HTML string for error display
 */
function createErrorDisplay(errorInfo) {
  return `
    <div class="max-w-lg mx-auto">
      <!-- Error Card -->
      <div class="bg-white border border-gray-200 rounded-lg shadow-sm overflow-hidden">
        <!-- Header -->
        <div class="px-6 py-6">
          <div class="flex items-center gap-3 mb-4">
            <div class="flex-shrink-0">
              <i data-lucide="wifi-off" class="w-6 h-6 text-red-500"></i>
            </div>
            <div>
              <h3 class="text-lg font-medium text-gray-900">${errorInfo.title}</h3>
              <p class="text-sm text-gray-600">${errorInfo.message}</p>
            </div>
          </div>
          
          ${errorInfo.details ? `
            <p class="text-sm text-gray-500 mb-4">${errorInfo.details}</p>
          ` : ''}
          
          <!-- Action Button -->
          <button id="retry-connection" class="flex items-center gap-2 px-4 py-2 bg-red-600 text-white text-sm font-medium rounded-lg hover:bg-red-700 transition-colors">
            <i data-lucide="refresh-cw" class="w-4 h-4"></i>
            Retry
          </button>
        </div>
      </div>
      
      <!-- Status Info -->
      <div class="mt-4 text-center text-xs text-gray-500">
        ${state.autoRefresh ? 'Auto-refresh will retry automatically' : 'Manual refresh required'}
      </div>
    </div>
  `;
}

/**
 * Sets up event handlers for error display elements
 * @returns {void}
 */
function setupErrorHandlers() {
  const retryBtn = document.getElementById('retry-connection');
  
  if (retryBtn) {
    retryBtn.addEventListener('click', fetchMetrics);
  }
}

// ============================================================================
// DATA FETCHING
// ============================================================================

/**
 * Fetches metrics data from the API and updates the dashboard
 * Handles loading states, error states, and successful data rendering
 * @async
 * @returns {Promise<void>}
 * @throws {Error} When API request fails or returns invalid data
 */
async function fetchMetrics() {
  // Show loading state
  elements.manualRefreshBtn.classList.add('opacity-70', 'pointer-events-none');
  elements.manualRefreshBtn.querySelector('.w-4').classList.add('spin');
  
  try {
    const response = await fetch('/observability/api/metrics');
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    
    const data = await response.json();
    renderDashboard(data);
    
    // Update last refresh time
    state.lastRefreshTime = new Date();
    updateLastRefreshDisplay();
    
    elements.footer.querySelector('div').textContent = 
      `Last updated ${new Date(data.timestamp).toLocaleTimeString()} • ${state.autoRefresh ? 'Auto refresh enabled' : 'Manual refresh'}`;
      
  } catch (error) {
    console.error('Failed to fetch metrics:', error);
    handleFetchError(error);
  } finally {
    // Remove loading state
    elements.manualRefreshBtn.classList.remove('opacity-70', 'pointer-events-none');
    elements.manualRefreshBtn.querySelector('.w-4').classList.remove('spin');
  }
}

/**
 * Handles fetch errors by updating UI to show error state
 * @param {Error} error - The error that occurred during fetch
 * @returns {void}
 */
function handleFetchError(error) {
  // Update status badge to show error
  elements.statusBadge.className = 'flex items-center gap-2 text-sm font-medium text-red-600';
  elements.statusBadge.querySelector('.w-2').className = 'w-2 h-2 rounded-full bg-red-500';
  elements.statusBadge.querySelector('span').textContent = 'Connection Error';
  
  // Show error display
  const errorInfo = getErrorInfo(error);
  elements.mainContent.innerHTML = createErrorDisplay(errorInfo);
  
  safeCreateIcons();
  setupErrorHandlers();
  
  // Update footer
  elements.footer.querySelector('div').textContent = 
    `Connection failed • ${state.autoRefresh ? 'Auto refresh will continue trying' : 'Manual refresh required'}`;
}

// ============================================================================
// REFRESH MANAGEMENT
// ============================================================================

/**
 * Sets up or clears the auto-refresh timer based on current state
 * @returns {void}
 */
function setupAutoRefresh() {
  clearInterval(state.refreshTimer);
  if (state.autoRefresh) {
    state.refreshTimer = setInterval(fetchMetrics, state.refreshInterval);
  }
}

/**
 * Handles refresh rate changes from the UI dropdown
 * @param {number} newInterval - New refresh interval in milliseconds
 * @returns {void}
 */
function handleRefreshRateChange(newInterval) {
  state.refreshInterval = newInterval;
  updateRefreshRateDisplay();
  setupAutoRefresh();
}

/**
 * Toggles auto-refresh on/off and updates the UI toggle switch
 * @returns {void}
 */
function toggleAutoRefresh() {
  state.autoRefresh = !state.autoRefresh;
  
  const toggleElement = elements.autoToggle.querySelector('div');
  if (state.autoRefresh) {
    elements.autoToggle.className = 'w-10 h-6 bg-blue-500 rounded-full relative cursor-pointer transition-all duration-200 shadow-sm';
    toggleElement.className = 'absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-all duration-200 translate-x-4 shadow-sm';
  } else {
    elements.autoToggle.className = 'w-10 h-6 bg-gray-300 rounded-full relative cursor-pointer transition-all duration-200 shadow-sm';
    toggleElement.className = 'absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-all duration-200 translate-x-0 shadow-sm';
  }
  
  setupAutoRefresh();
}

// ============================================================================
// EVENT LISTENERS
// ============================================================================

/**
 * Sets up event listeners for the dashboard
 * @returns {void}
 */
function setupEventListeners() {
  // Manual refresh button
  elements.manualRefreshBtn.addEventListener('click', fetchMetrics);

  // Refresh rate dropdown
  elements.refreshRateBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    elements.refreshDropdown.classList.toggle('hidden');
  });

  // Dropdown options
  document.querySelectorAll('.refresh-option').forEach(option => {
    option.addEventListener('click', (e) => {
      e.stopPropagation();
      handleRefreshRateChange(parseInt(e.target.dataset.interval));
      elements.refreshDropdown.classList.add('hidden');
    });
  });

  // Close dropdown when clicking outside
  document.addEventListener('click', (e) => {
    if (!elements.refreshDropdown.contains(e.target) && !elements.refreshRateBtn.contains(e.target)) {
      elements.refreshDropdown.classList.add('hidden');
    }
  });

  // Auto refresh toggle
  elements.autoToggle.addEventListener('click', toggleAutoRefresh);
}

// ============================================================================
// INITIALIZATION
// ============================================================================

/**
 * Initializes the dashboard application
 * @returns {void}
 */
function initialize() {
  safeCreateIcons();
  updateRefreshRateDisplay();
  setupEventListeners();
  
  // Show dashboard skeleton immediately for better perceived performance
  showDashboardSkeleton();
  
  // Load data asynchronously without blocking
  setTimeout(() => {
    fetchMetrics();
    setupAutoRefresh();
  }, 0);
  
  // Update last refresh display every 30 seconds
  setInterval(updateLastRefreshDisplay, CONFIG.LAST_REFRESH_UPDATE_INTERVAL);
}

/**
 * Shows the dashboard skeleton
 * @returns {void}
 */
function showDashboardSkeleton() {
  // Show the basic layout immediately
  elements.mainContent.innerHTML = createDashboardLayout();
  setupActivityToggleListeners();
  
  // Show loading states in key areas
  const overviewCards = document.getElementById('overview-cards');
  if (overviewCards) {
    overviewCards.innerHTML = createSkeletonCards();
  }
  
  const agentTable = document.getElementById('agent-table');
  if (agentTable) {
    agentTable.innerHTML = createSkeletonTable();
  }
  
  safeCreateIcons();
}

/**
 * Creates skeleton cards for the dashboard
 * @returns {string} HTML string for skeleton cards
 */
function createSkeletonCards() {
  return `
    <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
      <div class="animate-pulse">
        <div class="h-6 bg-gray-200 rounded w-12 mb-2"></div>
        <div class="h-3 bg-gray-200 rounded w-20"></div>
      </div>
      <i data-lucide="cpu" class="w-4 h-4 text-gray-300 absolute top-4 right-4"></i>
      <div class="mt-2 h-3 bg-gray-200 rounded w-24 animate-pulse"></div>
    </div>
    <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
      <div class="animate-pulse">
        <div class="h-6 bg-gray-200 rounded w-12 mb-2"></div>
        <div class="h-3 bg-gray-200 rounded w-20"></div>
      </div>
      <i data-lucide="clock" class="w-4 h-4 text-gray-300 absolute top-4 right-4"></i>
      <div class="mt-2 h-3 bg-gray-200 rounded w-24 animate-pulse"></div>
    </div>
    <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
      <div class="animate-pulse">
        <div class="h-6 bg-gray-200 rounded w-12 mb-2"></div>
        <div class="h-3 bg-gray-200 rounded w-20"></div>
      </div>
      <i data-lucide="zap" class="w-4 h-4 text-gray-300 absolute top-4 right-4"></i>
      <div class="mt-2 h-3 bg-gray-200 rounded w-24 animate-pulse"></div>
    </div>
    <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
      <div class="animate-pulse">
        <div class="h-6 bg-gray-200 rounded w-12 mb-2"></div>
        <div class="h-3 bg-gray-200 rounded w-20"></div>
      </div>
      <i data-lucide="check-circle" class="w-4 h-4 text-gray-300 absolute top-4 right-4"></i>
      <div class="mt-2 h-3 bg-gray-200 rounded w-24 animate-pulse"></div>
    </div>
  `;
}

/**
 * Creates skeleton table for the dashboard
 * @returns {string} HTML string for skeleton table
 */
function createSkeletonTable() {
  return `
    <div class="px-6 py-4 border-b border-gray-200">
      <h3 class="text-lg font-medium text-gray-900">Agents</h3>
    </div>
    <div class="overflow-x-auto">
      <table class="w-full">
        <thead class="bg-gray-50">
          <tr>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Agent</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
            <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Queue</th>
            <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Processing</th>
            <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Backoff</th>
            <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Completed/Min</th>
            <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Success Rate</th>
            <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Avg Time</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Last Active</th>
          </tr>
        </thead>
        <tbody class="bg-white divide-y divide-gray-200">
          <tr class="animate-pulse">
            <td class="px-6 py-4"><div class="h-4 bg-gray-200 rounded w-20"></div></td>
            <td class="px-6 py-4"><div class="h-4 bg-gray-200 rounded w-16"></div></td>
            <td class="px-6 py-4 text-center"><div class="h-4 bg-gray-200 rounded w-8 mx-auto"></div></td>
            <td class="px-6 py-4 text-center"><div class="h-4 bg-gray-200 rounded w-8 mx-auto"></div></td>
            <td class="px-6 py-4 text-center"><div class="h-4 bg-gray-200 rounded w-8 mx-auto"></div></td>
            <td class="px-6 py-4 text-center"><div class="h-4 bg-gray-200 rounded w-12 mx-auto"></div></td>
            <td class="px-6 py-4 text-center"><div class="h-4 bg-gray-200 rounded w-10 mx-auto"></div></td>
            <td class="px-6 py-4 text-center"><div class="h-4 bg-gray-200 rounded w-10 mx-auto"></div></td>
            <td class="px-6 py-4"><div class="h-4 bg-gray-200 rounded w-16"></div></td>
          </tr>
        </tbody>
      </table>
    </div>
  `;
}

// ============================================================================
// APPLICATION STARTUP
// ============================================================================

// Start the application
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initialize);
} else {
  initialize();
}