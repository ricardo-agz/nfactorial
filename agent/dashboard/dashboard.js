// ============================================================================
// CONSTANTS AND CONFIGURATION
// ============================================================================

const CONFIG = {
  DEFAULT_REFRESH_INTERVAL: 10000, // 10 seconds
  CHART_BARS: 60,
  MIN_BAR_HEIGHT_RATIO: 0.15,
  LAST_REFRESH_UPDATE_INTERVAL: 30000 // 30 seconds
};

const TASK_COLORS = {
  queued: '#3B82F6',
  processing: '#10B981', 
  backoff: '#F97316',
  completed: '#059669',
  failed: '#EF4444',
  cancelled: '#6B7280',
  pending_tool_results: '#F59E0B'
};

const ACTIVITY_COLORS = {
  completed: '#10B981',
  failed: '#EF4444', 
  cancelled: '#8B5CF6'
};

const STATUS_CONFIG = {
  active: { label: 'Active', color: 'text-green-600', dot: 'bg-green-500' },
  busy: { label: 'Busy', color: 'text-blue-600', dot: 'bg-blue-500' },
  idle: { label: 'Idle', color: 'text-gray-600', dot: 'bg-gray-400' },
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

function safeCreateIcons() {
  try {
    if (typeof lucide !== 'undefined' && lucide.createIcons) {
      lucide.createIcons();
    }
  } catch (e) {
    console.warn('Failed to load icons', e);
  }
}

function formatUptime(seconds) {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

function formatLastActive(timestamp) {
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

function formatRefreshInterval(ms) {
  return ms < 60000 ? `${ms / 1000} s` : `${ms / 60000} min`;
}

function formatLastRefresh() {
  if (!state.lastRefreshTime) return 'Last refresh: --';
  
  const now = new Date();
  const diffMs = now - state.lastRefreshTime;
  const diffSecs = Math.floor(diffMs / 1000);
  const diffMins = Math.floor(diffMs / 60000);
  
  if (diffSecs < 60) return 'Last refresh: < 1 min';
  if (diffMins < 60) return `Last refresh: ${diffMins} min ago`;
  
  const diffHours = Math.floor(diffMins / 60);
  return `Last refresh: ${diffHours}h ago`;
}

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
 * Determines the status of an agent based on its current activity:
 * - 'warning': Has stale tasks that may need attention
 * - 'active': Currently processing tasks
 * - 'busy': Has tasks queued but not processing (waiting for workers)
 * - 'idle': Has processed tasks before but currently inactive
 * - 'ready': Healthy agent with no activity history (ready to work)
 * - 'offline': Reserved for truly offline/unresponsive agents
 */
function getAgentStatus(agent) {
  if (agent.stale_tasks_count > 0) return 'warning';
  if (agent.processing_count > 0) return 'active';
  if (agent.main_queue_size > 0) return 'busy';
  if (agent.completed_count > 0 || agent.failed_count > 0) return 'idle';
  return 'ready';
}

function getStatusInfo(status) {
  return STATUS_CONFIG[status] || STATUS_CONFIG.default;
}

// ============================================================================
// CHART MANAGEMENT
// ============================================================================

function destroyChart(chartId) {
  if (state.charts[chartId]) {
    state.charts[chartId].destroy();
    delete state.charts[chartId];
  }
}

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

function updateTaskDistributionLegend(distribution, legendContainer) {
  if (!legendContainer) return;

  const legendData = [
    { label: 'Queued', color: TASK_COLORS.queued, value: distribution.queued },
    { label: 'Processing', color: TASK_COLORS.processing, value: distribution.processing },
    { label: 'Backoff', color: TASK_COLORS.backoff, value: distribution.backoff },
    { label: 'Completed', color: TASK_COLORS.completed, value: distribution.completed },
    { label: 'Failed', color: TASK_COLORS.failed, value: distribution.failed },
    { label: 'Cancelled', color: TASK_COLORS.cancelled, value: distribution.cancelled },
    { label: 'Idle', color: TASK_COLORS.pending_tool_results, value: distribution.idle }
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

function updateExistingTaskChart(distribution, totalTasks, chartContainer) {
  const chart = state.charts.taskDistribution;
  
  if (totalTasks === 0) {
    // Show placeholder
    chart.data.datasets[0].data = [1, 0, 0, 0, 0, 0, 0];
    chart.data.datasets[0].backgroundColor = ['#E5E7EB', ...Object.values(TASK_COLORS).slice(1)];
    chart.options.plugins.legend.display = false;
    chart.options.plugins.tooltip.enabled = false;
    chart.update('none');
    
    showChartOverlay(chartContainer, 'No Tasks', 'System is idle');
  } else {
    // Show real data
    chart.data.datasets[0].data = [
      distribution.queued, distribution.processing, distribution.backoff,
      distribution.completed, distribution.failed, distribution.cancelled,
      distribution.pending_tool_results
    ];
    chart.data.datasets[0].backgroundColor = Object.values(TASK_COLORS);
    chart.options.plugins.legend.display = false;
    chart.options.plugins.tooltip.enabled = true;
    chart.update('none');
    
    hideChartOverlay(chartContainer);
  }
}

function createNewTaskChart(ctx, distribution, totalTasks, chartContainer) {
  destroyChart('taskDistribution');

  const chartData = totalTasks === 0 
    ? [1, 0, 0, 0, 0, 0, 0]
    : [distribution.queued, distribution.processing, distribution.backoff,
       distribution.completed, distribution.failed, distribution.cancelled,
       distribution.pending_tool_results];

  const backgroundColor = totalTasks === 0
    ? ['#E5E7EB', ...Object.values(TASK_COLORS).slice(1)]
    : Object.values(TASK_COLORS);

  state.charts.taskDistribution = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: ['Queued', 'Processing', 'Backoff', 'Completed', 'Failed', 'Cancelled', 'Pending Tools'],
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
    showChartOverlay(chartContainer, 'No Tasks', 'System is idle');
  }
}

function showChartOverlay(container, title, subtitle) {
  let overlay = container.querySelector('.chart-overlay');
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
}

function hideChartOverlay(container) {
  const overlay = container.querySelector('.chart-overlay');
  if (overlay) overlay.remove();
}

function createActivityChart(canvasId, data, color, emptyMessage) {
  const ctx = document.getElementById(canvasId);
  if (!ctx || typeof Chart === 'undefined') return;

  destroyChart(canvasId);

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
        animation: false,
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

function updateActivityCharts(activityCharts) {
  if (!activityCharts) return;

  // Create/update charts
  createActivityChart('completedChart', activityCharts.completed, ACTIVITY_COLORS.completed, 'No completed tasks');
  createActivityChart('failedChart', activityCharts.failed, ACTIVITY_COLORS.failed, 'No failed tasks');
  createActivityChart('cancelledChart', activityCharts.cancelled, ACTIVITY_COLORS.cancelled, 'No cancelled tasks');

  // Update summaries
  updateActivitySummary('completed', activityCharts.completed);
  updateActivitySummary('failed', activityCharts.failed);
  updateActivitySummary('cancelled', activityCharts.cancelled);
}

function updateActivitySummary(type, data) {
  const summaryElement = document.getElementById(`${type}-summary`);
  if (summaryElement) {
    summaryElement.textContent = `${data.total_tasks} tasks over the last ${formatDuration(data.ttl_seconds)}`;
  }
}

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
}

function cleanupExistingAgentCharts(container) {
  const existingCanvases = container.querySelectorAll('canvas');
  existingCanvases.forEach(canvas => {
    destroyChart(canvas.id);
  });
  container.innerHTML = '';
}

function addAggregateChart(container, activityType) {
  const aggregateData = state.lastData?.activity_charts?.[activityType];
  const totalTasks = aggregateData?.total_tasks || 0;
  
  const allSection = document.createElement('div');
  allSection.className = 'space-y-3';
  allSection.innerHTML = createAgentChartHTML('All', totalTasks, `all-${activityType}`);
  container.appendChild(allSection);
  
  createActivityChart(`all-${activityType}`, aggregateData, ACTIVITY_COLORS[activityType], `No ${activityType} tasks`);
}

function addIndividualAgentChart(container, agentName, activityData, activityType) {
  const agentSection = document.createElement('div');
  agentSection.className = 'space-y-3';
  agentSection.innerHTML = createAgentChartHTML(agentName, activityData.total_tasks, `agent-${agentName}-${activityType}`);
  container.appendChild(agentSection);
  
  createActivityChart(`agent-${agentName}-${activityType}`, activityData, ACTIVITY_COLORS[activityType], `No ${activityType} tasks`);
}

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

function updateLastRefreshDisplay() {
  if (elements.lastRefreshText) {
    elements.lastRefreshText.textContent = formatLastRefresh();
  }
}

function updateRefreshRateDisplay() {
  if (elements.refreshRateDisplay) {
    elements.refreshRateDisplay.textContent = formatRefreshInterval(state.refreshInterval);
  }
}

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
      <div class="mt-2 text-xs text-gray-500">Recent throughput (last 5m)</div>
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
}

function updateAgentTable(queues, performance) {
  const agentTable = document.getElementById('agent-table');
  if (!agentTable) return;

  const tableRows = queues.map((agent, index) => {
    const status = getAgentStatus(agent);
    const statusInfo = getStatusInfo(status);
    const perf = performance[index] || {};
    
    return `
      <tr class="hover:bg-gray-50">
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
        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${formatLastActive(perf.last_activity)}</td>
      </tr>
    `;
  }).join('');

  agentTable.innerHTML = `
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
            <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Throughput</th>
            <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Success Rate</th>
            <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Avg Time</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Last Active</th>
          </tr>
        </thead>
        <tbody class="bg-white divide-y divide-gray-200">
          ${tableRows}
        </tbody>
      </table>
    </div>
  `;
}

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
}

// ============================================================================
// DASHBOARD LAYOUT TEMPLATES
// ============================================================================

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

function renderDashboard(data) {
  const { system, queues, performance, task_distribution, activity_charts, agent_activity_charts } = data;
  
  // Update status badge
  updateStatusBadge(data);

  // Check if this is the first render
  const isFirstRender = !elements.mainContent.querySelector('.w-full');
  
  if (isFirstRender) {
    elements.mainContent.innerHTML = createDashboardLayout();
    setupActivityToggleListeners();
  }

  // Update all dashboard components
  updateOverviewCards(system);
  createTaskDistributionChart(task_distribution);
  updateActivityCharts(activity_charts);
  
  // Update individual agent charts for any that are visible
  if (agent_activity_charts) {
    ['completed', 'failed', 'cancelled'].forEach(activityType => {
      if (state.showIndividualAgents[activityType]) {
        createIndividualAgentCharts(agent_activity_charts, activityType);
      }
    });
  }
  
  updateToggleButtonStates();
  updateAgentTable(queues, performance);
  
  safeCreateIcons();
  state.lastData = data;
}

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

function getErrorInfo(error) {
  if (error.message.includes('HTTP 500')) {
    return {
      title: 'Server Error',
      message: 'The dashboard service encountered an internal error',
      details: 'This might be due to Redis connectivity issues or service problems.'
    };
  }
  
  if (error.message.includes('HTTP 404')) {
    return {
      title: 'Service Not Found',
      message: 'The dashboard API endpoint is not available',
      details: 'Please check if the observability service is properly configured.'
    };
  }
  
  if (error.message.includes('HTTP 503')) {
    return {
      title: 'Service Unavailable',
      message: 'The dashboard service is temporarily unavailable',
      details: 'The service may be starting up or experiencing high load.'
    };
  }
  
  if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
    return {
      title: 'Network Error',
      message: 'Unable to reach the dashboard service',
      details: 'Please check your network connection and ensure the service is running.'
    };
  }
  
  return {
    title: 'Connection Error',
    message: 'Unable to connect to the dashboard service',
    details: ''
  };
}

function createErrorDisplay(errorInfo) {
  return `
    <div class="max-w-2xl mx-auto">
      <!-- Error Card -->
      <div class="bg-white border border-red-200 rounded-lg shadow-sm overflow-hidden">
        <!-- Header -->
        <div class="bg-red-50 px-6 py-4 border-b border-red-200">
          <div class="flex items-center gap-3">
            <div class="flex-shrink-0">
              <i data-lucide="wifi-off" class="w-8 h-8 text-red-500"></i>
            </div>
            <div>
              <h3 class="text-lg font-semibold text-red-900">${errorInfo.title}</h3>
              <p class="text-sm text-red-700">${errorInfo.message}</p>
            </div>
          </div>
        </div>
        
        <!-- Body -->
        <div class="px-6 py-6">
          <div class="space-y-4">
            ${errorInfo.details ? `
              <div class="text-sm text-gray-600">
                <p>${errorInfo.details}</p>
              </div>
            ` : ''}
            
            <!-- Status Information -->
            <div class="bg-gray-50 rounded-lg p-4">
              <h4 class="text-sm font-medium text-gray-900 mb-3">Connection Status</h4>
              <div class="space-y-2">
                <div class="flex items-center justify-between text-sm">
                  <span class="text-gray-600">Dashboard Service</span>
                  <span class="flex items-center gap-2 text-red-600">
                    <div class="w-2 h-2 rounded-full bg-red-500"></div>
                    Disconnected
                  </span>
                </div>
                <div class="flex items-center justify-between text-sm">
                  <span class="text-gray-600">Last Successful Refresh</span>
                  <span class="text-gray-900">${state.lastRefreshTime ? formatLastRefresh().replace('Last refresh: ', '') : 'Never'}</span>
                </div>
                <div class="flex items-center justify-between text-sm">
                  <span class="text-gray-600">Auto Refresh</span>
                  <span class="text-gray-900">${state.autoRefresh ? 'Enabled' : 'Disabled'}</span>
                </div>
              </div>
            </div>
            
            <!-- Troubleshooting -->
            <div class="bg-blue-50 rounded-lg p-4">
              <h4 class="text-sm font-medium text-blue-900 mb-3">Troubleshooting</h4>
              <ul class="text-sm text-blue-800 space-y-1">
                <li class="flex items-start gap-2">
                  <span class="text-blue-500 mt-0.5">•</span>
                  <span>Check if the Robonet service is running</span>
                </li>
                <li class="flex items-start gap-2">
                  <span class="text-blue-500 mt-0.5">•</span>
                  <span>Verify Redis connectivity</span>
                </li>
                <li class="flex items-start gap-2">
                  <span class="text-blue-500 mt-0.5">•</span>
                  <span>Ensure network connectivity to the service</span>
                </li>
                <li class="flex items-start gap-2">
                  <span class="text-blue-500 mt-0.5">•</span>
                  <span>Try refreshing the page</span>
                </li>
              </ul>
            </div>
            
            <!-- Action Buttons -->
            <div class="flex items-center gap-3 pt-2">
              <button id="retry-connection" class="flex items-center gap-2 px-4 py-2 bg-red-600 text-white text-sm font-medium rounded-lg hover:bg-red-700 transition-colors">
                <i data-lucide="refresh-cw" class="w-4 h-4"></i>
                Retry Connection
              </button>
              <button id="reload-page" class="flex items-center gap-2 px-4 py-2 bg-gray-600 text-white text-sm font-medium rounded-lg hover:bg-gray-700 transition-colors">
                <i data-lucide="rotate-ccw" class="w-4 h-4"></i>
                Reload Page
              </button>
            </div>
          </div>
        </div>
      </div>
      
      <!-- Additional Info -->
      <div class="mt-6 text-center">
        <p class="text-sm text-gray-500">
          The dashboard will automatically retry when auto-refresh is enabled.
        </p>
      </div>
    </div>
  `;
}

function setupErrorHandlers() {
  const retryBtn = document.getElementById('retry-connection');
  const reloadBtn = document.getElementById('reload-page');
  
  if (retryBtn) {
    retryBtn.addEventListener('click', fetchMetrics);
  }
  
  if (reloadBtn) {
    reloadBtn.addEventListener('click', () => window.location.reload());
  }
}

// ============================================================================
// DATA FETCHING
// ============================================================================

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

function setupAutoRefresh() {
  clearInterval(state.refreshTimer);
  if (state.autoRefresh) {
    state.refreshTimer = setInterval(fetchMetrics, state.refreshInterval);
  }
}

function handleRefreshRateChange(newInterval) {
  state.refreshInterval = newInterval;
  updateRefreshRateDisplay();
  setupAutoRefresh();
}

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
      const newInterval = parseInt(e.target.dataset.interval);
      handleRefreshRateChange(newInterval);
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

function initialize() {
  safeCreateIcons();
  updateRefreshRateDisplay();
  setupEventListeners();
  fetchMetrics();
  setupAutoRefresh();
  
  // Update last refresh display every 30 seconds
  setInterval(updateLastRefreshDisplay, CONFIG.LAST_REFRESH_UPDATE_INTERVAL);
}

function waitForLucideAndInitialize() {
  if (typeof lucide !== 'undefined' && lucide.createIcons) {
    initialize();
  } else {
    setTimeout(waitForLucideAndInitialize, 100);
  }
}

// Start the application
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', waitForLucideAndInitialize);
} else {
  setTimeout(waitForLucideAndInitialize, 100);
}