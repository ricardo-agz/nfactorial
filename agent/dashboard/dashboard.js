// Helper function to safely create icons
function safeCreateIcons() {
  try {
    if (typeof lucide !== 'undefined' && lucide.createIcons) {
      lucide.createIcons();
    }
  } catch (e) {
    console.warn('Failed to load icons', e);
  }
}

let refreshTimer = null;
let lastData = null;
let autoRefresh = true;
let charts = {};
let refreshInterval = 10000; // Default 10 seconds
let lastRefreshTime = null;

const statusBadge = document.getElementById('status-badge');
const manualRefreshBtn = document.getElementById('manual-refresh-btn');
const refreshRateBtn = document.getElementById('refresh-rate-btn');
const refreshDropdown = document.getElementById('refresh-dropdown');
const autoToggle = document.getElementById('auto-toggle');
const mainContent = document.getElementById('main-content');
const footer = document.getElementById('footer');
const lastRefreshText = document.getElementById('last-refresh-text');
const refreshRateDisplay = document.getElementById('refresh-rate-display');

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
  if (ms < 60000) {
    return `${ms / 1000} s`;
  } else {
    return `${ms / 60000} min`;
  }
}

function formatLastRefresh() {
  if (!lastRefreshTime) return 'Last refresh: --';
  
  const now = new Date();
  const diffMs = now - lastRefreshTime;
  const diffSecs = Math.floor(diffMs / 1000);
  const diffMins = Math.floor(diffMs / 60000);
  
  if (diffSecs < 60) {
    return `Last refresh: < 1 min`;
  } else if (diffMins < 60) {
    return `Last refresh: ${diffMins} min ago`;
  } else {
    const diffHours = Math.floor(diffMins / 60);
    return `Last refresh: ${diffHours}h ago`;
  }
}

function updateLastRefreshDisplay() {
  if (lastRefreshText) {
    lastRefreshText.textContent = formatLastRefresh();
  }
}

function updateRefreshRateDisplay() {
  if (refreshRateDisplay) {
    refreshRateDisplay.textContent = formatRefreshInterval(refreshInterval);
  }
}

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
  // Agent has no activity but is presumably ready to work
  return 'ready';
}

function getStatusInfo(status) {
  switch(status) {
    case 'active': return { label: 'Active', color: 'text-green-600', dot: 'bg-green-500' };
    case 'busy': return { label: 'Busy', color: 'text-blue-600', dot: 'bg-blue-500' };
    case 'idle': return { label: 'Idle', color: 'text-gray-600', dot: 'bg-gray-400' };
    case 'ready': return { label: 'Ready', color: 'text-green-600', dot: 'bg-green-400' };
    case 'warning': return { label: 'Warning', color: 'text-yellow-600', dot: 'bg-yellow-500' };
    case 'offline': return { label: 'Offline', color: 'text-red-600', dot: 'bg-red-500' };
    default: return { label: 'Unknown', color: 'text-gray-600', dot: 'bg-gray-500' };
  }
}

function createTaskDistributionChart(distribution) {
  const ctx = document.getElementById('taskDistributionChart');
  if (!ctx) return;

  // Check if there's any data
  const totalTasks = distribution.queued + distribution.processing + 
                    distribution.completed + distribution.failed + 
                    distribution.cancelled + distribution.pending_tool_results;

  const chartContainer = ctx.parentElement; // This is now the wheel container only
  const legendContainer = document.getElementById('chart-legend');

  // Legend data
  const legendData = [
    { label: 'Queued', color: '#3B82F6', value: distribution.queued },
    { label: 'Processing', color: '#10B981', value: distribution.processing },
    { label: 'Completed', color: '#059669', value: distribution.completed },
    { label: 'Failed', color: '#EF4444', value: distribution.failed },
    { label: 'Cancelled', color: '#6B7280', value: distribution.cancelled },
    { label: 'Pending Tools', color: '#F59E0B', value: distribution.pending_tool_results }
  ];

  // Update legend
  if (legendContainer) {
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

  // If chart exists and we can update it, do so instead of recreating
  if (charts.taskDistribution && !charts.taskDistribution.destroyed) {
    if (totalTasks === 0) {
      // Switch to placeholder mode - show a subtle gray wheel
      charts.taskDistribution.data.labels = ['Queued', 'Processing', 'Completed', 'Failed', 'Cancelled', 'Pending Tools'];
      charts.taskDistribution.data.datasets[0].data = [1, 0, 0, 0, 0, 0]; // Small value to show wheel
      charts.taskDistribution.data.datasets[0].backgroundColor = [
        '#E5E7EB', // much lighter gray for placeholder (different from queued blue)
        '#10B981', // green - processing
        '#059669', // dark green - completed
        '#EF4444', // red - failed
        '#6B7280', // gray - cancelled
        '#F59E0B'  // yellow - pending tools
      ];
      charts.taskDistribution.data.datasets[0].borderColor = undefined;
      charts.taskDistribution.options.plugins.legend.display = false;
      charts.taskDistribution.options.plugins.tooltip.enabled = false;
      charts.taskDistribution.update('none'); // Update without animation

      // Add overlay text
      let overlay = chartContainer.querySelector('.chart-overlay');
      if (!overlay) {
        overlay = document.createElement('div');
        overlay.className = 'chart-overlay absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 pointer-events-none';
        overlay.innerHTML = `
          <div class="text-center">
            <div class="text-sm font-normal text-gray-500">No Tasks</div>
            <div class="text-xs text-gray-400">System is idle</div>
          </div>
        `;
        chartContainer.appendChild(overlay);
      }
    } else {
      // Update with real data
      charts.taskDistribution.data.labels = ['Queued', 'Processing', 'Completed', 'Failed', 'Cancelled', 'Pending Tools'];
      charts.taskDistribution.data.datasets[0].data = [
        distribution.queued,
        distribution.processing,
        distribution.completed,
        distribution.failed,
        distribution.cancelled,
        distribution.pending_tool_results
      ];
      charts.taskDistribution.data.datasets[0].backgroundColor = [
        '#3B82F6', // blue - queued
        '#10B981', // green - processing
        '#059669', // dark green - completed
        '#EF4444', // red - failed
        '#6B7280', // gray - cancelled
        '#F59E0B'  // yellow - pending tools
      ];
      charts.taskDistribution.data.datasets[0].borderColor = undefined;
      charts.taskDistribution.options.plugins.legend.display = false;
      charts.taskDistribution.options.plugins.tooltip.enabled = true;
      charts.taskDistribution.update('none'); // Update without animation

      // Remove overlay if it exists
      const overlay = chartContainer.querySelector('.chart-overlay');
      if (overlay) {
        overlay.remove();
      }
    }
    return;
  }

  // Create new chart only if it doesn't exist
  if (charts.taskDistribution) {
    charts.taskDistribution.destroy();
  }

  if (totalTasks === 0) {
    // Show placeholder when no data
    charts.taskDistribution = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['Queued', 'Processing', 'Completed', 'Failed', 'Cancelled', 'Pending Tools'],
        datasets: [{
          data: [1, 0, 0, 0, 0, 0], // Small value to show wheel
          backgroundColor: [
            '#E5E7EB', // much lighter gray for placeholder (different from queued blue)
            '#10B981', // green - processing
            '#059669', // dark green - completed
            '#EF4444', // red - failed
            '#6B7280', // gray - cancelled
            '#F59E0B'  // yellow - pending tools
          ],
          borderWidth: 0
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: {
          legend: {
            display: false
          },
          tooltip: {
            enabled: false
          }
        },
        elements: {
          arc: {
            borderWidth: 2
          }
        }
      }
    });

    // Add overlay text
    let overlay = chartContainer.querySelector('.chart-overlay');
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.className = 'chart-overlay absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 pointer-events-none';
      overlay.innerHTML = `
        <div class="text-center">
          <div class="text-sm font-normal text-gray-500">No Tasks</div>
          <div class="text-xs text-gray-400">System is idle</div>
        </div>
      `;
      chartContainer.appendChild(overlay);
    }
  } else {
    // Remove overlay if it exists
    const overlay = chartContainer.querySelector('.chart-overlay');
    if (overlay) {
      overlay.remove();
    }

    charts.taskDistribution = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['Queued', 'Processing', 'Completed', 'Failed', 'Cancelled', 'Pending Tools'],
        datasets: [{
          data: [
            distribution.queued,
            distribution.processing,
            distribution.completed,
            distribution.failed,
            distribution.cancelled,
            distribution.pending_tool_results
          ],
          backgroundColor: [
            '#3B82F6', // blue - queued
            '#10B981', // green - processing
            '#059669', // dark green - completed
            '#EF4444', // red - failed
            '#6B7280', // gray - cancelled
            '#F59E0B'  // yellow - pending tools
          ],
          borderWidth: 0
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: {
          legend: {
            display: false
          }
        }
      }
    });
  }
}

function updateTaskDistributionChart(distribution) {
  // This is now just an alias to the main function since we handle updates there
  createTaskDistributionChart(distribution);
}

function formatBucketTime(timestamp, bucketSize) {
  const date = new Date(timestamp * 1000);
  
  if (bucketSize < 3600) { // Less than 1 hour - show time
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  } else if (bucketSize < 86400) { // Less than 1 day - show hour
    return date.toLocaleTimeString([], { hour: '2-digit' }) + 'h';
  } else { // 1 day or more - show date
    return date.toLocaleDateString([], { month: 'short', day: 'numeric' });
  }
}

function createActivityChart(canvasId, data, color, emptyMessage) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) {
    return;
  }
  
  // Check if Chart.js is available
  if (typeof Chart === 'undefined') {
    return;
  }

  // Destroy existing chart if it exists
  if (charts[canvasId]) {
    charts[canvasId].destroy();
  }

  // If no data or no tasks, show empty state
  if (!data || !data.task_timestamps || data.task_timestamps.length === 0) {
    // Create a minimal chart with no data
    charts[canvasId] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: Array.from({length: 60}, () => ''),
        datasets: [{
          data: Array.from({length: 60}, () => 1), // Full height for empty state
          backgroundColor: '#F3F4F6',
          borderWidth: 0
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: {
          legend: { display: false },
          tooltip: { enabled: false }
        },
        scales: {
          x: { display: false, grid: { display: false } },
          y: { display: false, beginAtZero: true, min: 0, max: 1, grid: { display: false } }
        },
        layout: { padding: 0 },
        elements: {
          bar: {
            categoryPercentage: 1.0,
            barPercentage: 0.9
          }
        }
      }
    });
    return charts[canvasId];
  }

  // Calculate time range and bar configuration
  const now = Date.now() / 1000;
  const ttlSeconds = data.ttl_seconds;
  const startTime = now - ttlSeconds;
  const endTime = now;
  const timeSpan = endTime - startTime;
  
  // Determine number of bars based on time span
  const numBars = Math.min(120, Math.max(60, Math.floor(timeSpan / 30))); // 1 bar per 30 seconds, more bars
  const barTimeSpan = timeSpan / numBars;
  
  // Create bar data - count actual tasks in each time bucket
  const barData = [];
  const backgroundColors = [];
  
  for (let i = 0; i < numBars; i++) {
    const barStartTime = startTime + (i * barTimeSpan);
    const barEndTime = barStartTime + barTimeSpan;
    
    // Count tasks that fall within this bar's time range
    const tasksInBar = data.task_timestamps.filter(timestamp => 
      timestamp >= barStartTime && timestamp < barEndTime
    ).length;
    
    // All bars have the same height (1), only color changes
    barData.push(1);
    backgroundColors.push(tasksInBar > 0 ? color : '#F3F4F6');
  }
  
  // Fixed scale since all bars are height 1
  const maxValue = 1;
  
  const labels = Array.from({length: numBars}, () => '');

  try {
    charts[canvasId] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [{
          data: barData,
          backgroundColor: backgroundColors,
          borderWidth: 0
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: {
          legend: { display: false },
          tooltip: { enabled: false }
        },
        scales: {
          x: {
            display: false,
            grid: { display: false }
          },
          y: {
            display: false,
            beginAtZero: true,
            min: 0,
            max: maxValue,
            grid: { display: false }
          }
        },
        layout: {
          padding: 0
        },
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

  return charts[canvasId];
}

function updateActivityCharts(activityCharts) {
  if (!activityCharts) {
    return;
  }

  // Create/update completed chart
  createActivityChart('completedChart', activityCharts.completed, '#10B981', 'No completed tasks');
  
  // Create/update failed chart  
  createActivityChart('failedChart', activityCharts.failed, '#EF4444', 'No failed tasks');
  
  // Create/update cancelled chart
  createActivityChart('cancelledChart', activityCharts.cancelled, '#8B5CF6', 'No cancelled tasks');

  // Update summaries
  function formatDuration(seconds) {
    if (seconds < 60) {
      return `${seconds}s`;
    } else if (seconds < 3600) {
      const minutes = seconds / 60;
      return minutes % 1 === 0 ? `${minutes}m` : `${minutes.toFixed(1)}m`;
    } else {
      const hours = seconds / 3600;
      return hours % 1 === 0 ? `${hours}h` : `${hours.toFixed(1)}h`;
    }
  }

  const completedSummary = document.getElementById('completed-summary');
  if (completedSummary) {
    const completed = activityCharts.completed;
    completedSummary.textContent = `${completed.total_tasks} tasks over the last ${formatDuration(completed.ttl_seconds)}`;
  }

  const failedSummary = document.getElementById('failed-summary');
  if (failedSummary) {
    const failed = activityCharts.failed;
    failedSummary.textContent = `${failed.total_tasks} tasks over the last ${formatDuration(failed.ttl_seconds)}`;
  }

  const cancelledSummary = document.getElementById('cancelled-summary');
  if (cancelledSummary) {
    const cancelled = activityCharts.cancelled;
    cancelledSummary.textContent = `${cancelled.total_tasks} tasks over the last ${formatDuration(cancelled.ttl_seconds)}`;
  }
}

function renderDashboard(data) {
  const { system, queues, performance, task_distribution, activity_charts, ttl_info } = data;
  
  // Update status badge
  const hasIssues = queues.some(q => getAgentStatus(q) === 'warning' || getAgentStatus(q) === 'offline');
  const isHealthy = system.redis_connected && !hasIssues;
  
  statusBadge.className = `flex items-center gap-2 text-sm font-medium ${isHealthy ? 'text-green-600' : hasIssues ? 'text-yellow-600' : 'text-red-600'}`;
  statusBadge.querySelector('.w-2').className = `w-2 h-2 rounded-full ${isHealthy ? 'bg-green-500' : hasIssues ? 'bg-yellow-500' : 'bg-red-500'}`;
  statusBadge.querySelector('span').textContent = isHealthy ? 'All systems operational' : hasIssues ? 'Some agents need attention' : 'System issues detected';

  // Check if this is the first render or if we need to recreate the layout
  const isFirstRender = !mainContent.querySelector('.w-full');
  
  if (isFirstRender) {
    mainContent.innerHTML = `
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
            <!-- Completed Tasks Chart -->
            <div class="space-y-3">
              <div class="flex items-center justify-between">
                <div class="flex items-center gap-2">
                  <i data-lucide="check-circle" class="w-4 h-4 text-green-500"></i>
                  <h4 class="text-sm font-medium text-gray-900">Completed Tasks</h4>
                </div>
                <div id="completed-summary" class="text-xs text-gray-600">
                  <!-- Summary will be populated by JavaScript -->
                </div>
              </div>
              <div class="h-8 chart-container">
                <canvas id="completedChart" class="w-full h-full"></canvas>
              </div>
            </div>

            <!-- Failed Tasks Chart -->
            <div class="space-y-3">
              <div class="flex items-center justify-between">
                <div class="flex items-center gap-2">
                  <i data-lucide="x-circle" class="w-4 h-4 text-red-500"></i>
                  <h4 class="text-sm font-medium text-gray-900">Failed Tasks</h4>
                </div>
                <div id="failed-summary" class="text-xs text-gray-600">
                  <!-- Summary will be populated by JavaScript -->
                </div>
              </div>
              <div class="h-8 chart-container">
                <canvas id="failedChart" class="w-full h-full"></canvas>
              </div>
            </div>

            <!-- Cancelled Tasks Chart -->
            <div class="space-y-3">
              <div class="flex items-center justify-between">
                <div class="flex items-center gap-2">
                  <i data-lucide="ban" class="w-4 h-4 text-purple-500"></i>
                  <h4 class="text-sm font-medium text-gray-900">Cancelled Tasks</h4>
                </div>
                <div id="cancelled-summary" class="text-xs text-gray-600">
                  <!-- Summary will be populated by JavaScript -->
                </div>
              </div>
              <div class="h-8 chart-container">
                <canvas id="cancelledChart" class="w-full h-full"></canvas>
              </div>
            </div>
          </div>
        </div>

        <!-- Agent Performance Table -->
        <div id="agent-table" class="bg-white border border-gray-200 rounded-lg overflow-hidden">
          <!-- Table will be updated separately -->
        </div>
      </div>
    `;
  }

  // Update overview cards
  const overviewCards = document.getElementById('overview-cards');
  if (overviewCards) {
    overviewCards.innerHTML = `
      <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
        <div>
          <div class="text-xl font-semibold text-gray-900">${system.total_agents}</div>
          <div class="text-xs text-gray-600">Active Agents</div>
        </div>
        <i data-lucide="cpu" class="w-4 h-4 text-blue-500 absolute top-4 right-4"></i>
        <div class="mt-2 text-xs text-gray-500">
          ${system.total_workers} workers running
        </div>
      </div>

      <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
        <div>
          <div class="text-xl font-semibold text-gray-900">${system.total_tasks_queued}</div>
          <div class="text-xs text-gray-600">Tasks Queued</div>
        </div>
        <i data-lucide="clock" class="w-4 h-4 text-yellow-500 absolute top-4 right-4"></i>
        <div class="mt-2 text-xs text-gray-500">
          ${system.total_tasks_processing} currently processing
        </div>
      </div>

      <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
        <div>
          <div class="text-xl font-semibold text-gray-900">${system.system_throughput_per_minute.toFixed(1)}</div>
          <div class="text-xs text-gray-600">Tasks/Min</div>
        </div>
        <i data-lucide="zap" class="w-4 h-4 text-green-500 absolute top-4 right-4"></i>
        <div class="mt-2 text-xs text-gray-500">
          Current system throughput
        </div>
      </div>

      <div class="bg-white border border-gray-200 rounded-lg p-4 relative">
        <div>
          <div class="text-xl font-semibold text-gray-900">${system.overall_success_rate_percent.toFixed(1)}%</div>
          <div class="text-xs text-gray-600">Success Rate</div>
        </div>
        <i data-lucide="check-circle" class="w-4 h-4 text-green-500 absolute top-4 right-4"></i>
        <div class="mt-2 text-xs text-gray-500">
          ${system.activity_window_display}
        </div>
      </div>
    `;
  }

  // Update chart (this will now update existing chart instead of recreating)
  updateTaskDistributionChart(task_distribution);
  
  // Update activity charts
  updateActivityCharts(activity_charts);
  
  // Create icons for any new elements
  safeCreateIcons();

  // Update agent table
  const agentTable = document.getElementById('agent-table');
  if (agentTable) {
    agentTable.innerHTML = `
      <div class="px-6 py-4 border-b border-gray-200">
        <h3 class="text-lg font-medium text-gray-900">Agent Performance</h3>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Agent</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
              <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Queue</th>
              <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Processing</th>
              <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Throughput</th>
              <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Success Rate</th>
              <th class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Avg Time</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Last Active</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            ${queues.map((agent, index) => {
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
                  <td class="px-6 py-4 whitespace-nowrap text-center text-sm text-gray-900">${(perf.current_throughput || 0)}/min</td>
                  <td class="px-6 py-4 whitespace-nowrap text-center text-sm text-gray-900">${(perf.success_rate_percent || 0).toFixed(1)}%</td>
                  <td class="px-6 py-4 whitespace-nowrap text-center text-sm text-gray-900">${(perf.avg_processing_time_seconds || 0).toFixed(1)}s</td>
                  <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${formatLastActive(perf.last_activity)}</td>
                </tr>
              `;
            }).join('')}
          </tbody>
        </table>
      </div>
    `;
  }

  lastData = data;
}

async function fetchMetrics() {
  manualRefreshBtn.classList.add('opacity-70', 'pointer-events-none');
  manualRefreshBtn.querySelector('.w-4').classList.add('spin');
  
  try {
    const response = await fetch('/observability/api/metrics');
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    
    const data = await response.json();
    renderDashboard(data);
    
    // Update last refresh time
    lastRefreshTime = new Date();
    updateLastRefreshDisplay();
    
    footer.querySelector('div').textContent = `Last updated ${new Date(data.timestamp).toLocaleTimeString()} • ${autoRefresh ? 'Auto refresh enabled' : 'Manual refresh'}`;
  } catch (error) {
    console.error('Failed to fetch metrics:', error);
    statusBadge.className = 'flex items-center gap-2 text-sm font-medium text-red-600';
    statusBadge.querySelector('.w-2').className = 'w-2 h-2 rounded-full bg-red-500';
    statusBadge.querySelector('span').textContent = 'Connection Error';
    
    // Determine error type and show appropriate message
    let errorTitle = 'Connection Error';
    let errorMessage = 'Unable to connect to the dashboard service';
    let errorDetails = '';
    let showRetryButton = true;
    
    if (error.message.includes('HTTP 500')) {
      errorTitle = 'Server Error';
      errorMessage = 'The dashboard service encountered an internal error';
      errorDetails = 'This might be due to Redis connectivity issues or service problems.';
    } else if (error.message.includes('HTTP 404')) {
      errorTitle = 'Service Not Found';
      errorMessage = 'The dashboard API endpoint is not available';
      errorDetails = 'Please check if the observability service is properly configured.';
    } else if (error.message.includes('HTTP 503')) {
      errorTitle = 'Service Unavailable';
      errorMessage = 'The dashboard service is temporarily unavailable';
      errorDetails = 'The service may be starting up or experiencing high load.';
    } else if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
      errorTitle = 'Network Error';
      errorMessage = 'Unable to reach the dashboard service';
      errorDetails = 'Please check your network connection and ensure the service is running.';
    }
    
    mainContent.innerHTML = `
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
                <h3 class="text-lg font-semibold text-red-900">${errorTitle}</h3>
                <p class="text-sm text-red-700">${errorMessage}</p>
              </div>
            </div>
          </div>
          
          <!-- Body -->
          <div class="px-6 py-6">
            <div class="space-y-4">
              ${errorDetails ? `
                <div class="text-sm text-gray-600">
                  <p>${errorDetails}</p>
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
                    <span class="text-gray-900">${lastRefreshTime ? formatLastRefresh().replace('Last refresh: ', '') : 'Never'}</span>
                  </div>
                  <div class="flex items-center justify-between text-sm">
                    <span class="text-gray-600">Auto Refresh</span>
                    <span class="text-gray-900">${autoRefresh ? 'Enabled' : 'Disabled'}</span>
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
              
              ${showRetryButton ? `
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
              ` : ''}
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
    
    safeCreateIcons();
    
    // Add event listeners for action buttons
    const retryBtn = document.getElementById('retry-connection');
    const reloadBtn = document.getElementById('reload-page');
    
    if (retryBtn) {
      retryBtn.addEventListener('click', () => {
        fetchMetrics();
      });
    }
    
    if (reloadBtn) {
      reloadBtn.addEventListener('click', () => {
        window.location.reload();
      });
    }
    
    // Update footer
    footer.querySelector('div').textContent = `Connection failed • ${autoRefresh ? 'Auto refresh will continue trying' : 'Manual refresh required'}`;
  } finally {
    manualRefreshBtn.classList.remove('opacity-70', 'pointer-events-none');
    manualRefreshBtn.querySelector('.w-4').classList.remove('spin');
  }
}

function setupAutoRefresh() {
  clearInterval(refreshTimer);
  if (autoRefresh) {
    refreshTimer = setInterval(fetchMetrics, refreshInterval);
  }
}

// Event listeners
manualRefreshBtn.addEventListener('click', fetchMetrics);

// Handle refresh rate dropdown
refreshRateBtn.addEventListener('click', (e) => {
  e.stopPropagation();
  refreshDropdown.classList.toggle('hidden');
});

// Handle dropdown options
document.querySelectorAll('.refresh-option').forEach(option => {
  option.addEventListener('click', (e) => {
    e.stopPropagation();
    const newInterval = parseInt(e.target.dataset.interval);
    refreshInterval = newInterval;
    updateRefreshRateDisplay();
    refreshDropdown.classList.add('hidden');
    setupAutoRefresh(); // Restart with new interval
  });
});

// Close dropdown when clicking outside
document.addEventListener('click', (e) => {
  if (!refreshDropdown.contains(e.target) && !refreshRateBtn.contains(e.target)) {
    refreshDropdown.classList.add('hidden');
  }
});

autoToggle.addEventListener('click', () => {
  autoRefresh = !autoRefresh;
  const toggleElement = autoToggle.querySelector('div');
  if (autoRefresh) {
    autoToggle.className = 'w-10 h-6 bg-blue-500 rounded-full relative cursor-pointer transition-all duration-200 shadow-sm';
    toggleElement.className = 'absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-all duration-200 translate-x-4 shadow-sm';
  } else {
    autoToggle.className = 'w-10 h-6 bg-gray-300 rounded-full relative cursor-pointer transition-all duration-200 shadow-sm';
    toggleElement.className = 'absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-all duration-200 translate-x-0 shadow-sm';
  }
  setupAutoRefresh();
});

// Update last refresh display every 30 seconds
setInterval(updateLastRefreshDisplay, 30000);

// Initialize
function initialize() {
  safeCreateIcons();
  updateRefreshRateDisplay(); // Set initial refresh rate display
  fetchMetrics();
  setupAutoRefresh();
}

// Wait for both DOM and Lucide to be ready
function waitForLucideAndInitialize() {
  if (typeof lucide !== 'undefined' && lucide.createIcons) {
    initialize();
  } else {
    // Retry after a short delay
    setTimeout(waitForLucideAndInitialize, 100);
  }
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', waitForLucideAndInitialize);
} else {
  setTimeout(waitForLucideAndInitialize, 100);
} 