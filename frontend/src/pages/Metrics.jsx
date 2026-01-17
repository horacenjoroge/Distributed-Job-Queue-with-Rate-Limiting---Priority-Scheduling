import React, { useState, useEffect } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement
} from 'chart.js'
import { Line, Bar, Doughnut } from 'react-chartjs-2'
import { metricsService } from '../services/api'

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement
)

const Metrics = () => {
  const [metrics, setMetrics] = useState(null)
  const [loading, setLoading] = useState(true)
  const [timeWindow, setTimeWindow] = useState(3600) // 1 hour

  useEffect(() => {
    loadMetrics()

    const interval = setInterval(loadMetrics, 10000) // Refresh every 10 seconds

    return () => clearInterval(interval)
  }, [timeWindow])

  const loadMetrics = async () => {
    try {
      setLoading(true)
      const response = await metricsService.get(timeWindow)
      setMetrics(response.data)
    } catch (error) {
      console.error('Error loading metrics:', error)
    } finally {
      setLoading(false)
    }
  }

  if (loading && !metrics) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-gray-500">Loading metrics...</div>
      </div>
    )
  }

  if (!metrics) {
    return (
      <div className="text-center py-8">
        <p className="text-gray-500">No metrics available</p>
      </div>
    )
  }

  // Throughput Chart Data
  const throughputData = {
    labels: ['Tasks Enqueued/sec', 'Tasks Completed/sec'],
    datasets: [
      {
        label: 'Rate',
        data: [
          metrics.tasks?.enqueued_per_second || 0,
          metrics.tasks?.completed_per_second || 0
        ],
        backgroundColor: ['rgba(59, 130, 246, 0.5)', 'rgba(34, 197, 94, 0.5)'],
        borderColor: ['rgba(59, 130, 246, 1)', 'rgba(34, 197, 94, 1)'],
        borderWidth: 2
      }
    ]
  }

  // Duration Percentiles Chart Data
  const durationData = {
    labels: ['P50', 'P95', 'P99'],
    datasets: [
      {
        label: 'Duration (ms)',
        data: [
          (metrics.duration_percentiles?.p50_ms || 0) / 1000,
          (metrics.duration_percentiles?.p95_ms || 0) / 1000,
          (metrics.duration_percentiles?.p99_ms || 0) / 1000
        ],
        backgroundColor: 'rgba(147, 51, 234, 0.5)',
        borderColor: 'rgba(147, 51, 234, 1)',
        borderWidth: 2
      }
    ]
  }

  // Success/Failure Rate Chart Data
  const successRate = metrics.success_rate?.success_rate || 0
  const failureRate = metrics.success_rate?.failure_rate || 0
  const successRateData = {
    labels: ['Success', 'Failure'],
    datasets: [
      {
        data: [
          successRate * 100,
          failureRate * 100
        ],
        backgroundColor: ['rgba(34, 197, 94, 0.5)', 'rgba(239, 68, 68, 0.5)'],
        borderColor: ['rgba(34, 197, 94, 1)', 'rgba(239, 68, 68, 1)'],
        borderWidth: 2
      }
    ]
  }

  // Queue Sizes Chart Data
  const queueSizesData = {
    labels: Object.keys(metrics.queue_size_per_priority || {}),
    datasets: [
      {
        label: 'Queue Size by Priority',
        data: Object.values(metrics.queue_size_per_priority || {}),
        backgroundColor: 'rgba(59, 130, 246, 0.5)',
        borderColor: 'rgba(59, 130, 246, 1)',
        borderWidth: 2
      }
    ]
  }
  
  // Queue Info Chart Data (by queue name)
  const queueInfoData = {
    labels: Object.keys(metrics.queue_info?.queues || {}),
    datasets: [
      {
        label: 'Pending Tasks by Queue',
        data: Object.values(metrics.queue_info?.queues || {}),
        backgroundColor: 'rgba(251, 191, 36, 0.5)',
        borderColor: 'rgba(251, 191, 36, 1)',
        borderWidth: 2
      }
    ]
  }

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top'
      }
    }
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Metrics</h1>
        <select
          value={timeWindow}
          onChange={(e) => setTimeWindow(Number(e.target.value))}
          className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
        >
          <option value={300}>Last 5 minutes</option>
          <option value={900}>Last 15 minutes</option>
          <option value={1800}>Last 30 minutes</option>
          <option value={3600}>Last 1 hour</option>
          <option value={7200}>Last 2 hours</option>
          <option value={86400}>Last 24 hours</option>
        </select>
      </div>

      {/* Summary Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="text-2xl font-bold text-gray-900">
            {(metrics.tasks?.enqueued_per_second || 0).toFixed(2)}
          </div>
          <div className="text-sm text-gray-500">Tasks Enqueued/sec</div>
        </div>
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="text-2xl font-bold text-gray-900">
            {(metrics.tasks?.completed_per_second || 0).toFixed(2)}
          </div>
          <div className="text-sm text-gray-500">Tasks Completed/sec</div>
        </div>
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="text-2xl font-bold text-green-600">
            {((metrics.success_rate?.success_rate || 0) * 100).toFixed(1)}%
          </div>
          <div className="text-sm text-gray-500">Success Rate</div>
        </div>
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="text-2xl font-bold text-gray-900">
            {(metrics.worker_utilization?.utilization_percent || 0).toFixed(1)}%
          </div>
          <div className="text-sm text-gray-500">Worker Utilization</div>
        </div>
      </div>

      {/* Charts Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        {/* Throughput Chart */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Task Throughput</h2>
          <div className="h-64">
            <Bar data={throughputData} options={chartOptions} />
          </div>
        </div>

        {/* Success Rate Chart */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Success vs Failure Rate</h2>
          <div className="h-64">
            <Doughnut data={successRateData} options={chartOptions} />
          </div>
        </div>

        {/* Duration Percentiles Chart */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Task Duration Percentiles</h2>
          <div className="h-64">
            <Bar data={durationData} options={chartOptions} />
          </div>
        </div>

        {/* Queue Sizes Chart */}
        {Object.keys(metrics.queue_size_per_priority || {}).length > 0 && (
          <div className="bg-white rounded-lg shadow-md p-6">
            <h2 className="text-xl font-bold text-gray-900 mb-4">Queue Sizes by Priority</h2>
            <div className="h-64">
              <Bar data={queueSizesData} options={chartOptions} />
            </div>
          </div>
        )}
        
        {/* Queue Info Chart */}
        {Object.keys(metrics.queue_info?.queues || {}).length > 0 && (
          <div className="bg-white rounded-lg shadow-md p-6">
            <h2 className="text-xl font-bold text-gray-900 mb-4">Pending Tasks by Queue</h2>
            <div className="h-64">
              <Bar data={queueInfoData} options={chartOptions} />
            </div>
          </div>
        )}
      </div>

      {/* Task Status Breakdown */}
      {metrics.queue_info?.status_counts && Object.keys(metrics.queue_info.status_counts).length > 0 && (
        <div className="bg-white rounded-lg shadow-md p-6 mb-8">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Task Status Breakdown</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {Object.entries(metrics.queue_info.status_counts).map(([status, count]) => (
              <div key={status} className="text-center">
                <div className="text-2xl font-bold text-gray-900">{count}</div>
                <div className="text-sm text-gray-500 capitalize">{status}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Detailed Metrics Table */}
      <div className="bg-white rounded-lg shadow-md p-6">
        <h2 className="text-xl font-bold text-gray-900 mb-4">Detailed Metrics</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <div className="text-sm font-medium text-gray-500 mb-2">Task Statistics</div>
            <dl className="space-y-2 text-sm">
              <div className="flex justify-between">
                <dt className="text-gray-600">Tasks Enqueued/sec:</dt>
                <dd className="font-medium text-gray-900">
                  {(metrics.tasks?.enqueued_per_second || 0).toFixed(2)}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Tasks Completed/sec:</dt>
                <dd className="font-medium text-gray-900">
                  {(metrics.tasks?.completed_per_second || 0).toFixed(2)}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Success Rate:</dt>
                <dd className="font-medium text-gray-900">
                  {((metrics.success_rate?.success_rate || 0) * 100).toFixed(1)}%
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Failure Rate:</dt>
                <dd className="font-medium text-gray-900">
                  {((metrics.success_rate?.failure_rate || 0) * 100).toFixed(1)}%
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Total Completed:</dt>
                <dd className="font-medium text-gray-900">
                  {metrics.success_rate?.total || 0}
                </dd>
              </div>
            </dl>
          </div>
          <div>
            <div className="text-sm font-medium text-gray-500 mb-2">System Statistics</div>
            <dl className="space-y-2 text-sm">
              <div className="flex justify-between">
                <dt className="text-gray-600">Total Workers:</dt>
                <dd className="font-medium text-gray-900">
                  {metrics.worker_utilization?.total_workers || 0}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Active Workers:</dt>
                <dd className="font-medium text-gray-900">
                  {metrics.worker_utilization?.active_workers || 0}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Idle Workers:</dt>
                <dd className="font-medium text-gray-900">
                  {metrics.worker_utilization?.idle_workers || 0}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Pending Tasks:</dt>
                <dd className="font-medium text-gray-900">
                  {metrics.queue_info?.pending_tasks || 0}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Running Tasks:</dt>
                <dd className="font-medium text-gray-900">
                  {metrics.queue_info?.running_tasks || 0}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Total Queue Size:</dt>
                <dd className="font-medium text-gray-900">
                  {Object.values(metrics.queue_size_per_priority || {}).reduce((a, b) => a + b, 0)}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-600">Worker Utilization:</dt>
                <dd className="font-medium text-gray-900">
                  {(metrics.worker_utilization?.utilization_percent || 0).toFixed(1)}%
                </dd>
              </div>
            </dl>
          </div>
        </div>
      </div>
    </div>
  )
}

export default Metrics
