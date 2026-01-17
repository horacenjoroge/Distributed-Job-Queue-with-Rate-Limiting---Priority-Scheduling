import React, { useState, useEffect } from 'react'
import { dlqService } from '../services/api'
import websocket from '../services/websocket'

const DLQ = () => {
  const [tasks, setTasks] = useState([])
  const [stats, setStats] = useState({ total: 0, by_reason: {} })
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    loadDLQ()
    setupWebSocket()

    const interval = setInterval(loadDLQ, 5000)

    return () => {
      clearInterval(interval)
      websocket.off('dlq_updated', handleDLQUpdate)
    }
  }, [])

  const setupWebSocket = () => {
    websocket.connect()
    websocket.on('dlq_updated', handleDLQUpdate)
  }

  const handleDLQUpdate = (data) => {
    loadDLQ()
  }

  const loadDLQ = async () => {
    try {
      setLoading(true)
      const [tasksRes, statsRes] = await Promise.all([
        dlqService.list(),
        dlqService.getStats()
      ])
      setTasks(tasksRes.data.tasks || [])
      setStats(statsRes.data || { total: 0, by_reason: {} })
    } catch (error) {
      console.error('Error loading DLQ:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleRetry = async (taskId) => {
    try {
      await dlqService.retry(taskId)
      alert('Task queued for retry')
      loadDLQ()
    } catch (error) {
      alert('Error retrying task: ' + (error.response?.data?.detail || error.message))
    }
  }

  const handleDelete = async (taskId) => {
    if (window.confirm('Are you sure you want to delete this task from DLQ?')) {
      try {
        await dlqService.delete(taskId)
        alert('Task deleted')
        loadDLQ()
      } catch (error) {
        alert('Error deleting task: ' + (error.response?.data?.detail || error.message))
      }
    }
  }

  const handlePurge = async () => {
    if (window.confirm('Are you sure you want to purge all tasks from DLQ? This cannot be undone.')) {
      try {
        await dlqService.purge()
        alert('DLQ purged successfully')
        loadDLQ()
      } catch (error) {
        alert('Error purging DLQ: ' + (error.response?.data?.detail || error.message))
      }
    }
  }

  if (loading && tasks.length === 0) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-gray-500">Loading DLQ...</div>
      </div>
    )
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Dead Letter Queue</h1>
        {tasks.length > 0 && (
          <button
            onClick={handlePurge}
            className="bg-red-600 text-white px-4 py-2 rounded-lg hover:bg-red-700 transition-colors"
          >
            Purge All
          </button>
        )}
      </div>

      {/* Stats */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="text-3xl font-bold text-red-600">{stats.total || tasks.length}</div>
          <div className="text-sm text-gray-500 mt-1">Total Failed Tasks</div>
        </div>
        <div className="bg-white rounded-lg shadow-md p-6">
          <h3 className="text-sm font-medium text-gray-500 mb-3">Failed by Reason</h3>
          <div className="space-y-2">
            {Object.entries(stats.by_reason || {}).map(([reason, count]) => (
              <div key={reason} className="flex justify-between text-sm">
                <span className="text-gray-700">{reason || 'Unknown'}</span>
                <span className="font-medium text-gray-900">{count}</span>
              </div>
            ))}
            {Object.keys(stats.by_reason || {}).length === 0 && (
              <div className="text-gray-500 text-sm">No data available</div>
            )}
          </div>
        </div>
      </div>

      {/* Tasks Table */}
      <div className="bg-white rounded-lg shadow-md overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Task ID</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Queue</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Retry Count</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Failure Reason</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Failed At</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {tasks.map((task) => (
                <tr key={task.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <a
                      href={`/tasks/${task.id}`}
                      className="text-blue-600 hover:text-blue-800 font-mono text-sm"
                    >
                      {task.id.substring(0, 12)}...
                    </a>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {task.name}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {task.queue_name || 'default'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {task.retry_count || 0} / {task.max_retries || 3}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    <div className="max-w-xs truncate" title={task.failure_reason || task.error || 'Unknown'}>
                      {task.failure_reason || task.error || 'Unknown'}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {task.failed_at
                      ? new Date(task.failed_at).toLocaleString()
                      : task.created_at
                      ? new Date(task.created_at).toLocaleString()
                      : 'N/A'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm">
                    <div className="flex space-x-2">
                      <button
                        onClick={() => handleRetry(task.id)}
                        className="text-green-600 hover:text-green-800"
                      >
                        Retry
                      </button>
                      <button
                        onClick={() => handleDelete(task.id)}
                        className="text-red-600 hover:text-red-800"
                      >
                        Delete
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
              {tasks.length === 0 && (
                <tr>
                  <td colSpan="7" className="px-6 py-4 text-center text-gray-500">
                    No failed tasks in DLQ
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

export default DLQ
