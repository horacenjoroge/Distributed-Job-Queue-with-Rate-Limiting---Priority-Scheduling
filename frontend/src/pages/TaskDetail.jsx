import React, { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { taskService } from '../services/api'
import websocket from '../services/websocket'

const TaskDetail = () => {
  const { id } = useParams()
  const navigate = useNavigate()
  const [task, setTask] = useState(null)
  const [loading, setLoading] = useState(true)
  const [logs, setLogs] = useState([])

  useEffect(() => {
    loadTask()
    setupWebSocket()

    return () => {
      websocket.off('task_updated', handleTaskUpdate)
    }
  }, [id])

  const setupWebSocket = () => {
    websocket.connect()
    websocket.on('task_updated', handleTaskUpdate)
  }

  const handleTaskUpdate = (data) => {
    if (data.task_id === id) {
      loadTask()
    }
  }

  const loadTask = async () => {
    try {
      setLoading(true)
      const response = await taskService.get(id)
      setTask(response.data)
      
      // Load logs if available
      if (response.data.logs) {
        setLogs(Array.isArray(response.data.logs) ? response.data.logs : [])
      }
    } catch (error) {
      console.error('Error loading task:', error)
      if (error.response?.status === 404) {
        alert('Task not found')
        navigate('/tasks')
      }
    } finally {
      setLoading(false)
    }
  }

  const handleRetry = async () => {
    try {
      await taskService.retry(id)
      alert('Task queued for retry')
      loadTask()
    } catch (error) {
      alert('Error retrying task: ' + (error.response?.data?.detail || error.message))
    }
  }

  const handleCancel = async () => {
    if (window.confirm('Are you sure you want to cancel this task?')) {
      try {
        await taskService.cancel(id, 'User requested cancellation')
        loadTask()
      } catch (error) {
        alert('Error canceling task: ' + (error.response?.data?.detail || error.message))
      }
    }
  }

  const getStatusBadge = (status) => {
    const colors = {
      pending: 'bg-yellow-100 text-yellow-800',
      running: 'bg-blue-100 text-blue-800',
      success: 'bg-green-100 text-green-800',
      failed: 'bg-red-100 text-red-800',
      cancelled: 'bg-gray-100 text-gray-800',
      retry: 'bg-orange-100 text-orange-800',
      timeout: 'bg-purple-100 text-purple-800'
    }
    return colors[status] || 'bg-gray-100 text-gray-800'
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-gray-500">Loading task details...</div>
      </div>
    )
  }

  if (!task) {
    return (
      <div className="text-center py-8">
        <p className="text-gray-500">Task not found</p>
        <button
          onClick={() => navigate('/tasks')}
          className="mt-4 text-blue-600 hover:text-blue-800"
        >
          Back to Tasks
        </button>
      </div>
    )
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-8">
        <div>
          <button
            onClick={() => navigate('/tasks')}
            className="text-gray-600 hover:text-gray-800 mb-2"
          >
            ← Back to Tasks
          </button>
          <h1 className="text-3xl font-bold text-gray-900">{task.name}</h1>
          <p className="text-gray-500 font-mono text-sm mt-1">{task.id}</p>
        </div>
        <div className="flex space-x-2">
          {task.status === 'failed' && (
            <button
              onClick={handleRetry}
              className="bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition-colors"
            >
              Retry Task
            </button>
          )}
          {(task.status === 'pending' || task.status === 'running') && (
            <button
              onClick={handleCancel}
              className="bg-red-600 text-white px-4 py-2 rounded-lg hover:bg-red-700 transition-colors"
            >
              Cancel Task
            </button>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Task Info */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Task Information</h2>
          <dl className="space-y-3">
            <div>
              <dt className="text-sm font-medium text-gray-500">Status</dt>
              <dd className="mt-1">
                <span className={`px-2 py-1 text-xs font-semibold rounded-full ${getStatusBadge(task.status)}`}>
                  {task.status}
                </span>
              </dd>
            </div>
            <div>
              <dt className="text-sm font-medium text-gray-500">Queue</dt>
              <dd className="mt-1 text-sm text-gray-900">{task.queue_name || 'default'}</dd>
            </div>
            <div>
              <dt className="text-sm font-medium text-gray-500">Priority</dt>
              <dd className="mt-1 text-sm text-gray-900">{task.priority || 'normal'}</dd>
            </div>
            <div>
              <dt className="text-sm font-medium text-gray-500">Worker Type</dt>
              <dd className="mt-1 text-sm text-gray-900">{task.worker_type || 'default'}</dd>
            </div>
            <div>
              <dt className="text-sm font-medium text-gray-500">Created At</dt>
              <dd className="mt-1 text-sm text-gray-900">{new Date(task.created_at).toLocaleString()}</dd>
            </div>
            {task.started_at && (
              <div>
                <dt className="text-sm font-medium text-gray-500">Started At</dt>
                <dd className="mt-1 text-sm text-gray-900">{new Date(task.started_at).toLocaleString()}</dd>
              </div>
            )}
            {task.completed_at && (
              <div>
                <dt className="text-sm font-medium text-gray-500">Completed At</dt>
                <dd className="mt-1 text-sm text-gray-900">{new Date(task.completed_at).toLocaleString()}</dd>
              </div>
            )}
            {task.duration && (
              <div>
                <dt className="text-sm font-medium text-gray-500">Duration</dt>
                <dd className="mt-1 text-sm text-gray-900">{task.duration.toFixed(2)}s</dd>
              </div>
            )}
            <div>
              <dt className="text-sm font-medium text-gray-500">Retry Count</dt>
              <dd className="mt-1 text-sm text-gray-900">{task.retry_count || 0} / {task.max_retries || 3}</dd>
            </div>
          </dl>
        </div>

        {/* Task Arguments */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Arguments</h2>
          <div className="space-y-3">
            <div>
              <dt className="text-sm font-medium text-gray-500 mb-1">Args</dt>
              <dd className="bg-gray-50 p-3 rounded-lg">
                <pre className="text-sm text-gray-900 overflow-x-auto">
                  {JSON.stringify(task.args || [], null, 2)}
                </pre>
              </dd>
            </div>
            <div>
              <dt className="text-sm font-medium text-gray-500 mb-1">Kwargs</dt>
              <dd className="bg-gray-50 p-3 rounded-lg">
                <pre className="text-sm text-gray-900 overflow-x-auto">
                  {JSON.stringify(task.kwargs || {}, null, 2)}
                </pre>
              </dd>
            </div>
          </div>
        </div>

        {/* Result/Error */}
        {(task.status === 'success' || task.status === 'failed') && (
          <div className={`bg-white rounded-lg shadow-md p-6 ${task.status === 'success' ? 'lg:col-span-2' : ''}`}>
            <h2 className="text-xl font-bold text-gray-900 mb-4">
              {task.status === 'success' ? 'Result' : 'Error'}
            </h2>
            <div className="bg-gray-50 p-4 rounded-lg">
              <pre className="text-sm text-gray-900 overflow-x-auto whitespace-pre-wrap">
                {task.status === 'success'
                  ? JSON.stringify(task.result || task.return_value || 'No result', null, 2)
                  : task.error || task.failure_reason || 'No error message'}
              </pre>
            </div>
          </div>
        )}

        {/* Logs */}
        {logs.length > 0 && (
          <div className="bg-white rounded-lg shadow-md p-6 lg:col-span-2">
            <h2 className="text-xl font-bold text-gray-900 mb-4">Logs</h2>
            <div className="bg-gray-900 text-green-400 p-4 rounded-lg font-mono text-sm max-h-96 overflow-y-auto">
              {logs.map((log, index) => (
                <div key={index} className="mb-1">
                  {typeof log === 'string' ? log : JSON.stringify(log)}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default TaskDetail
